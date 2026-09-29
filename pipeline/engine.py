"""Pipeline engine — orchestrates all stages end-to-end."""
from __future__ import annotations

import logging
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .audit import AuditLogger
from .checkpoint import CheckpointManager
from .config import PipelineConfig
from .deduplicator import Deduplicator
from .extractor import Extractor
from .loader import Loader
from .models import QualityStatus, RunResult, RunStatus
from .quality import QualityGate
from .schema_handler import SchemaHandler
from .transformer import Transformer

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Execution order per batch:
      Extract → Deduplicate → Schema-coerce → Transform → Upsert → Quality Gate → Checkpoint

    The checkpoint advances only after both the upsert commits and the quality gate passes.
    A failure at either step leaves the checkpoint unchanged, so the file is retried next run.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._checkpoint = CheckpointManager(config.checkpoint_path)
        self._extractor = Extractor(config, self._checkpoint)
        self._deduplicator = Deduplicator(config)
        self._schema_handler = SchemaHandler(config)
        self._transformer = Transformer(config)
        self._loader = Loader(config)

    def run(self) -> RunResult:
        run_id = str(uuid.uuid4())
        result = RunResult(
            run_id=run_id,
            pipeline_name=self._config.name,
            started_at=datetime.now(timezone.utc),
        )
        self._loader.connect()

        try:
            audit = AuditLogger(self._loader._conn_or_raise())
            quality_gate = QualityGate(self._config, self._loader._conn_or_raise())
            batches_seen = 0

            for batch in self._extractor.extract():
                batches_seen += 1
                result.rows_extracted += len(batch.rows)

                clean_rows, rejected = self._deduplicator.deduplicate(batch)
                result.rows_rejected += len(rejected)
                self._write_dead_letters(rejected)

                coerced = self._schema_handler.coerce_batch(clean_rows)
                transformed = self._transformer.transform(coerced)

                inserted, updated = self._loader.upsert(transformed)
                result.rows_inserted += inserted
                result.rows_updated += updated

                result.files_processed += 1
                result.quality_checks = quality_gate.run_all(
                    expected_row_count=self._loader.row_count()
                )

                quality_failed = any(
                    qc.status == QualityStatus.FAILED for qc in result.quality_checks
                )
                if quality_failed:
                    logger.error("Quality gate FAILED for file %s — checkpoint NOT advanced", batch.file.path)
                    result.status = RunStatus.FAILED
                    break

                self._checkpoint.mark_processed(batch.file.content_hash, batch.file.modified_at)

            result.status = RunStatus.SUCCESS if result.status != RunStatus.FAILED else RunStatus.FAILED
            if batches_seen == 0:
                result.status = RunStatus.SKIPPED
                logger.info("No new files to process.")

        except Exception as exc:
            result.status = RunStatus.FAILED
            result.error = traceback.format_exc()
            logger.exception("Pipeline run failed: %s", exc)

        finally:
            result.finished_at = datetime.now(timezone.utc)
            audit.log(result)
            self._loader.close()

        return result

    def backfill(self, from_date: datetime | None = None) -> RunResult:
        """Reset checkpoint and reprocess all files (or from a given date)."""
        logger.info("Starting backfill from %s", from_date or "epoch")
        self._checkpoint.reset(from_date)
        return self.run()

    def _write_dead_letters(self, rejected: list[dict]) -> None:
        if not rejected or self._config.dead_letter_dir is None:
            return
        import csv
        import uuid as _uuid
        dlq_dir = self._config.dead_letter_dir
        dlq_dir.mkdir(parents=True, exist_ok=True)
        path = dlq_dir / f"rejected_{_uuid.uuid4().hex[:8]}.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rejected[0].keys()))
            writer.writeheader()
            writer.writerows(rejected)
        logger.info("Dead-letter: wrote %d rejected rows to %s", len(rejected), path)
