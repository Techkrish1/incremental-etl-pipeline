"""Extractor — discovers new source files and reads them into Batches."""
from __future__ import annotations

import csv
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from .checkpoint import CheckpointManager
from .config import PipelineConfig
from .models import Batch, FileRecord

logger = logging.getLogger(__name__)


def _hash_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]


_READERS = {".csv": _read_csv, ".json": _read_json}


class Extractor:
    def __init__(self, config: PipelineConfig, checkpoint: CheckpointManager) -> None:
        self._config = config
        self._checkpoint = checkpoint

    def extract(self) -> Iterator[Batch]:
        source_files = sorted(
            self._config.source_dir.glob(self._config.file_pattern),
            key=lambda p: p.stat().st_mtime,
        )

        for path in source_files:
            content_hash = _hash_file(path)

            if self._checkpoint.is_processed(content_hash):
                logger.debug("Skipping already-processed file: %s", path.name)
                continue

            reader = _READERS.get(path.suffix.lower())
            if reader is None:
                logger.warning("Unsupported file format, skipping: %s", path.name)
                continue

            stat = path.stat()
            file_record = FileRecord(
                path=str(path),
                content_hash=content_hash,
                size_bytes=stat.st_size,
                modified_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            )

            rows = reader(path)
            logger.info("Extracted %d rows from %s", len(rows), path.name)

            for i in range(0, max(len(rows), 1), self._config.batch_size):
                chunk = rows[i : i + self._config.batch_size]
                yield Batch(
                    file=file_record,
                    rows=chunk,
                    source_row_count=len(rows),
                )
