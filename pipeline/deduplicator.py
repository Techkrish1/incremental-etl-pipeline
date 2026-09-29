"""Deduplicator — removes duplicates within a batch and handles CDC event streams."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from .config import PipelineConfig
from .models import Batch

logger = logging.getLogger(__name__)

_CDC_DELETE_OPS = {"D", "d", "delete", "DELETE"}


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


class Deduplicator:
    """
    Within-batch: keeps the row with the latest `updated_at` per merge key.
    Across batches: the loader's MERGE handles this — newer records overwrite older ones.

    CDC deletes (_op = 'D') become soft deletes (_deleted=True) to preserve
    audit history. Hard purges run as a separate scheduled vacuum.

    Rows with null merge keys go straight to the dead-letter queue — they can't
    be meaningfully upserted without a key.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._merge_key = config.merge_key
        self._ts_col = config.updated_at_column

    def deduplicate(self, batch: Batch) -> tuple[list[dict], list[dict]]:
        """Returns (clean_rows, rejected_rows)."""
        seen: dict[Any, dict] = {}
        rejected: list[dict] = []

        for row in batch.rows:
            key = row.get(self._merge_key)
            if key is None:
                row["_rejection_reason"] = f"null merge key ({self._merge_key})"
                rejected.append(row)
                continue

            op = row.get("_op", "")
            if op in _CDC_DELETE_OPS:
                row["_deleted"] = True
                row.pop("_op", None)

            existing = seen.get(key)
            if existing is None:
                seen[key] = row
                continue

            incoming_ts = _parse_ts(row.get(self._ts_col))
            existing_ts = _parse_ts(existing.get(self._ts_col))

            if incoming_ts is None or (existing_ts is not None and incoming_ts <= existing_ts):
                row["_rejection_reason"] = "superseded by newer record in same batch"
                rejected.append(row)
            else:
                existing["_rejection_reason"] = "superseded by newer record in same batch"
                rejected.append(existing)
                seen[key] = row

        clean = list(seen.values())
        logger.debug("Dedup: %d clean, %d rejected from %d input rows", len(clean), len(rejected), len(batch.rows))
        return clean, rejected
