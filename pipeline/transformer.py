"""Transformer — injects pipeline metadata onto clean rows before loading."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .config import PipelineConfig


class Transformer:
    """
    Intentionally thin. Heavy business logic (aggregations, joins, derived columns)
    belongs in SQL views on the warehouse — ELT keeps this layer fast and testable.

    This layer only adds: _deleted default, _pipeline_name, _loaded_at.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._pipeline_name = config.name

    def transform(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc).isoformat()
        result = []
        for row in rows:
            row = dict(row)
            row.setdefault("_deleted", False)
            row["_pipeline_name"] = self._pipeline_name
            row["_loaded_at"] = now
            result.append(row)
        return result
