"""Schema handler — detects drift and coerces types without breaking the pipeline."""
from __future__ import annotations

import logging
from typing import Any

from .config import PipelineConfig, SchemaFieldConfig

logger = logging.getLogger(__name__)

_COERCERS = {
    "integer": int,
    "float": float,
    "boolean": lambda v: str(v).lower() in ("true", "1", "yes"),
    "string": str,
    "timestamp": str,  # kept as string; DuckDB casts on load
}


class SchemaHandler:
    """
    Handles three categories of schema change:

    New columns    — passed through as-is; Loader will ALTER TABLE to add them.
    Missing columns — filled with NULL and logged; no row rejection.
    Type changes   — coercion attempted; on failure the cell becomes NULL and
                     a WARNING is logged. Destructive changes (e.g. string→int
                     with non-numeric data) will show up as NULL spikes in the
                     quality gate, surfacing the issue without losing the row.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._fields: dict[str, SchemaFieldConfig] = {f.name: f for f in config.schema_fields}

    def detect_drift(self, incoming_columns: set[str]) -> dict:
        expected = set(self._fields)
        new_cols = incoming_columns - expected
        missing_cols = expected - incoming_columns
        drift = {}
        if new_cols:
            drift["new_columns"] = sorted(new_cols)
            logger.warning("Schema drift — new columns detected: %s", new_cols)
        if missing_cols:
            drift["missing_columns"] = sorted(missing_cols)
            logger.warning("Schema drift — expected columns missing: %s", missing_cols)
        return drift

    def coerce_row(self, row: dict[str, Any]) -> dict[str, Any]:
        if not self._fields:
            return row

        result: dict[str, Any] = {}
        for name, field_cfg in self._fields.items():
            raw = row.get(name)
            if raw is None or raw == "":
                result[name] = None
                continue
            coercer = _COERCERS[field_cfg.dtype]
            try:
                result[name] = coercer(raw)
            except (ValueError, TypeError):
                logger.warning("Type coercion failed for column=%s value=%r — setting NULL", name, raw)
                result[name] = None

        for col, val in row.items():
            if col not in result:
                result[col] = val

        return result

    def coerce_batch(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return rows
        self.detect_drift(set(rows[0].keys()))
        return [self.coerce_row(r) for r in rows]
