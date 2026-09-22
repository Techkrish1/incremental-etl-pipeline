"""Audit trail — writes every pipeline run to a queryable _pipeline_runs table."""
from __future__ import annotations

import json
import logging
from dataclasses import asdict

import duckdb

from .models import RunResult

logger = logging.getLogger(__name__)

_CREATE_AUDIT_TABLE = """
CREATE TABLE IF NOT EXISTS _pipeline_runs (
    run_id          VARCHAR PRIMARY KEY,
    pipeline_name   VARCHAR,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          VARCHAR,
    files_processed INTEGER,
    rows_extracted  INTEGER,
    rows_inserted   INTEGER,
    rows_updated    INTEGER,
    rows_rejected   INTEGER,
    quality_checks  JSON,
    error           VARCHAR
)
"""


class AuditLogger:
    """
    Stores run history in the same DuckDB database as the data.
    No external monitoring tool needed — run history is plain SQL:

        SELECT * FROM _pipeline_runs ORDER BY started_at DESC LIMIT 10;

    Row count trends (rows_inserted over time) surface volume anomalies.
    Failed quality checks are stored as JSON for downstream alerting queries.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        self._conn.execute(_CREATE_AUDIT_TABLE)

    def log(self, result: RunResult) -> None:
        qc_json = json.dumps([asdict(qc) for qc in result.quality_checks])
        self._conn.execute(
            "INSERT OR REPLACE INTO _pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                result.run_id,
                result.pipeline_name,
                result.started_at,
                result.finished_at,
                result.status.value,
                result.files_processed,
                result.rows_extracted,
                result.rows_inserted,
                result.rows_updated,
                result.rows_rejected,
                qc_json,
                result.error,
            ],
        )
        logger.info(
            "Audit logged: run_id=%s status=%s inserted=%d updated=%d rejected=%d",
            result.run_id,
            result.status.value,
            result.rows_inserted,
            result.rows_updated,
            result.rows_rejected,
        )
