"""Post-load data quality gate — runs after every committed batch."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb

from .config import PipelineConfig
from .models import QualityCheckResult, QualityStatus

logger = logging.getLogger(__name__)


class QualityGate:
    """
    Runs after the upsert commits so checks reflect actual warehouse state.

    On failure: RunStatus is set to FAILED and the run is logged to the audit
    table. The committed data is intentionally NOT rolled back — a failing
    quality check should alert engineers, not silently discard good data.
    """

    def __init__(self, config: PipelineConfig, conn: duckdb.DuckDBPyConnection) -> None:
        self._config = config
        self._conn = conn
        self._table = config.target_table
        self._rules = config.quality

    def _query(self, sql: str) -> Any:
        return self._conn.execute(sql).fetchone()[0]

    def run_all(self, expected_row_count: int | None = None) -> list[QualityCheckResult]:
        results = []
        results.append(self._check_pk_uniqueness())
        results.append(self._check_null_rates())
        if expected_row_count is not None:
            results.append(self._check_row_count_parity(expected_row_count))
        if self._rules.freshness_sla_hours is not None:
            results.append(self._check_freshness())
        return results

    def _check_pk_uniqueness(self) -> QualityCheckResult:
        key = self._config.merge_key
        total = self._query(f'SELECT COUNT(*) FROM "{self._table}"')
        distinct = self._query(f'SELECT COUNT(DISTINCT "{key}") FROM "{self._table}"')
        dup_count = total - distinct
        dup_rate = dup_count / total if total > 0 else 0.0
        status = (
            QualityStatus.PASSED
            if dup_rate <= self._rules.max_duplicate_rate
            else QualityStatus.FAILED
        )
        return QualityCheckResult(
            check_name="pk_uniqueness",
            status=status,
            expected=f"dup_rate <= {self._rules.max_duplicate_rate}",
            actual=f"dup_rate = {dup_rate:.4f} ({dup_count} duplicates)",
            message=f"PK uniqueness {'OK' if status == QualityStatus.PASSED else 'FAILED'}",
        )

    def _check_null_rates(self) -> QualityCheckResult:
        critical_fields = [f.name for f in self._config.schema_fields if not f.nullable]
        if not critical_fields:
            return QualityCheckResult(
                check_name="null_rate",
                status=QualityStatus.PASSED,
                expected="no non-nullable fields configured",
                actual="skipped",
                message="No non-nullable fields to check",
            )

        total = self._query(f'SELECT COUNT(*) FROM "{self._table}"')
        violations = []
        for col in critical_fields:
            null_count = self._query(f'SELECT COUNT(*) FROM "{self._table}" WHERE "{col}" IS NULL')
            null_rate = null_count / total if total > 0 else 0.0
            if null_rate > self._rules.max_null_rate:
                violations.append(f"{col}={null_rate:.2%}")

        status = QualityStatus.PASSED if not violations else QualityStatus.FAILED
        return QualityCheckResult(
            check_name="null_rate",
            status=status,
            expected=f"null_rate <= {self._rules.max_null_rate:.0%} for non-nullable fields",
            actual=", ".join(violations) if violations else "all within threshold",
            message=f"Null rate check {'OK' if status == QualityStatus.PASSED else 'FAILED: ' + str(violations)}",
        )

    def _check_row_count_parity(self, expected: int) -> QualityCheckResult:
        actual = self._query(f'SELECT COUNT(*) FROM "{self._table}"')
        status = QualityStatus.PASSED if actual >= expected else QualityStatus.WARNING
        return QualityCheckResult(
            check_name="row_count_parity",
            status=status,
            expected=f">= {expected} rows",
            actual=str(actual),
            message=f"Row count {'OK' if status == QualityStatus.PASSED else 'WARNING — fewer rows than source'}",
        )

    def _check_freshness(self) -> QualityCheckResult:
        ts_col = self._config.updated_at_column
        try:
            max_ts_raw = self._query(f'SELECT MAX("{ts_col}") FROM "{self._table}"')
        except Exception:
            return QualityCheckResult(
                check_name="freshness",
                status=QualityStatus.WARNING,
                expected="timestamp column accessible",
                actual="query failed",
                message=f"Freshness check skipped — column '{ts_col}' not available",
            )

        if max_ts_raw is None:
            return QualityCheckResult(
                check_name="freshness",
                status=QualityStatus.WARNING,
                expected="non-null max timestamp",
                actual="NULL",
                message="No data in table — freshness check skipped",
            )

        sla_cutoff = datetime.now(timezone.utc) - timedelta(hours=self._rules.freshness_sla_hours)
        try:
            max_ts = datetime.fromisoformat(str(max_ts_raw))
            if max_ts.tzinfo is None:
                max_ts = max_ts.replace(tzinfo=timezone.utc)
            status = QualityStatus.PASSED if max_ts >= sla_cutoff else QualityStatus.FAILED
        except ValueError:
            status = QualityStatus.WARNING
            max_ts = None

        return QualityCheckResult(
            check_name="freshness",
            status=status,
            expected=f"max({ts_col}) >= {sla_cutoff.isoformat()}",
            actual=str(max_ts),
            message=f"Freshness {'OK' if status == QualityStatus.PASSED else 'STALE DATA DETECTED'}",
        )
