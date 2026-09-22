"""Loader — upserts rows into DuckDB using a stage-and-merge pattern."""
from __future__ import annotations

import logging
from typing import Any

import duckdb

from .config import PipelineConfig

logger = logging.getLogger(__name__)


class Loader:
    """
    Stage-and-merge approach:
      1. Bulk-insert the batch into a temp staging table.
      2. UPDATE existing rows from staging.
      3. INSERT rows from staging that have no match in the target.

    This avoids row-by-row round-trips and matches how Spark/dbt handle upserts.
    The entire batch is wrapped in a transaction — a mid-batch failure rolls back
    cleanly and the checkpoint won't advance, so the file gets reprocessed.

    Schema evolution: _ensure_columns() issues ALTER TABLE ADD COLUMN for any new
    column found in the batch, so source additions never break the load.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._db_path = str(config.warehouse_path / f"{config.target_table}.duckdb")
        self._conn: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> None:
        self._conn = duckdb.connect(self._db_path)
        logger.info("Connected to DuckDB: %s", self._db_path)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def _conn_or_raise(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("Loader not connected. Call connect() first.")
        return self._conn

    def _ensure_table(self, columns: list[str]) -> None:
        conn = self._conn_or_raise()
        col_defs = ", ".join(f'"{c}" VARCHAR' for c in columns)
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{self._config.target_table}" ({col_defs})'
        )

    def _ensure_columns(self, columns: list[str]) -> None:
        conn = self._conn_or_raise()
        existing = {
            row[0]
            for row in conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{self._config.target_table}'"
            ).fetchall()
        }
        for col in columns:
            if col not in existing:
                conn.execute(f'ALTER TABLE "{self._config.target_table}" ADD COLUMN "{col}" VARCHAR')
                logger.info("Schema evolution: added column '%s' to %s", col, self._config.target_table)

    def upsert(self, rows: list[dict[str, Any]]) -> tuple[int, int]:
        """Returns (inserted_count, updated_count)."""
        if not rows:
            return 0, 0

        conn = self._conn_or_raise()
        columns = list(rows[0].keys())
        merge_key = self._config.merge_key

        self._ensure_table(columns)
        self._ensure_columns(columns)

        import pandas as pd
        df = pd.DataFrame(rows)

        col_list = ", ".join(f'"{c}"' for c in columns)
        update_cols = [c for c in columns if c != merge_key]

        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute("CREATE TEMP TABLE IF NOT EXISTS _stage AS SELECT * FROM df LIMIT 0")
            conn.execute("DELETE FROM _stage")
            conn.register("_stage_df", df)
            conn.execute("INSERT INTO _stage SELECT * FROM _stage_df")

            existing_count = conn.execute(
                f'SELECT COUNT(*) FROM "{self._config.target_table}" t '
                f'JOIN _stage s ON t."{merge_key}" = s."{merge_key}"'
            ).fetchone()[0]

            set_clause = ", ".join(f't."{c}" = s."{c}"' for c in update_cols)
            conn.execute(
                f"""
                UPDATE "{self._config.target_table}" t
                SET {set_clause}
                FROM _stage s
                WHERE t."{merge_key}" = s."{merge_key}"
                """
            )
            conn.execute(
                f"""
                INSERT INTO "{self._config.target_table}" ({col_list})
                SELECT {col_list} FROM _stage s
                WHERE NOT EXISTS (
                    SELECT 1 FROM "{self._config.target_table}" t
                    WHERE t."{merge_key}" = s."{merge_key}"
                )
                """
            )
            conn.execute("COMMIT")
            inserted = len(rows) - existing_count
            updated = existing_count
            logger.info("Upsert complete: %d inserted, %d updated", inserted, updated)
            return inserted, updated

        except Exception:
            conn.execute("ROLLBACK")
            logger.exception("Upsert failed — transaction rolled back")
            raise

    def row_count(self) -> int:
        conn = self._conn_or_raise()
        return conn.execute(f'SELECT COUNT(*) FROM "{self._config.target_table}"').fetchone()[0]
