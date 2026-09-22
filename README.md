# Incremental ETL Pipeline

A production-grade, config-driven ETL pipeline in Python that processes source files
incrementally — loading only new or changed records on every run.

Built with **DuckDB** as the embedded warehouse, **Pydantic v2** for config validation,
and **Click + Rich** for the CLI.

---

## Problem

Daily CSV/JSON dumps from source systems need to be loaded into an analytical warehouse.
The naive approach — truncate and reload — doesn't scale and loses the ability to track
changes. This pipeline solves:

- Loading only new/changed files without re-scanning everything
- Safely re-running after failures without duplicating data
- Handling late-arriving and duplicate records from the source
- Adapting when the source schema changes without breaking the load
- Knowing whether the load was actually correct

---

## Architecture

```
Source Files (CSV / JSON)
        │
        ▼
┌──────────────────┐
│    Extractor     │  content-hash fingerprinting, sorted file discovery
└────────┬─────────┘
         │  new files only (checkpoint filter)
         ▼
┌──────────────────┐
│  Deduplicator    │  within-batch dedup by merge key + updated_at, CDC soft-deletes
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Schema Handler  │  type coercion, drift detection, new-column pass-through
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   Transformer    │  pipeline metadata (_pipeline_name, _loaded_at, _deleted)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Loader (Upsert) │  DuckDB stage-and-MERGE, ALTER TABLE for new columns, transactions
└────────┬─────────┘
         │  on commit
         ▼
┌──────────────────┐
│  Quality Gate    │  PK uniqueness, null rates, row count parity, freshness SLA
└────────┬─────────┘
         │  on pass
         ▼
┌──────────────────┐
│   Checkpoint     │  advance watermark, mark file hash as processed
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   Audit Logger   │  append RunResult to _pipeline_runs table
└──────────────────┘
```

---

## Design Decisions

**Content hash as file identity** — Using MD5 of file bytes rather than filename or mtime
correctly handles renamed files, re-delivered files, and S3 eventual-consistency
re-uploads. The identity is the content, not the path.

**Checkpoint after quality gate** — The watermark only advances after the upsert commits
*and* the quality checks pass. A failure at either step leaves the checkpoint unchanged,
so the file is retried on the next run without any manual intervention.

**Stage-and-merge upsert** — Rows are bulk-inserted into a temp staging table first, then
merged into the target in a single pass. This avoids row-by-row round-trips and is the
same pattern used by Spark structured streaming and dbt incremental models.

**Soft deletes for CDC** — When a source sends a delete event (`_op = D`), the row is
marked `_deleted = True` rather than physically removed. This preserves audit history
and lets downstream consumers filter at query time. Hard purges run as a separate
scheduled job on a retention policy.

**ELT over ETL for business logic** — The `Transformer` is intentionally thin (metadata
only). Aggregations, joins, and derived columns live in SQL views on the warehouse side.
SQL is more readable, versionable, and warehouse-native than Python transforms.

**Quality checks alert, don't revert** — A failing quality check sets the run status to
`FAILED` and logs it to the audit table, but does *not* roll back committed data.
Rolling back good data because of a monitoring bug is a worse outcome than alerting
on suspicious numbers and letting engineers investigate.

---

## Quick Start

```bash
pip install -e ".[dev]"

# Run pipeline on the sample data
etl run --config config/pipeline.yaml

# Check run history
etl status --config config/pipeline.yaml

# Full backfill
etl backfill --config config/pipeline.yaml

# Partial backfill from a specific date
etl backfill --config config/pipeline.yaml --from-date 2024-01-01

# Run tests
pytest -v
```

---

## Configuration

```yaml
# config/pipeline.yaml
name: orders_pipeline
source_dir: ./data/source
warehouse_path: ./data/warehouse
checkpoint_path: ./data/checkpoint.json
target_table: orders
merge_key: order_id
updated_at_column: updated_at
batch_size: 10000
file_pattern: "*.csv"

schema_fields:
  - name: order_id
    dtype: integer
    nullable: false
    primary_key: true
  - name: amount
    dtype: float
    nullable: true

quality:
  max_null_rate: 0.05
  max_duplicate_rate: 0.0
  freshness_sla_hours: 48
```

---

## Project Structure

```
pipeline/
├── models.py          # Dataclasses: Batch, RunResult, QualityCheckResult
├── config.py          # Pydantic config (YAML-driven, validated on load)
├── checkpoint.py      # Content-hash watermark / idempotency state
├── extractor.py       # File discovery + CSV/JSON reading + batching
├── deduplicator.py    # Within-batch dedup, CDC handling, dead-letter routing
├── schema_handler.py  # Type coercion, drift detection, additive evolution
├── transformer.py     # Pipeline metadata injection
├── loader.py          # DuckDB stage-and-merge upsert + schema evolution
├── quality.py         # Post-load quality checks
├── audit.py           # Run history (_pipeline_runs table)
├── engine.py          # Orchestration layer
└── cli.py             # Click CLI: run / backfill / status
tests/
├── test_checkpoint.py     # Idempotency, watermark advancement, backfill reset
├── test_deduplicator.py   # Dedup, late arrivals, CDC ops, null merge keys
├── test_loader.py         # Insert, update, idempotency, schema evolution
├── test_schema_handler.py # Type coercion, bad values, drift detection
├── test_quality.py        # PK uniqueness, null rates, row count, freshness
└── test_pipeline_e2e.py   # Full run/backfill cycle integration tests
```

---

## Querying Run History

```sql
-- Last 10 runs
SELECT run_id, status, started_at, rows_inserted, rows_updated, rows_rejected
FROM _pipeline_runs
ORDER BY started_at DESC
LIMIT 10;

-- Failed quality checks
SELECT run_id, started_at, quality_checks
FROM _pipeline_runs
WHERE status = 'failed';

-- Daily volume trend
SELECT DATE_TRUNC('day', started_at) AS day, SUM(rows_inserted) AS inserted
FROM _pipeline_runs
WHERE status = 'success'
GROUP BY 1
ORDER BY 1;
```

---

## Scaling Up

| Concern | Approach |
|---|---|
| Many source files | Partition by date, run each partition as a parallel task (Celery/Ray) |
| Large warehouse | Swap `Loader` for Snowflake/BigQuery MERGE — same interface |
| Orchestration | `etl run` returns exit code 1 on failure — drop it into Airflow/Prefect/GH Actions |
| Streaming sources | Replace `Extractor` with a Kafka consumer; rest of the pipeline is unchanged |
