"""CLI entry point — etl run / etl backfill / etl status"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from .config import PipelineConfig
from .engine import Pipeline
from .models import RunStatus

console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


@click.group()
def main() -> None:
    """Incremental ETL Pipeline — production-grade, idempotent, observable."""


@main.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to pipeline YAML config")
def run(config: str) -> None:
    """Run the pipeline — processes only new/changed files."""
    cfg = PipelineConfig.from_yaml(config)
    pipeline = Pipeline(cfg)
    result = pipeline.run()
    _print_result(result)
    sys.exit(0 if result.status == RunStatus.SUCCESS or result.status == RunStatus.SKIPPED else 1)


@main.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
@click.option("--from-date", default=None, help="Reprocess files modified on/after this date (YYYY-MM-DD)")
def backfill(config: str, from_date: str | None) -> None:
    """Reset checkpoint and reprocess all source files (full or partial backfill)."""
    cfg = PipelineConfig.from_yaml(config)
    from_dt = None
    if from_date:
        from_dt = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    pipeline = Pipeline(cfg)
    result = pipeline.backfill(from_date=from_dt)
    _print_result(result)
    sys.exit(0 if result.status in (RunStatus.SUCCESS, RunStatus.SKIPPED) else 1)


@main.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
def status(config: str) -> None:
    """Show last N pipeline runs from the audit table."""
    import duckdb
    cfg = PipelineConfig.from_yaml(config)
    db_path = str(cfg.warehouse_path / f"{cfg.target_table}.duckdb")
    try:
        conn = duckdb.connect(db_path)
        rows = conn.execute(
            "SELECT run_id, status, started_at, rows_inserted, rows_updated, rows_rejected, error "
            "FROM _pipeline_runs ORDER BY started_at DESC LIMIT 10"
        ).fetchall()
        conn.close()
    except Exception as e:
        console.print(f"[red]Could not read audit table: {e}[/red]")
        sys.exit(1)

    table = Table(title="Last 10 Pipeline Runs", show_lines=True)
    for col in ("run_id", "status", "started_at", "inserted", "updated", "rejected", "error"):
        table.add_column(col)
    for row in rows:
        run_id, sts, started, ins, upd, rej, err = row
        color = "green" if sts == "success" else ("yellow" if sts == "skipped" else "red")
        table.add_row(
            run_id[:8] + "…",
            f"[{color}]{sts}[/{color}]",
            str(started)[:19],
            str(ins),
            str(upd),
            str(rej),
            (str(err)[:40] + "…") if err else "",
        )
    console.print(table)


def _print_result(result) -> None:
    color = {"success": "green", "skipped": "yellow", "failed": "red"}.get(result.status.value, "white")
    console.rule(f"[bold {color}]Run {result.status.value.upper()}[/bold {color}]")
    table = Table(show_header=False)
    table.add_column("key", style="bold")
    table.add_column("value")
    table.add_row("Run ID", result.run_id)
    table.add_row("Files processed", str(result.files_processed))
    table.add_row("Rows extracted", str(result.rows_extracted))
    table.add_row("Rows inserted", str(result.rows_inserted))
    table.add_row("Rows updated", str(result.rows_updated))
    table.add_row("Rows rejected", str(result.rows_rejected))
    if result.error:
        table.add_row("Error", f"[red]{result.error[:200]}[/red]")
    console.print(table)

    if result.quality_checks:
        qc_table = Table(title="Quality Checks")
        qc_table.add_column("Check")
        qc_table.add_column("Status")
        qc_table.add_column("Message")
        for qc in result.quality_checks:
            c = "green" if qc.status.value == "passed" else ("yellow" if qc.status.value == "warning" else "red")
            qc_table.add_row(qc.check_name, f"[{c}]{qc.status.value}[/{c}]", qc.message)
        console.print(qc_table)
