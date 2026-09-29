from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class RunStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class QualityStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"


@dataclass
class FileRecord:
    path: str
    content_hash: str
    size_bytes: int
    modified_at: datetime


@dataclass
class Batch:
    """Unit of work for one source file."""
    file: FileRecord
    rows: list[dict[str, Any]]
    source_row_count: int


@dataclass
class QualityCheckResult:
    check_name: str
    status: QualityStatus
    expected: Any
    actual: Any
    message: str


@dataclass
class RunResult:
    run_id: str
    pipeline_name: str
    started_at: datetime
    finished_at: datetime | None = None
    status: RunStatus = RunStatus.FAILED
    files_processed: int = 0
    rows_extracted: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_rejected: int = 0
    quality_checks: list[QualityCheckResult] = field(default_factory=list)
    error: str | None = None
