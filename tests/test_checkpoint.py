"""
Tests for CheckpointManager — covers:
  - Idempotency: same file processed twice → skipped on second run
  - Watermark advancement
  - Backfill reset
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline.checkpoint import CheckpointManager


@pytest.fixture
def ckpt(tmp_path):
    return CheckpointManager(tmp_path / "checkpoint.json")


def test_new_file_is_not_processed(ckpt):
    assert ckpt.is_processed("abc123") is False


def test_mark_processed_persists(ckpt, tmp_path):
    ts = datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)
    ckpt.mark_processed("abc123", ts)
    assert ckpt.is_processed("abc123") is True


def test_idempotency_same_hash_twice(ckpt):
    """Processing the same content hash a second time should be detected as already done."""
    ts = datetime(2024, 1, 15, tzinfo=timezone.utc)
    ckpt.mark_processed("abc123", ts)
    ckpt.mark_processed("abc123", ts)  # no error, no duplicate
    with open(ckpt._path) as f:
        state = json.load(f)
    assert state["processed_hashes"].count("abc123") == 1


def test_watermark_advances_to_latest(ckpt):
    t1 = datetime(2024, 1, 10, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 20, tzinfo=timezone.utc)
    ckpt.mark_processed("h1", t1)
    ckpt.mark_processed("h2", t2)
    assert ckpt.watermark == t2


def test_watermark_does_not_go_backward(ckpt):
    t2 = datetime(2024, 1, 20, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 10, tzinfo=timezone.utc)
    ckpt.mark_processed("h2", t2)
    ckpt.mark_processed("h1", t1)  # older — should not move watermark back
    assert ckpt.watermark == t2


def test_reset_clears_all(ckpt):
    ts = datetime(2024, 1, 15, tzinfo=timezone.utc)
    ckpt.mark_processed("abc123", ts)
    ckpt.reset()
    assert ckpt.is_processed("abc123") is False


def test_partial_reset_for_backfill(ckpt):
    ts = datetime(2024, 1, 15, tzinfo=timezone.utc)
    ckpt.mark_processed("abc123", ts)
    from_date = datetime(2024, 1, 5, tzinfo=timezone.utc)
    ckpt.reset(from_date=from_date)
    assert ckpt.watermark == from_date
    assert ckpt.is_processed("abc123") is False
