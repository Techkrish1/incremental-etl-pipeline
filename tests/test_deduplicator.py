"""
Tests for Deduplicator — covers:
  - Within-batch dedup by merge key
  - Late-arriving data: older record loses
  - CDC delete ops: soft-delete tagging
  - Null merge key → dead letter
"""
from datetime import datetime

import pytest

from pipeline.config import PipelineConfig
from pipeline.deduplicator import Deduplicator
from pipeline.models import Batch, FileRecord


def _make_config(tmp_path):
    return PipelineConfig(
        name="test",
        source_dir=tmp_path / "source",
        warehouse_path=tmp_path / "wh",
        checkpoint_path=tmp_path / "ckpt.json",
        target_table="orders",
        merge_key="order_id",
        updated_at_column="updated_at",
    )


def _make_batch(rows):
    return Batch(
        file=FileRecord("f.csv", "abc", 100, datetime(2024, 1, 1)),
        rows=rows,
        source_row_count=len(rows),
    )


@pytest.fixture
def dedup(tmp_path):
    return Deduplicator(_make_config(tmp_path))


def test_no_duplicates_passes_through(dedup):
    rows = [
        {"order_id": "1", "amount": "100", "updated_at": "2024-01-01 10:00:00"},
        {"order_id": "2", "amount": "200", "updated_at": "2024-01-01 11:00:00"},
    ]
    clean, rejected = dedup.deduplicate(_make_batch(rows))
    assert len(clean) == 2
    assert len(rejected) == 0


def test_within_batch_dedup_keeps_latest(dedup):
    """Two rows with same order_id — newer updated_at wins."""
    rows = [
        {"order_id": "1", "amount": "100", "updated_at": "2024-01-01 10:00:00"},
        {"order_id": "1", "amount": "150", "updated_at": "2024-01-01 12:00:00"},  # newer
    ]
    clean, rejected = dedup.deduplicate(_make_batch(rows))
    assert len(clean) == 1
    assert clean[0]["amount"] == "150"
    assert len(rejected) == 1


def test_late_arriving_data_is_rejected(dedup):
    """Late duplicate with older timestamp is rejected, not the fresh one."""
    rows = [
        {"order_id": "1", "amount": "150", "updated_at": "2024-01-05 12:00:00"},
        {"order_id": "1", "amount": "100", "updated_at": "2024-01-01 10:00:00"},  # older = late
    ]
    clean, rejected = dedup.deduplicate(_make_batch(rows))
    assert clean[0]["amount"] == "150"
    assert rejected[0]["amount"] == "100"


def test_null_merge_key_goes_to_dead_letter(dedup):
    rows = [
        {"order_id": None, "amount": "100", "updated_at": "2024-01-01"},
        {"order_id": "2", "amount": "200", "updated_at": "2024-01-01"},
    ]
    clean, rejected = dedup.deduplicate(_make_batch(rows))
    assert len(clean) == 1
    assert len(rejected) == 1
    assert "_rejection_reason" in rejected[0]


def test_cdc_delete_op_sets_deleted_flag(dedup):
    rows = [{"order_id": "1", "amount": "100", "updated_at": "2024-01-01", "_op": "D"}]
    clean, rejected = dedup.deduplicate(_make_batch(rows))
    assert len(clean) == 1
    assert clean[0]["_deleted"] is True
    assert "_op" not in clean[0]


def test_no_timestamp_column_no_crash(dedup):
    """Rows without updated_at should still process (no timestamp = keep last seen)."""
    rows = [
        {"order_id": "1", "amount": "100"},
        {"order_id": "1", "amount": "200"},
    ]
    clean, rejected = dedup.deduplicate(_make_batch(rows))
    assert len(clean) == 1
    assert len(rejected) == 1
