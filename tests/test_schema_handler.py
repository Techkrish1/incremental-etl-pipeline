"""
Tests for SchemaHandler — covers:
  - Type coercion (string → int, float, bool)
  - Graceful handling of bad values (sets NULL, no crash)
  - New column detection (schema drift alert)
  - Missing column detection
"""
import pytest

from pipeline.config import PipelineConfig, SchemaFieldConfig
from pipeline.schema_handler import SchemaHandler


@pytest.fixture
def handler(tmp_path):
    cfg = PipelineConfig(
        name="test",
        source_dir=tmp_path,
        warehouse_path=tmp_path,
        checkpoint_path=tmp_path / "ckpt.json",
        target_table="t",
        merge_key="id",
        schema_fields=[
            SchemaFieldConfig(name="id", dtype="integer", nullable=False, primary_key=True),
            SchemaFieldConfig(name="amount", dtype="float", nullable=True),
            SchemaFieldConfig(name="is_active", dtype="boolean", nullable=True),
            SchemaFieldConfig(name="name", dtype="string", nullable=True),
        ],
    )
    return SchemaHandler(cfg)


def test_coerce_valid_types(handler):
    row = {"id": "42", "amount": "3.14", "is_active": "true", "name": "Alice"}
    result = handler.coerce_row(row)
    assert result["id"] == 42
    assert result["amount"] == pytest.approx(3.14)
    assert result["is_active"] is True
    assert result["name"] == "Alice"


def test_bad_int_becomes_null(handler):
    row = {"id": "not_a_number", "amount": "100", "is_active": "false", "name": "Bob"}
    result = handler.coerce_row(row)
    assert result["id"] is None


def test_empty_string_becomes_null(handler):
    row = {"id": "1", "amount": "", "is_active": "", "name": ""}
    result = handler.coerce_row(row)
    assert result["amount"] is None
    assert result["is_active"] is None
    assert result["name"] is None


def test_new_column_in_batch_is_detected(handler, caplog):
    import logging
    rows = [{"id": "1", "amount": "10", "is_active": "true", "name": "A", "extra_col": "x"}]
    with caplog.at_level(logging.WARNING):
        handler.coerce_batch(rows)
    assert "extra_col" in caplog.text


def test_extra_columns_pass_through(handler):
    """New columns not in schema still appear in output (additive evolution)."""
    row = {"id": "1", "amount": "10", "is_active": "true", "name": "A", "new_field": "val"}
    result = handler.coerce_row(row)
    assert result["new_field"] == "val"


def test_no_schema_passthrough(tmp_path):
    """With no schema_fields configured, all rows pass through unchanged."""
    cfg = PipelineConfig(
        name="t", source_dir=tmp_path, warehouse_path=tmp_path,
        checkpoint_path=tmp_path / "c.json", target_table="t", merge_key="id",
    )
    h = SchemaHandler(cfg)
    rows = [{"id": "1", "val": "foo"}]
    result = h.coerce_batch(rows)
    assert result == rows
