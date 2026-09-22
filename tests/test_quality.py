"""
Tests for QualityGate — covers:
  - PK uniqueness passes/fails
  - Null rate check passes/fails
  - Row count parity warning
"""
import duckdb
import pytest

from pipeline.config import PipelineConfig, QualityRuleConfig, SchemaFieldConfig
from pipeline.models import QualityStatus
from pipeline.quality import QualityGate


@pytest.fixture
def conn_with_data():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders (order_id VARCHAR, amount VARCHAR, updated_at VARCHAR, region VARCHAR)"
    )
    conn.execute(
        "INSERT INTO orders VALUES "
        "('1','100','2024-01-01','US'),"
        "('2','200','2024-01-02','EU'),"
        "('3', NULL,'2024-01-03','US')"
    )
    yield conn
    conn.close()


@pytest.fixture
def cfg(tmp_path):
    return PipelineConfig(
        name="test",
        source_dir=tmp_path,
        warehouse_path=tmp_path,
        checkpoint_path=tmp_path / "c.json",
        target_table="orders",
        merge_key="order_id",
        updated_at_column="updated_at",
        schema_fields=[
            SchemaFieldConfig(name="order_id", dtype="string", nullable=False, primary_key=True),
            SchemaFieldConfig(name="amount", dtype="float", nullable=False),
        ],
        quality=QualityRuleConfig(max_null_rate=0.0, max_duplicate_rate=0.0),
    )


def test_pk_uniqueness_passes(cfg, conn_with_data):
    gate = QualityGate(cfg, conn_with_data)
    result = gate._check_pk_uniqueness()
    assert result.status == QualityStatus.PASSED


def test_pk_uniqueness_fails_on_duplicates(cfg, conn_with_data):
    conn_with_data.execute("INSERT INTO orders VALUES ('1', '999', '2024-01-10', 'UK')")  # dup key
    gate = QualityGate(cfg, conn_with_data)
    result = gate._check_pk_uniqueness()
    assert result.status == QualityStatus.FAILED


def test_null_rate_fails_when_critical_column_has_nulls(cfg, conn_with_data):
    gate = QualityGate(cfg, conn_with_data)
    result = gate._check_null_rates()
    assert result.status == QualityStatus.FAILED
    assert "amount" in result.actual


def test_null_rate_passes_when_within_threshold(tmp_path, conn_with_data):
    cfg2 = PipelineConfig(
        name="test",
        source_dir=tmp_path,
        warehouse_path=tmp_path,
        checkpoint_path=tmp_path / "c.json",
        target_table="orders",
        merge_key="order_id",
        schema_fields=[
            SchemaFieldConfig(name="amount", dtype="float", nullable=False),
        ],
        quality=QualityRuleConfig(max_null_rate=0.5),  # 33% null is OK
    )
    gate = QualityGate(cfg2, conn_with_data)
    result = gate._check_null_rates()
    assert result.status == QualityStatus.PASSED


def test_row_count_parity_warning(cfg, conn_with_data):
    gate = QualityGate(cfg, conn_with_data)
    result = gate._check_row_count_parity(expected=100)
    assert result.status == QualityStatus.WARNING


def test_row_count_parity_passes(cfg, conn_with_data):
    gate = QualityGate(cfg, conn_with_data)
    result = gate._check_row_count_parity(expected=3)
    assert result.status == QualityStatus.PASSED
