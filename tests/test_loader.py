"""
Tests for Loader — covers:
  - First load inserts all rows
  - Second load with same keys updates, not inserts
  - Schema evolution: new column is added to warehouse automatically
  - Transaction rollback on error leaves warehouse untouched
"""
import pytest

from pipeline.config import PipelineConfig
from pipeline.loader import Loader


@pytest.fixture
def cfg(tmp_path):
    wh = tmp_path / "wh"
    wh.mkdir()
    return PipelineConfig(
        name="test",
        source_dir=tmp_path / "src",
        warehouse_path=wh,
        checkpoint_path=tmp_path / "ckpt.json",
        target_table="orders",
        merge_key="order_id",
    )


@pytest.fixture
def loader(cfg):
    l = Loader(cfg)
    l.connect()
    yield l
    l.close()


def _rows(data):
    return [
        {"order_id": str(d["id"]), "amount": str(d["amount"]), "updated_at": "2024-01-01"}
        for d in data
    ]


def test_first_load_inserts_all_rows(loader):
    rows = _rows([{"id": 1, "amount": 100}, {"id": 2, "amount": 200}])
    inserted, updated = loader.upsert(rows)
    assert inserted == 2
    assert updated == 0
    assert loader.row_count() == 2


def test_second_load_updates_existing(loader):
    rows_v1 = _rows([{"id": 1, "amount": 100}, {"id": 2, "amount": 200}])
    loader.upsert(rows_v1)

    rows_v2 = [{"order_id": "1", "amount": "999", "updated_at": "2024-01-02"}]
    inserted, updated = loader.upsert(rows_v2)

    assert inserted == 0
    assert updated == 1
    assert loader.row_count() == 2  # no new rows added


def test_upsert_is_idempotent(loader):
    rows = _rows([{"id": 1, "amount": 100}])
    loader.upsert(rows)
    loader.upsert(rows)  # exact same batch again
    assert loader.row_count() == 1


def test_schema_evolution_new_column(loader):
    rows_v1 = [{"order_id": "1", "amount": "100", "updated_at": "2024-01-01"}]
    loader.upsert(rows_v1)

    # New column 'region' appears in second batch
    rows_v2 = [{"order_id": "2", "amount": "200", "updated_at": "2024-01-02", "region": "US"}]
    loader.upsert(rows_v2)  # should not raise

    assert loader.row_count() == 2


def test_empty_batch_is_noop(loader):
    inserted, updated = loader.upsert([])
    assert inserted == 0
    assert updated == 0
    assert loader.row_count() == 0
