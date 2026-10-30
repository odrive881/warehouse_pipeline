"""
Tests for loader.py. They run against a temporary SQLite database with a "raw" schema attached,
so no Postgres is needed. COPY is Postgres-only, so copy_to_sql is swapped for a plain to_sql append.
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, event

import loader

COLUMNS = ["event_id", "sku", "warehouse_id", "event_type", "quantity", "event_timestamp"]


def write_batch(path, first_id, timestamps):
    rows = [
        [first_id + i, "SKU-0001", "WH-POZNAN", "receipt", 10, ts]
        for i, ts in enumerate(timestamps)
    ]
    pd.DataFrame(rows, columns=COLUMNS).to_csv(path, index=False)
    return path


@pytest.fixture
def engine(tmp_path, monkeypatch):
    eng = create_engine(f"sqlite:///{tmp_path / 'main.db'}")

    @event.listens_for(eng, "connect")
    def attach_raw_schema(dbapi_conn, _):
        dbapi_conn.execute(f"ATTACH DATABASE '{tmp_path / 'raw.db'}' AS raw")

    def append_to_sql(dataframe, cursor_connection, raw_connection):
        dataframe.to_sql(name="movements", con=eng, schema="raw", if_exists="append", index=False)

    monkeypatch.setattr(loader, "copy_to_sql", append_to_sql)
    yield eng
    eng.dispose()


def loaded_ids(engine):
    return pd.read_sql("SELECT event_id FROM raw.movements ORDER BY event_id", engine)["event_id"].tolist()


def test_first_load_creates_table_and_loads_rows(engine, tmp_path):
    batch = write_batch(tmp_path / "batch1.csv", 1, ["2024-01-01 08:00:00", "2024-01-01 09:00:00"])

    loader.run_loader(lambda: engine, batch)

    assert loaded_ids(engine) == [1, 2]


def test_newer_batch_is_appended(engine, tmp_path):
    loader.run_loader(lambda: engine, write_batch(tmp_path / "batch1.csv", 1, ["2024-01-01 08:00:00", "2024-01-01 09:00:00"]))
    loader.run_loader(lambda: engine, write_batch(tmp_path / "batch2.csv", 3, ["2024-01-02 08:00:00", "2024-01-02 09:00:00"]))

    assert loaded_ids(engine) == [1, 2, 3, 4]


def test_newer_batch_starting_on_same_day_is_appended(engine, tmp_path):
    # The old check truncated both timestamps to the day and rejected this batch
    loader.run_loader(lambda: engine, write_batch(tmp_path / "batch1.csv", 1, ["2024-01-01 08:00:00"]))
    loader.run_loader(lambda: engine, write_batch(tmp_path / "batch2.csv", 2, ["2024-01-01 17:00:00"]))

    assert loaded_ids(engine) == [1, 2]


def test_duplicate_batch_is_rejected(engine, tmp_path):
    batch = write_batch(tmp_path / "batch1.csv", 1, ["2024-01-01 08:00:00", "2024-01-01 09:00:00"])
    loader.run_loader(lambda: engine, batch)

    with pytest.raises(ValueError, match="event_id"):
        loader.run_loader(lambda: engine, batch)

    assert loaded_ids(engine) == [1, 2]


def test_overlapping_ids_are_rejected_even_with_newer_timestamps(engine, tmp_path):
    loader.run_loader(lambda: engine, write_batch(tmp_path / "batch1.csv", 1, ["2024-01-01 08:00:00", "2024-01-01 09:00:00"]))

    with pytest.raises(ValueError, match="event_id"):
        loader.run_loader(lambda: engine, write_batch(tmp_path / "batch2.csv", 2, ["2024-01-05 08:00:00"]))

    assert loaded_ids(engine) == [1, 2]


def test_continuity_uses_earliest_timestamp_not_second_row():
    # The old check read row 2; here row 2 is newer than the loaded data but row 3 is not
    df = pd.DataFrame({
        "event_id": [10, 11, 12],
        "event_timestamp": pd.to_datetime(["2024-01-03 08:00", "2024-01-03 09:00", "2024-01-01 07:00"]),
    })

    with pytest.raises(ValueError, match="newer timestamps"):
        loader.validate_continuity(df, last_id=9, last_timestamp=pd.Timestamp("2024-01-02 12:00"))
