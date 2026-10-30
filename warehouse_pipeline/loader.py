import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import inspect
import io

csv_path = "/opt/airflow/warehouse_pipeline/data/movements_large.csv"


def get_engine():
    return create_engine(
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )

def ensure_raw_schema():
    """Makes sure the correct schema exists, if not it creates one"""
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
    finally:
        engine.dispose()


def copy_to_sql(dataframe, cursor_connection, raw_connection):
    """
    The equivalent of the \\copy functionality from before containerization.
    Faster than regular chunked insertion.
    """

    buffer = io.StringIO()
    dataframe.to_csv(buffer, index=False, header=True)
    buffer.seek(0)

    cursor_connection.copy_expert(
        "COPY raw.movements FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
        buffer
    )

    raw_connection.commit()
    print(f"Loaded {len(dataframe)} rows to raw.movements")


def read_batch(path):
    """Reads the batch CSV and casts the columns the continuity check relies on"""
    df = pd.read_csv(path, dtype=str, encoding="utf-8")
    df["event_id"] = df["event_id"].astype(int)
    df["quantity"] = df["quantity"].astype(int)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    return df


def get_last_loaded(engine):
    """Returns the highest event_id and latest event_timestamp in raw.movements, (None, None) if it is empty"""
    with engine.connect() as conn:
        last_id, last_timestamp = conn.execute(
            text("SELECT MAX(event_id), MAX(event_timestamp) FROM raw.movements")
        ).one()
    if last_id is None:
        return None, None
    return int(last_id), pd.Timestamp(last_timestamp)


def validate_continuity(df, last_id, last_timestamp):
    """
    Raises ValueError unless every event in the batch comes strictly after what is already loaded:
    the smallest new event_id must exceed the largest loaded one, and the earliest new timestamp
    must be later than the latest loaded one. An empty table (last_id is None) accepts any batch.
    """
    if last_id is None:
        return

    first_new_id = df["event_id"].min()
    first_new_timestamp = df["event_timestamp"].min()

    if first_new_id <= last_id:
        raise ValueError(
            f"Batch starts at event_id {first_new_id}, but raw.movements already holds ids up to {last_id}. "
            "Cannot upload the same file twice, create a new file with larger ids")
    if first_new_timestamp <= last_timestamp:
        raise ValueError(
            f"Batch starts at {first_new_timestamp}, but raw.movements already holds events up to {last_timestamp}. "
            "Create a new file with newer timestamps")


def run_loader(engine_func=get_engine, path=csv_path):
    """
    Validates dtypes, sets up the table if needed, and makes sure the new batch has larger ids
    and later timestamps than anything already loaded, so batches stay chronologically continuous
    without duplicate data
    """

    engine = engine_func()
    try:
        df = read_batch(path)

        if inspect(engine).has_table("movements", schema='raw'):
            last_id, last_timestamp = get_last_loaded(engine)
        else:
            df.head(0).to_sql(name='movements', con=engine, schema='raw', if_exists='replace', index=False)
            last_id, last_timestamp = None, None

        validate_continuity(df, last_id, last_timestamp)

        raw_conn = engine.raw_connection()
        try:
            copy_to_sql(df, raw_conn.cursor(), raw_conn)
        finally:
            raw_conn.close()
    finally:
        engine.dispose()
