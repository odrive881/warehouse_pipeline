import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import inspect
import io

old_file_path = "/opt/airflow/warehouse_pipeline/data/movements_large.csv"
output_path = "/opt/airflow/warehouse_pipeline/data/movements_large.csv"


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
    except ProgrammingError as e:
        if "already exists" in str(e):
            raise
    finally:
        engine.dispose()


def copy_to_sql(dataframe, cursor_connection, raw_connection):
    """
    The equivalent of the \copy functionality from before containerization.
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


def run_loader(engine_func=get_engine):
    """
    Validates dtypes, Sets up table and schema, checks if the table exists and makes sure the new piece
    of data has larger ids, and more advanced dates to allow for seamless chronological continuity without
    duplicate data
    """

    engine = engine_func()

    #Dtype validation
    df = pd.read_csv(output_path, dtype=str, encoding="utf-8")
    df["event_id"] = df["event_id"].astype(int)
    df["quantity"] = df["quantity"].astype(int)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])

    #Does the table exist?
    inspector = inspect(engine)
    table_exists = inspector.has_table("movements", schema='raw')

    if not table_exists:
        df.head(0).to_sql(name='movements', con=engine, schema='raw', if_exists='replace', index=False)


    raw_conn = engine.raw_connection()
    cursor = raw_conn.cursor()

    #Checking the chronological continuity (event_id, timestamp)
    try:
        if table_exists:
            # New check if the same file is being transferred to sql twice
            test_df = pd.read_csv(old_file_path, usecols=["event_timestamp"])
            new_timestamp = pd.to_datetime(test_df.iloc[1, 0].split(" ")[0])

            with engine.begin() as conn:
                existing_rows = pd.read_sql(text("SELECT COUNT(*) FROM raw.movements"), conn)
                if existing_rows.iloc[0, 0] == 0:
                    copy_to_sql(df, cursor, raw_conn)
                else:
                    old_timestamp = pd.read_sql(text("SELECT event_timestamp FROM raw.movements "
                                                     "ORDER BY event_timestamp DESC "
                                                     "LIMIT 1"), conn)

                    old_timestamp_formatted = pd.to_datetime(old_timestamp.iloc[0, 0].strftime("%Y-%m-%d"))

                    if old_timestamp_formatted < new_timestamp:
                        copy_to_sql(df, cursor, raw_conn)
                    else:
                        raise ValueError(
                            "Cannot upload the same file twice, create a new file with newer timestamps and larger ids")
        else:
            copy_to_sql(df, cursor, raw_conn)
    finally:
        raw_conn.close()
        engine.dispose()
