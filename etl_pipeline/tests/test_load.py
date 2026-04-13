import pandas as pd
from sqlalchemy import create_engine, text

from load import load_to_db, save_parquet


def test_load_to_db_writes_records(tmp_path):
    db_path = tmp_path / "etl_test.db"
    db_url = f"sqlite:///{db_path}"
    table = "data_test"
    df = pd.DataFrame([{"name": "Alice", "age": 30}])

    load_to_db(df, db_url=db_url, table_name=table)

    engine = create_engine(db_url)
    with engine.connect() as conn:
        row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
    assert row_count == 1


def test_save_parquet_creates_file(tmp_path):
    output_file = tmp_path / "output" / "data.parquet"
    df = pd.DataFrame([{"name": "Alice", "age": 30}])

    save_parquet(df, str(output_file))

    assert output_file.exists()
