from sqlalchemy import create_engine
from pathlib import Path


def load_to_db(df, db_url="sqlite:///etl.db", table_name="data"):
    if df is None or df.empty:
        raise ValueError("No records to load into database.")
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists="replace", index=False)


def save_parquet(df, output_path):
    if df is None or df.empty:
        raise ValueError("No records to save as parquet.")
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target, index=False)