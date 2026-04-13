import pandas as pd
from pathlib import Path


def extract(file_path):
    input_path = Path(file_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    extension = input_path.suffix.lower()
    if extension == ".csv":
        return pd.read_csv(input_path)

    if extension == ".json":
        return pd.read_json(input_path)

    if extension == ".xml":
        return pd.read_xml(input_path)

    if extension == ".parquet":
        return pd.read_parquet(input_path)
    raise ValueError(f"Unsupported file format: {extension}")