# Universal ETL Pipeline Project

This project reads raw data, cleans it, and saves it in useful formats.
It follows the ETL process:

- **E (Extract):** read data from files like CSV, JSON, XML, or Parquet
- **T (Transform):** clean data (remove duplicates, handle missing values, normalize column names)
- **L (Load):** save cleaned data to a database and a Parquet file

The goal is to provide a simple but production-ready ETL starter that is easy to run and easy to maintain.

## Folder and file overview

- `data/` - place your input files here
- `output/` - cleaned output files are saved here
- `logs/` - pipeline run logs are stored here
- `main.py` - starts the pipeline
- `extract.py` - reads input data
- `transform.py` - applies data cleaning rules
- `load.py` - writes data to database and parquet
- `utils.py` - shared helpers (config, logging, retry)

## How this pipeline works

1. Read the input file from `data/` (or custom path).
2. Apply cleaning and transformation rules.
3. Save output to:
   - database table (SQLite by default)
   - Parquet file in `output/`
4. Write logs for success/failure into `logs/etl.log`.

## Quick start

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Add sample input

Create a file: `data/sample.csv`

Example:

```csv
Name,Age,City
Alice,30,Delhi
Bob,,Mumbai
Alice,30,Delhi
```

### 3) Run the pipeline

```bash
python main.py
```

After running, check:

- `output/data.parquet`
- `logs/etl.log`
- `etl.db` (SQLite database file)

## Optional custom run

You can pass your own paths and DB settings:

```bash
python main.py --input data/sample.csv --output output/data.parquet --db-url sqlite:///etl.db --db-table data
```

## Environment variables (optional)

Use `.env.example` as reference. You can configure:

- `ETL_DATA_DIR`
- `ETL_OUTPUT_DIR`
- `ETL_LOGS_DIR`
- `ETL_LOG_FILE`
- `ETL_LOG_LEVEL`
- `ETL_DB_URL`
- `ETL_DB_TABLE`
- `ETL_MAX_RETRIES`
- `ETL_RETRY_DELAY_SECONDS`

## Run tests

```bash
pytest -q
```
