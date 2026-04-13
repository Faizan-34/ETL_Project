import argparse
import os
import time
from extract import extract
from transform import transform
from load import load_to_db, save_parquet
from utils import ETLConfig, ensure_directories, setup_logging, with_retry


def parse_args():
    parser = argparse.ArgumentParser(description="Run the ETL pipeline.")
    parser.add_argument(
        "--input",
        dest="input_path",
        default=None,
        help="Input file path (csv/json/xml/parquet).",
    )
    parser.add_argument(
        "--output",
        dest="output_path",
        default=None,
        help="Output parquet file path.",
    )
    parser.add_argument(
        "--db-url",
        dest="db_url",
        default=None,
        help="Database connection URL.",
    )
    parser.add_argument(
        "--db-table",
        dest="db_table",
        default=None,
        help="Target table name.",
    )
    return parser.parse_args()


def run_pipeline(file_path, output_path, config, logger):
    start_time = time.time()
    try:
        logger.info("Pipeline started for input: %s", file_path)

        df = with_retry(
            extract,
            retries=config.max_retries,
            delay_seconds=config.retry_delay_seconds,
            logger=logger,
            operation_name="extract",
            file_path=file_path,
        )
        logger.info("Extracted %s rows.", len(df))

        df = with_retry(
            transform,
            retries=config.max_retries,
            delay_seconds=config.retry_delay_seconds,
            logger=logger,
            operation_name="transform",
            df=df,
        )
        logger.info("Transformed dataframe has %s rows.", len(df))

        with_retry(
            load_to_db,
            retries=config.max_retries,
            delay_seconds=config.retry_delay_seconds,
            logger=logger,
            operation_name="load_to_db",
            df=df,
            db_url=config.db_url,
            table_name=config.db_table,
        )

        with_retry(
            save_parquet,
            retries=config.max_retries,
            delay_seconds=config.retry_delay_seconds,
            logger=logger,
            operation_name="save_parquet",
            df=df,
            output_path=output_path,
        )

        elapsed = round(time.time() - start_time, 2)
        logger.info("Pipeline completed successfully in %s seconds.", elapsed)
        return 0

    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        return 1


if __name__ == "__main__":
    args = parse_args()
    config = ETLConfig.from_env()
    ensure_directories(config)
    logger = setup_logging(config.log_file, config.log_level)

    input_path = args.input_path or os.path.join(config.data_dir, "sample.csv")
    output_path = args.output_path or os.path.join(config.output_dir, "data.parquet")
    if args.db_url:
        config.db_url = args.db_url
    if args.db_table:
        config.db_table = args.db_table

    raise SystemExit(run_pipeline(input_path, output_path, config, logger))