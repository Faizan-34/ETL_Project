import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeVar

T = TypeVar("T")


@dataclass
class ETLConfig:
    data_dir: str = "data"
    output_dir: str = "output"
    logs_dir: str = "logs"
    log_file: str = "logs/etl.log"
    log_level: str = "INFO"
    db_url: str = "sqlite:///etl.db"
    db_table: str = "data"
    max_retries: int = 3
    retry_delay_seconds: int = 2

    @classmethod
    def from_env(cls) -> "ETLConfig":
        return cls(
            data_dir=os.getenv("ETL_DATA_DIR", "data"),
            output_dir=os.getenv("ETL_OUTPUT_DIR", "output"),
            logs_dir=os.getenv("ETL_LOGS_DIR", "logs"),
            log_file=os.getenv("ETL_LOG_FILE", "logs/etl.log"),
            log_level=os.getenv("ETL_LOG_LEVEL", "INFO"),
            db_url=os.getenv("ETL_DB_URL", "sqlite:///etl.db"),
            db_table=os.getenv("ETL_DB_TABLE", "data"),
            max_retries=int(os.getenv("ETL_MAX_RETRIES", "3")),
            retry_delay_seconds=int(os.getenv("ETL_RETRY_DELAY_SECONDS", "2")),
        )


def ensure_directories(config: ETLConfig) -> None:
    Path(config.data_dir).mkdir(parents=True, exist_ok=True)
    Path(config.output_dir).mkdir(parents=True, exist_ok=True)
    Path(config.logs_dir).mkdir(parents=True, exist_ok=True)
    Path(config.log_file).parent.mkdir(parents=True, exist_ok=True)


def setup_logging(log_file: str, log_level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def with_retry(
    func: Callable[..., T],
    *,
    retries: int,
    delay_seconds: int,
    logger: logging.Logger,
    operation_name: str,
    **kwargs,
) -> T:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return func(**kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            logger.warning(
                "%s failed on attempt %s/%s: %s",
                operation_name,
                attempt,
                retries,
                exc,
            )
            if attempt < retries:
                time.sleep(delay_seconds)

    raise RuntimeError(
        f"{operation_name} failed after {retries} attempts"
    ) from last_error
