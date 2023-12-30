import os
from pathlib import Path

from loguru import logger


def setup_logging(log_dir: Path, log_level: str):
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    format_string = '{time:HH:mm:ss} | {level} | {message}'
    logger.add(log_dir / 'clickhouse-backup.log',
               format=format_string,
               rotation='00:00',
               retention='14 days',
               level=log_level,
               backtrace=True,
               diagnose=True)
