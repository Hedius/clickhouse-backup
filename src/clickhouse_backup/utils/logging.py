"""
Configures logging.
"""
import os
from pathlib import Path

from loguru import logger


def setup_logging(log_dir: Path or str, log_level: str):
    """
    Configures loguru for logging. Adds a file output.
    :param log_dir: logging directory.
    :param log_level: loglevel for log file.
    :return:
    """
    if isinstance(log_dir, str):
        log_dir = Path(log_dir)
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    # format_string = '{time:HH:mm:ss} | {level} | {message}'
    logger.add(log_dir / 'clickhouse-backup.log',
               # format=format_string,
               rotation='00:00',
               retention='14 days',
               level=log_level,
               backtrace=True,
               diagnose=True)
