"""
helpers for converting values from one format to a different one
"""
import re
from datetime import datetime
from pathlib import Path

TIMESTAMP_FORMAT = '%Y%m%d_%H%M'


def parse_timestamp(timestamp: str) -> datetime:
    """
    Convert the given timestamp string to a datetime object.
    Format: TIMESTAMP_FORMAT
    :param timestamp: timestamp to parse
    :return: parsed timestamp
    """
    return datetime.strptime(timestamp, TIMESTAMP_FORMAT)


def format_timestamp(timestamp: datetime) -> str:
    """
    Convert the given datetime object to the correct string.
    :param timestamp: datetime object
    :return: formatted time
    """
    return timestamp.strftime(TIMESTAMP_FORMAT)


def parse_file_name(file_path: str or Path) -> dict:
    """
    Parse the given file_path.
    ch-backup-base_ts-type-inc_timestamp.zip
    :param file_path:
    :return: Dictionary with keys: base_timestamp, backup_type, path, inc_timestamp
    """
    match = re.match(r'ch-backup-([\d_]+)-([^-]+)-?([\d_]+)?', str(file_path))
    if not match:
        raise ValueError(f'Invalid file name: {file_path}')
    base_timestamp = parse_timestamp(match.group(1))
    backup_type = match.group(2)
    data = {
        'base_timestamp': base_timestamp,
        'backup_type': backup_type,
    }
    if 'inc' in backup_type:
        inc_timestamp = parse_timestamp(match.group(3))
        data['inc_timestamp'] = inc_timestamp
    data['path'] = Path(file_path)
    return data
