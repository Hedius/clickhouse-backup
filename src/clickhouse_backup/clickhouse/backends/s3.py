import os
from pathlib import Path
from typing import Optional, List

from clickhouse_backup.clickhouse.backends.base import Backend
from clickhouse_backup.utils.datatypes import FullBackup, IncrementalBackup


class S3Backend(Backend):
    """
    Disk backend for handling file based backups to local disk.
    """

    def __init__(self, s3_endpoint: str, s3_access_key_id: str, s3_secret_access_key: str):
        """
        :param backup_dir: main dir for backups
        """
        self._s3_endpoint = s3_endpoint
        self._s3_access_key_id = s3_access_key_id
        self._s3_secret_access_key = s3_secret_access_key

    def get_existing_backups(self) -> List[str]:
        """
        Get all existing backups.
        :return: list with existing backup files.
        """
        pass

    def remove(self, backup: FullBackup or IncrementalBackup) -> None:
        pass
