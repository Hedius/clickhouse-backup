"""
Handles listing/deleting backups for File,Disk backend.
"""
import os
from pathlib import Path
from typing import List

from clickhouse_backup.clickhouse.backends.base import Backend
from clickhouse_backup.utils.datatypes import FullBackup, IncrementalBackup


class DiskBackend(Backend):
    """
    Disk backend for handling file based backups to local disk.
    """

    def __init__(self, backup_dir: Path):
        """
        :param backup_dir: main dir for backups
        """
        self.backup_dir = backup_dir

    def get_existing_backups(self) -> List[str]:
        """
        Get all existing backups.
        :return: list with existing backup files.
        """
        return [x for x in os.listdir(self.backup_dir) if x.endswith(".zip") or '.tar' in x]

    def remove(self, backup: FullBackup or IncrementalBackup or Path or str) -> None:
        if isinstance(backup, (Path, str)):
            path = Path(backup)
        else:
            path = backup.path
        os.remove(self.backup_dir / path)
