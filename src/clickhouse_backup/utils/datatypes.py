"""
Contains classes representing different backup types.
"""
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from .converters import format_timestamp


class Backup(ABC):
    """
    Abstract base class for backups.
    """

    def __init__(self, timestamp: Optional[datetime] = None, file_type: Optional[str] = None):
        """
        :param timestamp: timestamp of the backup
        :param file_type: file type of the backup.
            Should be zip or tar.gz Do not set if you handle S3.
        """
        self.timestamp = timestamp if timestamp else datetime.now()
        self.file_type = file_type

    def __str__(self):
        return f'Backup {self.timestamp}'

    @property
    def _file_type_suffix(self) -> str:
        """
        Returns self.file_type.
        """
        return f'.{self.file_type}' if self.file_type else ''

    @property
    @abstractmethod
    def path(self) -> Path:
        """
        file name of the backup file
        """

    @property
    def timestamp_str(self) -> str:
        """
        timestamp as string without seconds
        :return: timestamp as string
        """
        return self.timestamp.strftime('%Y-%m-%d %H:%M')


class IncrementalBackup(Backup):
    """
    Represents incremental backups.
    """

    def __init__(self, base_backup: Backup, timestamp: Optional[datetime] = None,
                 file_type: Optional[str] = None):
        """
        :param base_backup: full backup / reference to it
        :param timestamp: timestamp of the backup
        """
        super().__init__(timestamp, file_type)
        self.base_backup = base_backup

    def __str__(self):
        return f'Incremental Backup {self.path}'

    @property
    def path(self) -> Path:
        return Path(
            f'ch-backup-{format_timestamp(self.base_backup.timestamp)}-inc-'
            f'{format_timestamp(self.timestamp)}{self._file_type_suffix}')


class FullBackup(Backup):
    """
    Represents full backups.
    """

    def __init__(self, timestamp: Optional[datetime] = None,
                 file_type: Optional[str] = None):
        """
        :param timestamp: timestamp of the backup
        """
        super().__init__(timestamp, file_type)
        self.incremental_backups: List[IncrementalBackup] = []

    def __str__(self):
        return f'Full Backup {self.path}'

    @property
    def path(self) -> Path:
        return Path(f'ch-backup-{format_timestamp(self.timestamp)}-full{self._file_type_suffix}')

    def new_incremental_backup(self) -> IncrementalBackup:
        """
        Adds a new backup to the full backup. (You still have to the DB stuff manually!)
        :return: created backup
        """
        inc = IncrementalBackup(base_backup=self, file_type=self.file_type)
        self.incremental_backups.append(inc)
        return inc
