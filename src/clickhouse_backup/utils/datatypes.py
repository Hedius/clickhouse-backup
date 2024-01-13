"""
Contains classes representing different backup types.
"""
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from loguru import logger

from .converters import format_timestamp


class Backup(ABC):
    """
    Abstract base class for backups.
    """

    def __init__(self, timestamp: Optional[datetime] = None, backup_dir: Optional[Path] = None):
        """
        :param timestamp: timestamp of the backup
        :param backup_dir: directory where clickhouse stores backups
        """
        self.timestamp = timestamp if timestamp else datetime.now()
        self.backup_dir = Path(backup_dir) if backup_dir else None

    def __str__(self):
        return f'Backup {self.timestamp}'

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

    def remove(self):
        """
        Remove the backup.
        :raises NotImplementedError: if you call this with a S3 storage.
        """
        if not self.backup_dir:
            raise NotImplementedError('Deletion without a backup dir is not supported yet!')
        os.remove(self.backup_dir / self.path)
        logger.info(f'Removed backup: {self}')


class IncrementalBackup(Backup):
    """
    Represents incremental backups.
    """

    def __init__(self, base_backup: Backup, timestamp: Optional[datetime] = None):
        """
        :param base_backup: full backup / reference to it
        :param timestamp: timestamp of the backup
        """
        super().__init__(timestamp, base_backup.backup_dir)
        self.base_backup = base_backup

    def __str__(self):
        return f'Incremental Backup {self.path}'

    @property
    def path(self) -> str:
        return (
            f'ch-backup-{format_timestamp(self.base_backup.timestamp)}-inc-'
            f'{format_timestamp(self.timestamp)}.zip')


class FullBackup(Backup):
    """
    Represents full backups.
    """

    def __init__(self, timestamp: Optional[datetime] = None,
                 backup_dir: Optional[Path] = None):
        """
        :param timestamp: timestamp of the backup
        :param backup_dir: directory where clickhouse stores backups
        """
        super().__init__(timestamp, backup_dir)
        self.incremental_backups: List[IncrementalBackup] = []

    def __str__(self):
        return f'Full Backup {self.path}'

    @property
    def path(self) -> Path:
        return Path(f"ch-backup-{format_timestamp(self.timestamp)}-full.zip")

    def new_incremental_backup(self) -> IncrementalBackup:
        """
        Adds a new backup the full backup. (You still have to the DB stuff manually!)
        :return: created backup
        """
        inc = IncrementalBackup(self)
        self.incremental_backups.append(inc)
        return inc

    def remove(self):
        """
        Removes the full backup and all its incremental backups.
        :return:
        """
        for backup in self.incremental_backups:
            try:
                backup.remove()
            except Exception as e:
                logger.error(f'Failed to remove {backup}: {e}')
                raise e
        super().remove()
