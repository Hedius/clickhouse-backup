"""
ABC for handling listing/deleting backups.
"""
from abc import ABC, abstractmethod
from typing import List

from clickhouse_backup.utils.datatypes import FullBackup, IncrementalBackup


class Backend(ABC):
    """
    ABC for backend implementations.
    Implements how to delete and load existing backups from a backend.
    """

    @abstractmethod
    def remove(self, backup: FullBackup or IncrementalBackup) -> None:
        """
        Removes the backup.
        Full backups also cause a deletion of all incremental backups.
        :param backup: The backup to remove.
        """

    @abstractmethod
    def get_existing_backups(self) -> List[str]:
        """
        Returns a list of existing backups.
        """
