"""
ClickHouse client / db actions / interactions
"""
import os
import time
from abc import ABC
from enum import Enum
from pathlib import Path
from typing import Optional

from clickhouse_driver import Client as ClickHouseClient
from loguru import logger

from clickhouse_backup.utils.datatypes import Backup, FullBackup


class BackupTarget(Enum):
    """
    Represents supported backup targets.
    """
    FILE = 'File'
    DISK = 'Disk'
    S3 = 'S3'
    S3_DISK = 'S3-Disk'


class Client(ABC):
    """
    ClickHouse client. uses the native protocol.
    """

    def __init__(self, host: str = 'localhost', port: str = '9000',
                 user: str = 'default', password: str = '',
                 backup_target: BackupTarget = BackupTarget.FILE,
                 backup_dir: Optional[Path] = None,
                 disk: Optional[str] = None,
                 s3_endpoint: Optional[str] = None,
                 s3_bucket: Optional[str] = None,
                 s3_access_key_id: Optional[str] = None,
                 s3_secret_access_key: Optional[str] = None):
        """
        Init a new client.
        :param host: default: localhost
        :param port: 9000
        :param user: default: default
        :param password: default: ''
        :param backup_target: default: File
        :param backup_dir: default: None
        :param disk: default: None
        :param s3_endpoint: default: None
        :param s3_bucket: default: None
        :param s3_access_key_id: default: None
        :param s3_secret_access_key: default: None
        """
        match backup_target:
            case BackupTarget.FILE:
                if not backup_dir:
                    raise ValueError('backup_dir must be provided when using File backup target')
                if not os.path.isdir(backup_dir):
                    raise FileNotFoundError(f'backup_dir {backup_dir} does not exist!')
            case BackupTarget.DISK | BackupTarget.S3_DISK:
                if not disk:
                    raise ValueError('disk must be provided when using Disk backup target')
            case BackupTarget.S3 | BackupTarget.S3_DISK:
                if not s3_endpoint:
                    raise ValueError('s3_endpoint must be provided when using S3 backup target')
                if not s3_bucket:
                    raise ValueError('s3_bucket must be provided when using S3 backup target')
                if not s3_access_key_id:
                    raise ValueError(
                        's3_access_key_id must be provided when using S3 backup target')
                if not s3_secret_access_key:
                    raise ValueError(
                        's3_secret_access_key must be provided when using S3 backup target')

        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._client_socket: Optional[ClickHouseClient] = None

        self.backup_target = backup_target
        self.backup_dir = backup_dir
        self._disk = disk
        self.s3_endpoint = s3_endpoint
        self.s3_bucket = s3_bucket
        self.s3_access_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key

    @property
    def client(self) -> ClickHouseClient:
        """
        Open a new connection to ClickHouse.
        :return: driver socket
        """
        if not self._client_socket:
            logger.debug('Connecting to ClickHouse...')
            self._client_socket = ClickHouseClient(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password
            )
        return self._client_socket

    def _get_backup_path(self, file_path: str or Path) -> str:
        """
        Get the backup target.
        :return: backup target
        """
        match self.backup_target:
            case BackupTarget.FILE:
                # assuming a backup disk is defined.
                return f"File('{file_path}')"
            case BackupTarget.DISK | BackupTarget.S3_DISK:
                return f"Disk('{self._disk}', '{file_path}')"
            case BackupTarget.S3:
                return (f"S3('{self.s3_endpoint}/{self.s3_bucket}/{file_path}', "
                        f"'{self.s3_access_id}', '{self.s3_secret_access_key}')")
            case _:
                raise ValueError(f'Invalid backup target: {self.backup_target}')

    def _backup_command(self,
                        backup: Backup,
                        is_backup: bool = True,
                        table: Optional[str] = None,
                        dictionary: Optional[str] = None,
                        database: Optional[str] = None,
                        temporary_table: Optional[str] = None,
                        view: Optional[str] = None,
                        ignored_databases: Optional[list[str]] = None,
                        base_backup: Optional[FullBackup] = None,
                        overwrite: bool = False) -> str:
        """
        Wrapper for the backup/restore command of ClickHouse.
        Only one object can be restored/backed up.
        :param backup: backup object
        :param is_backup: whether to restore or back up
        :param table: table to restore
        :param dictionary: dictionary to restore
        :param database: database to restore
        :param temporary_table: temp table to restore
        :param view: view to restore
        :param ignored_databases: databases to ignore in the process.
            information_schema, system by default
        :param base_backup: full backup for base
        :param overwrite: whether to overwrite the existing tables/data
        :return: SQL command
        """
        ignored_databases = ignored_databases or ['system', 'information_schema',
                                                  'INFORMATION_SCHEMA']
        query = 'BACKUP ' if is_backup else 'RESTORE '
        if table:
            query += f'TABLE {table} '
        elif dictionary:
            query += f'DICTIONARY {dictionary} '
        elif database:
            query += f'DATABASE {database} '
        elif temporary_table:
            query += f'TEMPORARY TABLE {temporary_table} '
        elif view:
            query += f'VIEW {view} '
        else:
            if len(ignored_databases) == 0:
                raise ValueError(
                    'ignored_databases must contain at least one database e.g. system.')
            query += f"ALL EXCEPT DATABASES {', '.join(ignored_databases)} "
        query += f'{"TO" if is_backup else "FROM"} {self._get_backup_path(backup.path)} '
        settings = []
        if base_backup:
            settings.append(f'base_backup={self._get_backup_path(base_backup.path)}')
        if overwrite:
            settings.append('allow_non_empty_tables=true')
        if len(settings) > 0:
            query += 'SETTINGS ' + ', '.join(settings)

        # todo... will someone inject a query here? :) maybe should use the driver correctly hmmm
        return query

    def backup(self,
               backup: Backup,
               table: Optional[str] = None,
               dictionary: Optional[str] = None,
               database: Optional[str] = None,
               temporary_table: Optional[str] = None,
               view: Optional[str] = None,
               ignored_databases: Optional[list[str]] = None,
               base_backup: Optional[FullBackup] = None):
        """
        Backup a table, dictionary, database, temporary table, view or all databases.
        :param backup: backup object
        :param table: table name
        :param dictionary: dictionary name
        :param database: database name
        :param temporary_table: temporary table name
        :param view: view name
        :param ignored_databases: list of ignored databases
        :param base_backup: base backup file path
        :return: backup result
        """
        query = self._backup_command(
            backup=backup,
            table=table,
            dictionary=dictionary,
            database=database,
            temporary_table=temporary_table,
            view=view,
            ignored_databases=ignored_databases,
            base_backup=base_backup
        )
        logger.info(f'Creating a new backup: {backup}')
        result = self.client.execute(query + " ASYNC")
        (backup_id, status) = result[0]
        logger.info(f'Backup {backup_id} status: {status}')
        if status != 'CREATING_BACKUP':
            raise RuntimeError(
                f'Backup {backup_id} failed! Check the clickhouse logs or system.backups'
            )
        check_interval = 30
        while True:
            r = self.get_backup_status(backup_id)
            status = r[1]
            error = r[2]
            if status == 'CREATING_BACKUP':
                logger.debug(f'Still creating the backup... Checking again in {check_interval}s')
                time.sleep(check_interval)
                continue

            if status == 'BACKUP_CREATED':
                logger.info(f'Backup {backup_id} has been created. Status: {status}')
            else:
                logger.critical(f'Failed to create backup {backup_id} with error {error}')
            break
        return result

    def restore(self,
                backup: Backup,
                table: Optional[str] = None,
                dictionary: Optional[str] = None,
                database: Optional[str] = None,
                temporary_table: Optional[str] = None,
                view: Optional[str] = None,
                ignored_databases: Optional[list[str]] = None,
                base_backup: Optional[str] = None,
                overwrite: bool = False):
        """
        Restore a table, dictionary, database, temporary table, view or all databases.
        Only one object can be restored.
        :param backup: backup object
        :param table: table to restore
        :param dictionary: dictionary to restore
        :param database: database to restore
        :param temporary_table: temp table to restore
        :param view: view to restore
        :param ignored_databases: databases to ignore in restoration.
            information_schema, system by default
        :param base_backup: full backup for base
        :param overwrite: whether to overwrite the existing tables/data
        :return:
        """
        return self._backup_command(
            backup=backup,
            is_backup=False,
            table=table,
            dictionary=dictionary,
            database=database,
            temporary_table=temporary_table,
            view=view,
            ignored_databases=ignored_databases,
            base_backup=base_backup,
            overwrite=overwrite
        )

    def get_backup_status(self, backup_id):
        """
        Get the backup status for the given backup.
        :param backup_id: id of the backup
        :return: 3-tuple (name, status, error)
        """
        result = self.client.execute(
            'SELECT name, status, error FROM `system`.backups WHERE id = %(id)s',
            {'id': backup_id}
        )
        if len(result) == 0:
            raise RuntimeError("Backup not found!")
        return result[0]
