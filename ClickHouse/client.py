import logging
from enum import Enum
from typing import Optional

from clickhouse_driver import Client as ClickHouseClient


class BackupTarget(Enum):
    """
    Represents supported backup targets.
    """
    File = 'File'
    Disk = 'Disk'
    S3 = 'S3'


class Client:
    """
    ClickHouse client. uses the native protocol.
    """

    def __init__(self, host: str = 'localhost', port: str = '9000',
                 user: str = 'default', password: str = '',
                 backup_target: BackupTarget = BackupTarget.File,
                 backup_dir: Optional[str] = None,
                 disk: Optional[str] = None,
                 s3_endpoint: Optional[str] = None,
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
        :param s3_access_key_id: default: None
        :param s3_secret_access_key: default: None
        """
        match backup_target:
            case BackupTarget.File:
                if not backup_dir:
                    raise ValueError('backup_dir must be provided when using File backup target')
            case BackupTarget.Disk:
                if not disk:
                    raise ValueError('disk must be provided when using Disk backup target')
            case BackupTarget.S3:
                if not s3_endpoint:
                    raise ValueError('s3_endpoint must be provided when using S3 backup target')
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
        self._client = self._connect()

        self.backup_target = backup_target
        self._backup_dir = backup_dir
        self._disk = disk
        self._s3_endpoint = s3_endpoint
        self._s3_access_key_id = s3_access_key_id
        self._s3_secret_access_key = s3_secret_access_key

    def _connect(self) -> ClickHouseClient:
        """
        Open a new connection to ClickHouse.
        :return: driver socket
        """
        logging.info('Connecting to ClickHouse...')
        return ClickHouseClient(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password
        )

    def _get_backup_path(self, file_path: str) -> str:
        """
        Get the backup target.
        :return: backup target
        """
        file_path = file_path.lstrip('/')
        match self.backup_target:
            case BackupTarget.File:
                return f"File('{self._backup_dir.rstrip('/')}/{file_path}')"
            case BackupTarget.Disk:
                return f"Disk({self._disk.rstrip("/")}/{file_path}')"
            case BackupTarget.S3:
                return (f"S3('{self._s3_endpoint.rstrip('/')}/{file_path}', "
                        f"'{self._s3_access_key_id}', '{self._s3_secret_access_key}')")

    def backup_command(self,
                       file_path: str,
                       is_backup: bool = True,
                       table: Optional[str] = None,
                       dictionary: Optional[str] = None,
                       database: Optional[str] = None,
                       temporary_table: Optional[str] = None,
                       view: Optional[str] = None,
                       ignored_databases: Optional[list[str]] = None,
                       base_backup: Optional[str] = None):
        """
        Wrapper for the backup/restore command of ClickHouse.
        :param file_path:
        :param is_backup:
        :param table:
        :param dictionary:
        :param database:
        :param temporary_table:
        :param view:
        :param ignored_databases: system and information_schema are ignored by default
        :param base_backup:
        :return:
        """
        ignored_databases = ignored_databases or ['system', 'INFORMATION_SCHEMA',
                                                  'information_schema']
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
                raise ValueError('ignored_databases must contain at least one database e.g. system.')
            query += f"ALL EXCEPT DATABASES {', '.join(ignored_databases)} "
        query += f'{"TO" if is_backup else "FROM"} {self._get_backup_path(file_path)} '
        if base_backup:
            query += f'SETTINGS base_backup = {self._get_backup_path(base_backup)} '
        # todo... will someone inject a query here? :) maybe should use the driver's query method :)
        result = self._client.execute(query)
        return result

    def backup(self,
               file_path: str,
               table: Optional[str] = None,
               dictionary: Optional[str] = None,
               database: Optional[str] = None,
               temporary_table: Optional[str] = None,
               view: Optional[str] = None,
               ignored_databases: Optional[list[str]] = None,
               base_backup: Optional[str] = None):
        """
        Backup a table, dictionary, database, temporary table, view or all databases.
        :param file_path: backup file path
        :param table: table name
        :param dictionary: dictionary name
        :param database: database name
        :param temporary_table: temporary table name
        :param view: view name
        :param ignored_databases: list of ignored databases
        :param base_backup: base backup file path
        :return: backup result
        """
        return self.backup_command(
            file_path=file_path,
            table=table,
            dictionary=dictionary,
            database=database,
            temporary_table=temporary_table,
            view=view,
            ignored_databases=ignored_databases,
            base_backup=base_backup
        )

    def restore(self,
                file_path: str,
                table: Optional[str] = None,
                dictionary: Optional[str] = None,
                database: Optional[str] = None,
                temporary_table: Optional[str] = None,
                view: Optional[str] = None,
                ignored_databases: Optional[list[str]] = None,
                base_backup: Optional[str] = None):
        """
        Restore a table, dictionary, database, temporary table, view or all databases.
        :param file_path:
        :param table:
        :param dictionary:
        :param database:
        :param temporary_table:
        :param view:
        :param ignored_databases:
        :param base_backup:
        :return:
        """
        return self.backup_command(
            file_path=file_path,
            is_backup=False,
            table=table,
            dictionary=dictionary,
            database=database,
            temporary_table=temporary_table,
            view=view,
            ignored_databases=ignored_databases,
            base_backup=base_backup
        )

    def get_backup_status(self):
        """
        Get the backup status for all backups.
        Todo add filters to check the status of the specific backup we want.
        :return:
        """
        return self._client.execute('SELECT * FROM system.backups')
