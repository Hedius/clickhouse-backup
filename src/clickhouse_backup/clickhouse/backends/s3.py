from pathlib import Path
from typing import List

import boto3

from clickhouse_backup.clickhouse.backends.base import Backend
from clickhouse_backup.utils.datatypes import FullBackup, IncrementalBackup


class S3Backend(Backend):
    """
    Disk backend for handling file based backups to local disk.
    """

    def __init__(self, s3_endpoint: str, s3_bucket: str, s3_access_key_id: str,
                 s3_secret_access_key: str):
        """
        :param backup_dir: main dir for backups
        """
        self._s3_endpoint = s3_endpoint
        self._s3_bucket = s3_bucket
        self._s3_access_key_id = s3_access_key_id
        self._s3_secret_access_key = s3_secret_access_key

        self.s3 = boto3.client(
            endpoint_url=self._s3_endpoint,
            aws_access_key_id=self._s3_access_key_id,
            aws_secret_access_key=self._s3_secret_access_key,
            aws_session_token=None,
            # config=Config(s3={'addressing_style': 'path'})
        )

    def get_existing_backups(self) -> List[str]:
        """
        Get all existing backups.
        :return: list with existing backup files.
        """
        return self.s3.list_objects_v2(Bucket=self._s3_bucket)

    def remove(self, backup: FullBackup or IncrementalBackup) -> None:
        if isinstance(backup, Path) or isinstance(backup, str):
            key = Path(backup)
        else:
            key = backup.path
        self.s3.delete_object(Bucket=self._s3_bucket, Key=key)
