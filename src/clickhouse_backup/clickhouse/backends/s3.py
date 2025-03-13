from pathlib import Path
from typing import List

import boto3
from loguru import logger

from clickhouse_backup.clickhouse.backends.base import Backend
from clickhouse_backup.utils.datatypes import FullBackup, IncrementalBackup


class S3Backend(Backend):
    """
    Disk backend for handling file based backups to local disk.
    """

    def __init__(self, s3_endpoint: str, s3_bucket: str, s3_access_key_id: str,
                 s3_secret_access_key: str):
        """
        :param s3_endpoint:
        :param s3_bucket:
        :param s3_access_key_id:
        :param s3_secret_access_key:
        """
        self._s3_endpoint = s3_endpoint
        self._s3_bucket = s3_bucket
        self._s3_access_key_id = s3_access_key_id
        self._s3_secret_access_key = s3_secret_access_key

        self.s3 = boto3.resource(
            's3',
            endpoint_url=self._s3_endpoint,
            aws_access_key_id=self._s3_access_key_id,
            aws_secret_access_key=self._s3_secret_access_key,
            aws_session_token=None,
            # config=Config(s3={'addressing_style': 'path'})
        )
        self.bucket = self.s3.Bucket(self._s3_bucket)

    def get_existing_backups(self) -> List[str]:
        """
        Get all existing backups.
        :return: list with existing backup files.
        """
        # yep we gotta get all objects... to look at the prefixes ch backup creates :)
        logger.info(
            'Loading existing backups from s3 (listing all files) this might take a while...')
        objects = self.bucket.objects.all()
        backups = set()
        for entry in objects:
            backup = entry.key.split('/')[0]
            backups.add(backup)
        return list(backups)

    def remove(self, backup: FullBackup or IncrementalBackup) -> None:
        if isinstance(backup, Path) or isinstance(backup, str):
            prefix = backup
        else:
            prefix = backup.path
        logger.info(f'Delete all objects from S3 with prefix: {prefix}')
        self.bucket.objects.filter(Prefix=f'{prefix}/').delete()
