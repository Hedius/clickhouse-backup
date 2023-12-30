"""
Todo:
1. argument parsing
2. optional config file for storing the DB connection info
3. os path checking to check the existence of a full backup.
4. create x incremental backups and then a full backup again
    or full backup on monday and incremental backups on the other days
5. delete old chains at a certain point (e.g.: have 2 full chains at all times)
6. if a full backup is missing for a chain -> delete the chain / create a new full backup

When creating a backup:
check system.backups for the result of the backup job. (Since the query should be asymmetric iirc)
nevermind: Backups can be synchronous (default)
https://clickhouse.com/docs/en/operations/backup
"""
from pathlib import Path

from loguru import logger

from ClickHouse.client import Client
from utils.config import parse_config
from utils.logging import setup_logging


def main():
    try:
        settings = parse_config()

        log_dir = settings('logging.dir', cast=Path, default=None)
        if log_dir:
            setup_logging(log_dir, settings('logging.level', default='INFO'))

        ch = Client(
            host=settings('clickhouse.host', default='localhost'),
            port=settings('clickhouse.port', cast=int, default=9000),
            user=settings('clickhouse.user', default='default'),
            password=settings('clickhouse.password', default=''),
            backup_target=settings.backup.target,
            backup_dir=settings('backup.dir', default=None),
            disk=settings('backup.disk', default=None),
            s3_endpoint=settings('backup.s3.endpoint', default=None),
            s3_access_key_id=settings('backup.s3.access_key_id', default=None),
            s3_secret_access_key=settings('backup.s3.secret_access_key', default=None),
        )
    except Exception as e:
        logger.exception('Error during config parsing!', e)
        exit(1)


if __name__ == '__main__':
    main()
