"""
Creates backups in ClickHouse by using the BACKUP command of the DB.
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from loguru import logger

from ClickHouse.client import BackupTarget, Client
from utils.config import parse_config
from utils.converters import parse_file_name
from utils.datatypes import FullBackup, IncrementalBackup
from utils.logging import setup_logging


def get_existing_backups(backup_dir: Path) -> Dict[datetime, FullBackup]:
    """
    Get all existing backups.
    :param backup_dir: directory containing backups files.
    :return: dict of all existing backups
    """
    backups: Dict[datetime, FullBackup] = {}
    files = sorted(os.listdir(backup_dir), key=lambda x: 'full' in x)
    for file in files:
        try:
            data = parse_file_name(file)
        except ValueError:
            logger.warning(f'Invalid file name in backup dir: {file}')
            continue

        if 'inc_timestamp' in data:
            if data['base_timestamp'] not in backups:
                logger.error(
                    f'Full base backup {data["base_timestamp"]} for {file} is missing! '
                    f'Deleting...!')
                try:
                    os.remove(backup_dir / Path(file))
                except PermissionError:
                    logger.error(f'Could not delete {file}! Permission denied!')
                continue
            full_backup = backups[data['base_timestamp']]
            full_backup.incremental_backups.append(
                IncrementalBackup(base_backup=full_backup, timestamp=data['inc_timestamp'])
            )
        else:
            backups[data['base_timestamp']] = FullBackup(timestamp=data['base_timestamp'],
                                                         backup_dir=backup_dir)
        return backups


def get_base_backup(existing_backups: Dict[datetime, FullBackup],
                    max_incremental_backups: int) -> Optional[FullBackup]:
    """
    Get the base for the next backup.
    If an incremental backup can be created, the full backup is returned.
    :param existing_backups: dict containing existing backups
    :param max_incremental_backups: max incremental backups in a chain.
    :return: FullBackup or None
    """
    if len(existing_backups) == 0:
        return None
    newest_full_backup = existing_backups[max(existing_backups.keys())]
    if len(newest_full_backup.incremental_backups) < max_incremental_backups:
        return newest_full_backup


def clean_old_backups(existing_backups: Dict[datetime, FullBackup],
                      max_full_backups: int):
    """
    Remove old backup chains.
    If we have n > max_full_backups, the oldest full backup + its
    incremental backups are deleted.
    :param existing_backups: dict containing existing backups
    :param max_full_backups: max full backups to keep.
    :return:
    """
    for timestamp in sorted(existing_backups.keys()):
        if len(existing_backups) <= max_full_backups:
            break
        backup = existing_backups.pop(timestamp)
        logger.info(f'Deleting a full backup: {backup} (Too many backups)')
        backup.remove()


def main():
    """
    Main program for creating backups.
    :return:
    """
    try:
        settings, force_full = parse_config()

        log_dir = settings('logging.dir', cast=Path, default=None)
        if log_dir:
            setup_logging(log_dir, settings('logging.level', default='INFO'))

        ch = Client(
            host=settings('clickhouse.host', default='localhost'),
            port=settings('clickhouse.port', cast=int, default=9000),
            user=settings('clickhouse.user', default='default'),
            password=settings('clickhouse.password', default=''),
            backup_target=settings('backup.target', cast=BackupTarget),
            backup_dir=settings('backup.dir', default=None),
            disk=settings('backup.disk', default=None),
            s3_endpoint=settings('backup.s3.endpoint', default=None),
            s3_access_key_id=settings('backup.s3.access_key_id', default=None),
            s3_secret_access_key=settings('backup.s3.secret_access_key', default=None),
        )
    except Exception as e:
        logger.exception('Error during config parsing!', e)
        exit(1)

    if ch.backup_target == BackupTarget.S3 or not ch.backup_dir:
        logger.warning('Automatic incremental backups and retention are not '
                       'supported when using S3 as backup target!'
                       ' (Not implemented yet)')
        existing_backups = {}
    else:
        existing_backups = get_existing_backups(ch.backup_dir)
    base_backup = None
    if not force_full:
        base_backup = get_base_backup(
            existing_backups,
            settings('backup.max_incremental_backups', cast=int, default=6)
        )

    backup = (base_backup.new_incremental_backup()
              if base_backup
              else FullBackup(backup_dir=ch.backup_dir))
    try:
        ch.backup(
            backup=backup,
            base_backup=base_backup,
            ignored_databases=settings('backup.ignored_databases', cast=list[str], default=None)
        )
    except Exception as e:
        logger.critical('Backup failed!', e)
        exit(1)

    if len(existing_backups) > 1:  # we will never delete if we only have 1 chain
        try:
            clean_old_backups(
                existing_backups,
                settings('backup.max_full_backups', cast=int, default=2)
            )
        except Exception as e:
            logger.exception('Error while deleting backup', e)
            exit(1)


if __name__ == '__main__':
    main()
