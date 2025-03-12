"""
Creates backups in ClickHouse by using the BACKUP command of the DB.
"""
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import click
from dynaconf import Dynaconf
from loguru import logger

from clickhouse_backup.clickhouse.backends.base import Backend
from clickhouse_backup.clickhouse.backends.disk import DiskBackend
from clickhouse_backup.clickhouse.backends.s3 import S3Backend
from clickhouse_backup.clickhouse.client import BackupTarget, Client
from clickhouse_backup.utils.config import parse_config
from clickhouse_backup.utils.converters import parse_file_name
from clickhouse_backup.utils.datatypes import (Backup, FullBackup,
                                               IncrementalBackup)
from clickhouse_backup.utils.logging import setup_logging


class CtxArgs:
    """
    Cache object for arguments between click group and commands.
    """

    def __init__(self,
                 config_folder: Path, settings: Dynaconf, ch: Client,
                 existing_backups: Dict[datetime, FullBackup],
                 file_type: Optional[str],
                 backend: Backend):
        self.config_folder = Path(config_folder)
        self.settings = settings
        self.ch = ch
        self.existing_backups = existing_backups
        self.file_type = file_type
        self.backend = backend


def parse_existing_backups(backend: Backend, file_type: Optional[str] = None) -> Dict[
    datetime, FullBackup]:
    """
    Get all existing backups.
    :param backend: storage backend
    :param file_type: file type
    :return: dict of all existing backups
    """
    backups: Dict[datetime, FullBackup] = {}
    for file in sorted(backend.get_existing_backups(), key=lambda x: 'inc' in x):
        try:
            data = parse_file_name(file)
        except ValueError:
            # ignore the lost and found folder / etc. only check archives.
            if file_type and not file.endswith(file_type):
                logger.warning(f'Invalid file name in backup dir: {file}')
            continue

        if 'inc_timestamp' in data:
            if data['base_timestamp'] not in backups:
                logger.error(
                    f'Full base backup {data["base_timestamp"]} for {file} is missing! '
                    + 'Deleting...!')
                try:
                    backend.remove(file)
                except PermissionError:
                    logger.error(f'Could not delete {file}! Permission denied!')
                continue
            full_backup = backups[data['base_timestamp']]
            full_backup.incremental_backups.append(
                IncrementalBackup(base_backup=full_backup, timestamp=data['inc_timestamp'],
                                  file_type=file_type)
            )
            # sort the backups
            full_backup.incremental_backups.sort(key=lambda x: x.timestamp)
        else:
            backups[data['base_timestamp']] = FullBackup(timestamp=data['base_timestamp'],
                                                         file_type=file_type)
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


def clean_old_backups(backend: Backend, existing_backups: Dict[datetime, FullBackup],
                      max_full_backups: int,
                      next_backup: Backup):
    """
    Remove old backup chains.
    If we have n > max_full_backups, the oldest full backup + its
    incremental backups are deleted.
    If we have n >= max_full_backups and the next one is a full one, we
    also wipe the chain.
    :param backend: storage backend
    :param existing_backups: dict containing existing backups
    :param max_full_backups: max full backups to keep.
    :param next_backup: next backup what will be created.
    :return:
    """
    if max_full_backups == 0:
        return
    for timestamp in sorted(existing_backups.keys()):
        n = len(existing_backups)
        if n == max_full_backups:
            if isinstance(next_backup, IncrementalBackup):
                break
        elif n < max_full_backups:
            break
        x = existing_backups.pop(timestamp)
        logger.info(f'Deleting a full backup: {x} (Max {max_full_backups} full backups)')
        for inc in x.incremental_backups:
            backend.remove(inc)
        backend.remove(x)


@click.group()
@click.option(
    '-c',
    '--config-folder',
    help='Folder where the config files are stored. /etc/clickhouse-backup by default.'
         ' Make sure that the user has read and write access to the folder.',
    default='/etc/clickhouse-backup',
)
@click.pass_context
@click.version_option()
def main(ctx, config_folder):
    """
    Create and restore ClickHouse backups.
    Help and documentation are available at https://github.com/Hedius/clickhouse-backup.
    """
    try:
        settings = parse_config(Path(config_folder))
        log_dir = settings('logging.dir', cast=Path, default=None)
        if log_dir:
            setup_logging(log_dir, settings('logging.level', default='INFO'))

        ch = Client(
            host=settings('clickhouse.host', default='localhost'),
            port=settings('clickhouse.port', cast=int, default=9000),
            user=settings('clickhouse.user', default='default'),
            password=settings('clickhouse.password', default=''),
            backup_target=BackupTarget(settings('backup.target', default='File')),
            backup_dir=settings('backup.dir', default=None),
            disk=settings('backup.disk', default=None),
            s3_endpoint=settings('backup.s3.endpoint', default=None),
            s3_bucket=settings('backup.s3.bucket', default=None),
            s3_access_key_id=settings('backup.s3.access_key_id', default=None),
            s3_secret_access_key=settings('backup.s3.secret_access_key', default=None),
        )
    except Exception as e:
        logger.exception('Error during config parsing!', e)
        sys.exit(1)

    if ch.backup_target in (BackupTarget.S3, BackupTarget.S3_DISK):
        file_type = None
        backend = S3Backend(ch.s3_endpoint, ch.s3_bucket, ch.s3_access_id,
                            ch.s3_secret_access_key)
    else:
        file_type = 'tar.gz'
        backend = DiskBackend(ch.backup_dir)
    existing_backups = parse_existing_backups(backend, file_type)
    ctx.obj = CtxArgs(config_folder, settings, ch, existing_backups, file_type, backend)


@main.command('backup')
@click.option(
    '-f', '--force-full',
    is_flag=True, show_default=True, default=False,
    help='Force a full backup and ignore the rules for creating incremental backups.'
)
@click.pass_context
def backup_command(
        ctx,
        force_full
):
    """
    Perform a backup.
    Depending on the settings, this will create a full or an incremental backup.
    """
    args: CtxArgs = ctx.obj
    base_backup = None
    if not force_full:
        base_backup = get_base_backup(
            args.existing_backups,
            args.settings('backup.max_incremental_backups', cast=int, default=6)
        )
    new_backup = (base_backup.new_incremental_backup()
                  if base_backup
                  else FullBackup(file_type=args.file_type))
    if len(args.existing_backups) > 1:  # we will never delete if we only have 1 chain
        try:
            clean_old_backups(
                args.backend,
                args.existing_backups,
                args.settings('backup.max_full_backups', cast=int, default=2),
                new_backup
            )
        except Exception as e:
            logger.error(f'Error while deleting backup: {e}')
            sys.exit(1)
    try:
        args.ch.backup(
            backup=new_backup,
            base_backup=base_backup,
            ignored_databases=args.settings('backup.ignored_databases', cast=List[str],
                                            default=None)
        )
        if isinstance(new_backup, FullBackup):
            # append the new full backup to existing backups for the cleanup job
            args.existing_backups[new_backup.timestamp] = new_backup
    except Exception as e:
        logger.critical(f'Backup failed! (ClickHouse Error): {e}')
        sys.exit(1)


@main.command('list')
@click.pass_context
def list_command(ctx):
    """
    List all existing backups.
    """
    args: CtxArgs = ctx.obj
    if len(args.existing_backups) == 0:
        click.secho('None! You have to create a backup first...', fg='red',
                    file=sys.stderr)
        sys.exit(1)
    else:
        output = click.style('Listing backups:\n', fg='green', bold=True)
        newest_backup = None
        for full_backup in sorted(args.existing_backups.values(), key=lambda x: x.timestamp):
            newest_backup = full_backup
            output += click.style(f'{full_backup} @ {full_backup.timestamp_str}\n\t', fg='cyan')
            if len(full_backup.incremental_backups) == 0:
                output += click.style('No incremental backups.', fg='red')
            else:
                output += click.style('Incremental backups:', fg='bright_green')
            for incremental_backup in full_backup.incremental_backups:
                newest_backup = incremental_backup
                output += click.style(
                    f'\n\t\t{incremental_backup.path} @ {incremental_backup.timestamp_str}',
                    fg='yellow'
                )
            output += '\n\n'
        output += (
            'Call the restore command with the file name of a backup as the argument '
            'to get the restore DB command for a backup.\n'
            'E.g. for the newest one:\n'
        )
        output += click.style(
            f'clickhouse-backup -c {args.config_folder} restore -f {newest_backup.path}',
            fg='green'
        )
        click.echo(output)


@main.command('restore')
@click.argument(
    'backup',
    required=True,
)
@click.pass_context
def restore_command(ctx, backup):
    """
    Generate the restore command for the given backup.
    Use the command in clickhouse-client to restore the backup.
    You can use the output of the list command to view available backups.
    """
    args: CtxArgs = ctx.obj
    backup_to_restore = None
    for full_backup in args.existing_backups.values():
        if str(full_backup.path) == backup:
            backup_to_restore = full_backup
            break
        for incremental_backup in full_backup.incremental_backups:
            if str(incremental_backup.path) == backup:
                backup_to_restore = incremental_backup
                break
    if not backup_to_restore:
        click.secho(f'No match for {backup}! Check the name!\n', file=sys.stderr,
                    fg='red', bold=True)
        ctx.invoke(list_command)
        sys.exit(1)

    ignored_databases = args.settings('backup.ignored_databases', cast=List[str],
                                      default=None)
    examples = {
        "Restore all databases except the ignored ones": {
            "ignored_databases": ignored_databases,
        },
        "Force restore all databases and overwrite existing data": {
            "ignored_databases": ignored_databases,
            "overwrite": True,
        },
        "Restore a specific table": {
            "table": "database.table",
        },
        "Force restore a specific table": {
            "table": "database.table",
            "overwrite": True,
        },
        "Restore a specific table to a new table": {
            "table": "database.table AS database.new_table",
        }
    }
    click.secho(
        'Execute one of the following queries in clickhouse-client to restore the backup.\n',
        fg='green'
    )
    for msg, config in examples.items():
        # Will not run the query here. Just generate and print it.
        full_args = {
            "backup": backup_to_restore,
            "base_backup": None if isinstance(backup_to_restore,
                                              FullBackup) else backup_to_restore.base_backup,
            **config
        }
        query = args.ch.restore(**full_args)
        click.secho(f'{msg}:', fg='yellow')
        click.secho(f'{query}\n', fg='green')

    click.secho('Check the documentation of clickhouse for more information:\n'
                'Docs: https://clickhouse.com/docs/en/operations/backup',
                fg='yellow')


if __name__ == '__main__':
    main()
