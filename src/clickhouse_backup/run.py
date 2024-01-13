"""
Creates backups in ClickHouse by using the BACKUP command of the DB.
"""
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import click
from dynaconf import Dynaconf
from loguru import logger

from clickhouse_backup.clickhouse.client import BackupTarget, Client
from clickhouse_backup.utils.config import parse_config
from clickhouse_backup.utils.converters import parse_file_name
from clickhouse_backup.utils.datatypes import FullBackup, IncrementalBackup
from clickhouse_backup.utils.logging import setup_logging


class CtxArgs:
    """
    Cache object for arguments between click group and commands.
    """

    def __init__(self,
                 config_folder: Path, settings: Dynaconf, ch: Client,
                 existing_backups: Dict[datetime, FullBackup]):
        self.config_folder = Path(config_folder)
        self.settings = settings
        self.ch = ch
        self.existing_backups = existing_backups


def get_existing_backups(backup_dir: Path) -> Dict[datetime, FullBackup]:
    """
    Get all existing backups.
    :param backup_dir: directory containing backups files.
    :return: dict of all existing backups
    """
    backups: Dict[datetime, FullBackup] = {}
    files = sorted(os.listdir(backup_dir), key=lambda x: 'inc' in x)
    for file in files:
        try:
            data = parse_file_name(file)
        except ValueError:
            # ignore the lost and found folder / etc. only check archives.
            if not file.endswith('.zip') and not '.tar' not in file:
                logger.warning(f'Invalid file name in backup dir: {file}')
            continue

        if 'inc_timestamp' in data:
            if data['base_timestamp'] not in backups:
                logger.error(
                    f'Full base backup {data["base_timestamp"]} for {file} is missing! '
                    + 'Deleting...!')
                try:
                    os.remove(backup_dir / Path(file))
                except PermissionError:
                    logger.error(f'Could not delete {file}! Permission denied!')
                continue
            full_backup = backups[data['base_timestamp']]
            full_backup.incremental_backups.append(
                IncrementalBackup(base_backup=full_backup, timestamp=data['inc_timestamp'])
            )
            # sort the backups
            full_backup.incremental_backups.sort(key=lambda x: x.timestamp)
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
        return
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
    if max_full_backups == 0:
        return
    for timestamp in sorted(existing_backups.keys()):
        if len(existing_backups) <= max_full_backups:
            break
        x = existing_backups.pop(timestamp)
        logger.info(f'Deleting a full backup: {x} (Max {max_full_backups} full backups)')
        x.remove()


@click.group()
@click.option(
    '-c',
    '--config-folder',
    help='Folder where the config files are stored. E.g.: /etc/clickhouse-backup',
    required=True
)
@click.pass_context
@click.version_option()
def main(ctx, config_folder):
    """
    Create and restore ClickHouse backups.
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
            s3_access_key_id=settings('backup.s3.access_key_id', default=None),
            s3_secret_access_key=settings('backup.s3.secret_access_key', default=None),
        )
    except Exception as e:
        logger.exception('Error during config parsing!', e)
        sys.exit(1)

    if ch.backup_target == BackupTarget.S3 or not ch.backup_dir:
        logger.warning('Automatic incremental backups and retention are not '
                       'supported when using S3 as backup target!'
                       ' (Not implemented yet)')
        existing_backups = {}
    else:
        existing_backups = get_existing_backups(ch.backup_dir)
    ctx.obj = CtxArgs(config_folder, settings, ch, existing_backups)


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
    Depending on the settings, this will create a full or incremental backup.
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
                  else FullBackup(backup_dir=args.ch.backup_dir))
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

    if len(args.existing_backups) > 1:  # we will never delete if we only have 1 chain
        try:
            clean_old_backups(
                args.existing_backups,
                args.settings('backup.max_full_backups', cast=int, default=2)
            )
        except Exception as e:
            logger.error(f'Error while deleting backup: {e}')
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
            output += click.style(f'{full_backup} @ {full_backup.timestamp}\n\t', fg='cyan')
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
            'To get the restore DB command for a backup call the restore command with the '
            'file name of a backup as the argument.\n'
            'E.g. for the newest one:\n'
        )
        output += click.style(
            f'clickhouse-backup -c {args.config_folder} restore -f {newest_backup.path}',
            fg='green'
        )
        click.echo(output)


@main.command('restore')
@click.option(
    '-f', '--file',
    required=True,
    help='The file to restore. Name has to fully match!'
)
@click.pass_context
def restore_command(ctx, file):
    """
    Generate the restore command for the given backup.
    Use the command in clickhouse-client to restore the backup.
    You can use the output of the list command to view available backups.
    """
    args: CtxArgs = ctx.obj
    backup_to_restore = None
    for full_backup in args.existing_backups.values():
        if str(full_backup.path) == file:
            backup_to_restore = full_backup
            break
        for incremental_backup in full_backup.incremental_backups:
            if str(incremental_backup.path) == file:
                backup_to_restore = incremental_backup
                break
    if not backup_to_restore:
        click.secho(f'No match for {file}! Check the name!\n', file=sys.stderr,
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
        "Force Restore a specific table": {
            "table": "database.table",
            "overwrite": True,
        },
        "Restore a specific table to a new table": {
            "table": "database.table AS database.new_table",
        }
    }
    click.secho(
        f'Execute one of the following queries in clickhouse-client to restore the backup.\n',
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
