"""
config handling for dynaconf
"""
import logging
import os
import sys
from argparse import ArgumentParser, Namespace
from importlib.resources import files
from pathlib import Path
from typing import Tuple

from dynaconf import Dynaconf, Validator

from clickhouse_backup.clickhouse.client import BackupTarget


def parse_config() -> Tuple[Dynaconf, Namespace]:
    """
    Parse config with dynaconf and argparse
    :return: 2-Tuple[Dynaconf, Namespace]
    """
    ap = ArgumentParser(description='Create backups of your clickhouse database. '
                                    'Docs: https://github.com/Hedius/clickhouse-backup')
    ap.add_argument('-c', '--config-folder',
                    help='Folder where the config files are stored. '
                         'E.g.: /etc/clickhouse-backup',
                    required=True,
                    dest='config_folder')
    ap.add_argument('-f', '--force-full',
                    help='Force a full backup. And ignore any rules for incremental backups.',
                    dest='force_full',
                    action='store_true')

    args = ap.parse_args()

    config_folder = Path(args.config_folder)
    default_config = config_folder / 'default.toml'
    if not os.path.isfile(default_config):
        try:
            config_folder.mkdir(parents=True, exist_ok=True)
            with open(default_config, 'w', encoding='utf-8') as f:
                f.write(files('clickhouse_backup.data').joinpath('default.toml').read_text())
        except Exception as e:
            logging.exception(f'Failed to create default config {args.config_folder}!', e)
            sys.exit(1)

    settings = Dynaconf(
        envvar_prefix='CH_BACKUP',
        # environments=True,
        settings_files=['default.toml', 'config.toml'],
        root_path=str(args.config_folder),
        merge_enabled=True,
        validators=[
            # Validator('clickhouse.host', must_exist=True),
            Validator('clickhouse.port', cast=int, default=9000),
            # Validator('clickhouse.user', must_exist=True),
            # Validator('clickhouse.password', must_exist=True),
            Validator('backup.target', must_exist=True, cast=BackupTarget),
            Validator('backup.max_incremental_backups', cast=int, default=6),
            Validator('backup.max_full_backups', cast=int, default=2),
            # Validator('logging.dir'),
        ]
    )
    # todo not working in debian 12 with 3.1.7
    # fine -> client also validates what it needs
    # match settings.backup.target:
    #     case BackupTarget.FILE:
    #         target_validators = [
    #             Validator('backup.dir', must_exist=True, cast=Path),
    #         ]
    #     case BackupTarget.DISK:
    #         target_validators = [
    #             Validator('backup.dir', must_exist=True, cast=Path),
    #             Validator('backup.disk', must_exist=True),
    #         ]
    #     case BackupTarget.S3:
    #         target_validators = [
    #             Validator('backup.s3.endpoint', must_exist=True),
    #             Validator('backup.s3.access_key_id', must_exist=True),
    #             Validator('backup.s3.secret_access_key', must_exist=True),
    #         ]
    #     case _:
    #         target_validators = []
    # settings.validators.extend(target_validators)
    # settings.validators.validate_all()
    return settings, args.force_full
