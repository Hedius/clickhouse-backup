__AUTHOR__ = 'Hedius'
__VERSION__ = '0.0.1'
__EMAIL__ = 'clickhouse-backup@hedius.eu'
__LICENSE__ = 'GPLv3'

import os
from pathlib import Path

from setuptools import find_namespace_packages, setup

with open(Path(__file__).parent / 'README.md') as f:
    readme = f.read()

package_data = {
    'clickhouse_backup.data': ['*.toml', '*.service'],
}

data_files = None
if os.environ.get('DEB_BUILD') in ('1', 'true', 'True'):
    data_files = [
        ('/etc/clickhouse-backup',
         ['debian/default.toml']),
        ('/lib/systemd/system',
         ['debian/clickhouse-backup.service', 'debian/clickhouse-backup.timer']),
        ('/usr/share/clickhouse-backup',
         ['debian/backup_storage.xml',
          # this is dirty, but stdeb will not copy them... so gotta put them somewhere.
          'debian/clickhouse-backup.postinst',
          'debian/clickhouse-backup.postrm'])
    ]


setup(
    name='clickhouse_backup',
    python_requires=">=3.10",
    version=__VERSION__,
    url='https://github.com/Hedius/clickhouse-backup',
    license=__LICENSE__,
    author=__AUTHOR__,
    author_email=__EMAIL__,
    maintainer=__AUTHOR__,
    maintainer_email=__EMAIL__,
    description='A python wrapper around ClickHouse to use the BACKUP '
                'command for creating backups of your database.',
    long_description=readme,
    long_description_content_type='text/markdown',
    # Not needed / using auto discovery
    package_dir={'': 'src'},
    packages=find_namespace_packages(where='src'),
    package_data=package_data,
    entry_points={
        'console_scripts': ['clickhouse-backup=clickhouse_backup.backup:main'],
    },
    install_requires=[
        'clickhouse-driver>=0.2.5',
        'dynaconf>=3.1.7',
        'loguru>=0.6.0'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Archiving :: Backup',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3',
    ],
    data_files=data_files
)
