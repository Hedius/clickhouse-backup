__AUTHOR__ = 'Hedius'
__VERSION__ = '2.0.3'
__EMAIL__ = 'clickhouse-backup@hedius.eu'
__LICENSE__ = 'GPLv3'

import os
import re
from pathlib import Path

from setuptools import find_namespace_packages, setup

with open(Path(__file__).parent / 'README.md') as f:
    lines = f.readlines()
    filtered = [
        x for x in lines
        if not re.match(r'^[\[!]{2}', x) and len(x) > 0
    ]
    readme = ''.join(filtered)

with open(Path(__file__).parent / 'requirements.txt') as f:
    requirements = f.read()

package_data = {
    'clickhouse_backup.data': ['*.toml', '*.service'],
}

data_files = None
if os.environ.get('DEB_BUILD') in ('1', 'true', 'True'):
    data_files = [
        ('/etc/clickhouse-backup',
         ['debian/default.toml']),
        ('/usr/share/clickhouse-backup',
         ['debian/backup_storage.xml']),
        ('/usr/share/doc/clickhouse-backup', ['README.md']),
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
    description='Backup tool for the column-based database ClickHouse.',
    long_description=readme.split('## Installation')[0].split('# clickhouse-backup')[-1].strip(),
    long_description_content_type='text/markdown',
    # Not needed / using auto discovery
    package_dir={'': 'src'},
    packages=find_namespace_packages(where='src'),
    package_data=package_data,
    entry_points={
        'console_scripts': ['clickhouse-backup=clickhouse_backup.run:main'],
    },
    install_requires=requirements,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Archiving :: Backup',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3',
    ],
    data_files=data_files
)
