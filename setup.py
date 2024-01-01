from setuptools import setup, find_namespace_packages

from pathlib import Path
root = Path(__file__).parent
with open(root / 'requirements.txt') as f:
    requirements = f.read().splitlines()

with open(root / 'README.md') as f:
    readme = f.read()

setup(
    name='clickhouse_backup',
    version='0.0.1',
    url='https://github.com/Hedius/clickhouse-backup',
    license='GPLv3',
    author='Hedius',
    author_email='clickhouse-backup@hedius.eu',
    description='A python wrapper around ClickHouse to use the BACKUP '
                'command for creating backups of your database.',
    long_description=readme,
    long_description_content_type='text/markdown',
    packages=find_namespace_packages(include=['clickhouse_backup.*']),
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
    ]
)
