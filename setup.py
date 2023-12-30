from setuptools import setup

setup(
    name='clickhouse_backup',
    version='0.0.1',
    url='https://github.com/Hedius/clickhouse-backup',
    license='GPLv3',
    author='Hedius',
    author_email='git@hedius.eu',
    description='A simple python wrapper around clickhouse to use the BACKUP '
                'command for creating backups of your database.',
    packages=['clickhouse_backup'],
    entry_points={
        'console_scripts': ['clickhouse-backup=clickhouse_backup.clickhouse_backup:main'],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Archiving :: Backup',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3',
    ]
)
