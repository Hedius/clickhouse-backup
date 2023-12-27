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
"""