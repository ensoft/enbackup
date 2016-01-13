#!/usr/bin/env python
#
# EnBackup setup file
#

from distutils.core import setup

setup(name='enbackup',
      version="1.4.15",
      description='EnBackup: backup tools based on rdiff-backup',
      packages=['enbackup', 'enbackup.cmd'],
      package_dir={'': 'src'},
      scripts=['bin/enbackup',
               'bin/enbackup-archive.py',
               'bin/enbackup-rdiff-to-mirror.py',
               'bin/enbackup-sata-poll.py',
               'bin/enbackup-sata-insert.sh',
               'bin/enbackup-sata-remove.sh',
               'bin/backup_breakers.sh',
               'bin/enbackup-suspend-usb-device.sh'],
      data_files=[('/etc/enbackup.d',
                   ['etc/enbackup-archive.rc',
                    'etc/enbackup-mirror.rc',
                    'etc/enbackup.rc']),
                  ('/usr/share/doc/enbackup/examples/',
                   ['etc/enbackup.rules',
                    'etc/enbackup-mysql-dump.cron',
                    'etc/enbackup-ldap-dump.cron',
                    'etc/enbackup-python-dump.cron',
                    'etc/enbackup.cron',
                    'etc/enbackup-installed-packages.cron',
                    'etc/enbackup_unaged.rc',
                    'etc/enbackup_aged.rc']),
                  ('/usr/share/doc/enbackup/',
                   ['doc/README'])
                  ]
      )
