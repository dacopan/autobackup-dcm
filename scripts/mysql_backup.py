#!/usr/bin/env python
# autobackup-dcm: Simple python script to autobackup, rotate backup and upload to google drive.
# this script create one daily incremental backups (if you need), and one full backup each week, month and year
#
# backups should be created and named as: app_name_DATE_HOUR_BACKTYPE.EXTENSION for example:
# jom_2015-12-25_09-58_daily.gz
# Author: dacopanCM <dacopan.bsc@gmail.com>
# URL: https://github.com/dacopan/autobackup-dcm

# Standard library modules.
import logging.config
import os
import time

# External dependencies.
from humanfriendly import format_path, Timer
from executor import execute, ExternalCommandFailed

# Modules included in our package.
from core.generic_backup import GenericBackupCM

# Semi-standard module version.
__version__ = '1.0'

log = logging.getLogger('dacopancm.mysql')

# constants
CONFIG_FILE = '../config/mysql_config.json'


class MysqlBackupCM(GenericBackupCM):
    def __init__(self):
        super().__init__(log, CONFIG_FILE)

    def do_backup(self, app, backup_type):
        if backup_type == 'yearly':
            return self.create_full_backup(app, backup_type)
        elif backup_type == 'monthly':
            return self.create_full_backup(app, backup_type)
        elif backup_type == 'weekly':
            return self.create_full_backup(app, backup_type)
        elif backup_type == 'daily':
            return self.create_full_backup(app, backup_type)
        else:
            return False

    def create_full_backup(self, app, backup_type):
        """Create a full backup of a database defined in attr:´app´

        :param app: :class:`dict` with configuration returned by :func:`read_config()`
        :param backup_type: the key backup type to include in backup filename: daily, weekly, monthly, yearly
        :return: ``True`` if local and remote backup created correctly, ``False`` otherwise
        """
        log.info("starting full backup_{} to '{}'".format(backup_type, app['cfg']['app_name']))

        filestamp = time.strftime('%Y-%m-%d_%H-%M')

        backup_file = '{}{}_{}_{}.{}'.format(app['cfg']['local_backup_dir'], app['cfg']['app_name'], filestamp,
                                             backup_type,
                                             'gz')

        # here create backup
        try:
            os.makedirs(os.path.dirname(backup_file), exist_ok=True)
            timer = Timer()

            mysql_cmd = '/opt/lamp/mysql/bin/mysqldump '
            mysql_cmd += '--opt --triggers --events --user={} --password={} --databases {}'.format(
                app['custom']['db_user'], app['custom']['db_password'], app['custom']['db_name'])

            gzip_cmd = 'gzip -c > {}'.format(format_path(backup_file))

            mysql_cmd_res = execute(mysql_cmd, logger=log, capture=True)
            cmd_result = execute(gzip_cmd, logger=log, input=mysql_cmd_res, error_message='error en gzip')

            if cmd_result:
                log.info(
                    "finish full backup_{} to '{}:{} in {}'".format(backup_type, app['cfg']['app_name'],
                                                                    format_path(backup_file), timer))
                cmd_result = self.upload_backup(app, backup_file)
            else:
                log.error("error creating full backup_{} to '{}'".format(backup_type, app['cfg']['app_name']))
            return cmd_result
        except ExternalCommandFailed as ex:
            log.error(
                "error creating full backup_{} to '{} :{}'".format(backup_type, app['cfg']['app_name'],
                                                                   ex.error_message))

    def create_incremental_backup(self, app, backup_type):
        """Create a incremental backup of a database defined in attr:´app´

            :param app: :class:`dict` with configuration returned by :func:`read_config()`
            :param backup_type: the key backup type to include in backup filename: daily, weekly, monthly, yearly
            :return: ``True`` if local and remote backup created correctly, ``False`` otherwise
        """

        log.info("starting incremental backup_{} to '{}'".format(backup_type, app['cfg']['app_name']))

        filestamp = time.strftime('%Y-%m-%d_%H-%M')
        backup_file = '{}{}_{}_{}.{}'.format(app['cfg']['local_backup_dir'], app['cfg']['app_name'], filestamp,
                                             backup_type,
                                             'gz')
        # TODO here create incremental backup if you need

        self.upload_backup(app, backup_file)

        log.info("finish incremental backup_{} to '{}:{}'".format(backup_type, app['cfg']['app_name'], backup_file))


if __name__ == "__main__":
    MysqlBackupCM().run_backups()
