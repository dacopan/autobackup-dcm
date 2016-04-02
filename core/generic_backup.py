#!/usr/bin/env python
# autobackup-dcm: Simple python script to autobackup, rotate backup and upload to google drive.
# this script define a generic class that provide generic extensible to create backups
#
# backups should be created and named as: app_name_DATE_HOUR_BACKTYPE.EXTENSION for example:
# jom_2015-12-25_09-58_daily.gz
# Author: dacopanCM <dacopan.bsc@gmail.com>
# URL: https://github.com/dacopan/autobackup-dcm

# Standard library modules.
import datetime
import json
import time

# External dependencies.

# Modules included in our package.

# Semi-standard module version.
from core.rotate_dcm import RotateBackupsCM
from core.gdrive_dcm import GDriveCM


class GenericBackupCM:
    def __init__(self, log, config_file):
        self.log = log
        self.CONFIG_FILE = config_file

    def read_config(self):
        """Read configuration of app to backup from json file defined by `~CONFIG_FILE`

        :return: :class:`list` with all apps to backup loaded from json
        """
        with open(self.CONFIG_FILE, 'r') as f:
            cfg = json.load(f)
            return cfg

    def save_last_backup_datetime(self, app, cfg):
        """
        Save in the app config file the timestamp of the last successful backup

        :param app: :class:`dict` with configuration returned by :func:`read_config()` modified by
        :param cfg: the modified version of cfg returned by :func:`~read_config()`.
        """
        self.log.info("starting save_last_backup_datetime to '{}'".format(app['cfg']['app_name']))

        filestamp = time.strftime('%Y-%m-%d_%H-%M')

        with open(self.CONFIG_FILE, 'w') as f:
            json.dump(cfg, f, indent=2, sort_keys=False)

        self.log.info("finish save_last_backup_datetime to '{}': {}".format(app['cfg']['app_name'], filestamp))

    def rotate_backups(self, app):
        """ Rotate backups in local and remote directories of this app

        :param app: :class:`dict` with configuration returned by :func:`read_config()`
        """
        self.log.info("starting rotate_backups to '{}'".format(app['cfg']['app_name']))

        RotateBackupsCM(
            rotation_scheme=app['rotate']['local'],
            include_list=app['rotate']['include_list'],
            exclude_list=app['rotate']['exclude_list'],
            dry_run=app['rotate']['dry_run'],
            io_scheduling_class=app['rotate']['ionice'],
            rotate_type='local'
        ).rotate_backups(app['cfg']['local_backup_dir'])

        RotateBackupsCM(
            rotation_scheme=app['rotate']['remote'],
            include_list=app['rotate']['include_list'],
            exclude_list=app['rotate']['exclude_list'],
            dry_run=app['rotate']['dry_run'],
            io_scheduling_class=app['rotate']['ionice'],
            rotate_type='remote',
            gdrivecm=GDriveCM(google_credentials_name=app['cfg']['google_credentials_name'],
                              google_authorized=app['cfg']['google_authorized'],
                              remote_folder=app['cfg']['remote_backup_dir'])
        ).rotate_backups(app['cfg']['remote_backup_dir'])

        self.log.info("finish rotate_backups to '{}'".format(app['cfg']['app_name']))

    def upload_backup(self, app, backup_file):
        """ Upload backup_file to Google Drive

        :param app: :class:`dict` with configuration returned by :func:`read_config()`
        :param backup_file: the local path of backup_file to upload
        :return: True if upload otherwise False
        """
        self.log.debug("uploading %s", backup_file)
        try:
            res = GDriveCM(google_credentials_name=app['cfg']['google_credentials_name'],
                           google_authorized=app['cfg']['google_authorized'],
                           remote_folder=app['cfg']['remote_backup_dir']
                           ).upload_file(backup_file)

            if res:
                self.log.info("uploaded %s", backup_file)
            else:
                self.log.info("error uploading %s", backup_file)

            return res

        except:
            self.log.error("Error uploading %s", backup_file)
            return False

    def run_backups(self):
        self.log.info('starting all backups')
        cfg = self.read_config()

        # get current time to determinate type of backup
        now = datetime.datetime.now()
        current_year = now.year
        current_month = now.month
        current_week = now.isocalendar()[1]
        current_day = now.day

        for app in cfg:
            rotate = False
            # now determine type of backup and run it
            if current_year > app['bk']['last_year']:
                backup_created = self.do_backup(app, 'yearly')  # now create backup to current app
                # if yearly full backup was created so not need create full backup of this month and week and daily
                if backup_created:
                    app['bk']['last_year'] = current_year
                    app['bk']['last_month'] = current_month
                    app['bk']['last_week'] = current_week
                    app['bk']['last_day'] = current_day
                    rotate = True

            elif current_month > app['bk']['last_month']:
                backup_created = self.do_backup(app, 'monthly')  # now create backup to current app
                # if monthly full backup was created so not need create full backup of this week and daily
                if backup_created:
                    app['bk']['last_month'] = current_month
                    app['bk']['last_week'] = current_week
                    app['bk']['last_day'] = current_day
                    rotate = True

            elif current_week > app['bk']['last_week']:
                backup_created = self.do_backup(app, 'weekly')  # now create backup to current app
                # if weekly full backup was created so not need create daily backup of this day
                if backup_created:
                    app['bk']['last_week'] = current_week
                    app['bk']['last_day'] = current_day
                    rotate = True

            elif current_day > app['bk']['last_day']:
                backup_created = self.do_backup(app, 'daily')  # now create backup to current app
                if backup_created:
                    app['bk']['last_day'] = current_day
                    rotate = True

            if rotate:
                self.rotate_backups(app)  # now rotate backups after backup created and uploaded
                #  if all are correctly now update config file to save the last backup created
                self.save_last_backup_datetime(app, cfg)

            else:
                self.log.info("No rotate all backups to '{}' up to date".format(app['cfg']['app_name']))

            self.log.info('end backups to \'{}\''.format(app['cfg']['app_name']))

        self.log.info('end all backups')

    def do_backup(self, app, backup_type):
        return False
