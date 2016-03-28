#!/usr/bin/env python
# autobackup-dcm: Simple python script to autobackup, rotate backup and upload to google drive.
# this script create one daily incremental backups, and one full backup each week, month and year
#
# backups should be created and named as: prefix_DATE_HOUR_BACKTYPE.EXTENSION
#
# Author: dacopanCM <peter@peterodding.com>
# Last Change: March 27, 2016
# URL: https://github.com/dacopan/autobackup-dcm

# Standard library modules.
import os
import time
import datetime
import json
import logging
import sys

# External dependencies.

# Modules included in our package.
from rotate_backupcm import coerce_retention_period, RotateBackupsCM
from upload_backupcm import GDriveCM

# Semi-standard module versioning.
__version__ = '1.0'

# Initialize a logger for this module.
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger(__name__)

CONFIG_FILE = '../config/mysql_config.json'


def read_config():
    global CONFIG_FILE
    with open(CONFIG_FILE, 'r') as f:
        cfg = json.load(f)
        return cfg


def save_last_backup_datetime(app, cfg):
    print("starting save_last_backup_datetime to '{}'".format(app['cfg']['app_name']))

    filestamp = time.strftime('%Y-%m-%d_%H-%M')
    global CONFIG_FILE
    with open(CONFIG_FILE, 'w') as f:
        json.dump(cfg, f, indent=2, sort_keys=False)

    print("finish save_last_backup_datetime to '{}': {}".format(app['cfg']['app_name'], filestamp))


def rotate_backups(app):
    print("starting rotate_backups to '{}'".format(app['cfg']['app_name']))

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
        gdrivecm=GDriveCM(app)
    ).rotate_backups(app['cfg']['remote_backup_dir'])

    print("finish rotate_backups to '{}'".format(app['cfg']['app_name']))


def upload_backup(app, backup_file):
    log.info("uploading %s", backup_file)
    try:
        GDriveCM(app).upload_file(backup_file)
        log.info("uploaded %s", backup_file)

    except:
        log.error("error uploading %s", backup_file)


def do_backup():
    print('starting all backups')
    cfg = read_config()

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
            create_full_backup(app, 'yearly')  # now create backup to current app
            # if yearly full backup was created so not need create full backup of this month and week and daily
            app['bk']['last_year'] = current_year
            app['bk']['last_month'] = current_month
            app['bk']['last_week'] = current_week
            app['bk']['last_day'] = current_day
            rotate = True

        elif current_month > app['bk']['last_month']:
            create_full_backup(app, 'monthly')  # now create backup to current app
            # if monthly full backup was created so not need create full backup of this week and daily
            app['bk']['last_month'] = current_month
            app['bk']['last_week'] = current_week
            app['bk']['last_day'] = current_day
            rotate = True

        elif current_week > app['bk']['last_week']:
            create_full_backup(app, 'weekly')  # now create backup to current app
            # if weekly full backup was created so not need create daily backup of this day
            app['bk']['last_week'] = current_week
            app['bk']['last_day'] = current_day
            rotate = True

        elif current_day > app['bk']['last_day']:
            create_incremental_backup(app, 'daily')  # now create backup to current app
            app['bk']['last_day'] = current_day
            rotate = True

        if rotate:
            rotate_backups(app)  # now rotate backups after backup created and uploaded
            #  if all are correctly now update config file to save the last backup created
            # save_last_backup_datetime(app, cfg)

        else:
            print("all backups to '{}' up to date".format(app['cfg']['app_name']))

        print('end backups to \'{}\''.format(app['cfg']['app_name']))

    print('end all backups')


def create_full_backup(app, backup_type):
    print("starting full backup_{} to '{}'".format(backup_type, app['cfg']['app_name']))

    # filestamp = time.strftime('%Y-%m-%d_%H-%M')
    filestamp = '2016-03-28_09-17'
    backup_file = '{}{}_{}_{}.{}'.format(app['cfg']['local_backup_dir'], app['cfg']['prefix'], filestamp, backup_type,
                                         'gz')

    # here create backup
    """
    os.makedirs(os.path.dirname(backup_file), exist_ok=True)
    f = open(backup_file, 'w')
    f.write(backup_file)
    f.close()
    # """

    print("finish incremental backup_{} to '{}:{}'".format(backup_type, app['cfg']['app_name'], backup_file))
    upload_backup(app, backup_file)


def create_incremental_backup(app, backup_type):
    print("starting incremental backup_{} to '{}'".format(backup_type, app['cfg']['app_name']))

    filestamp = time.strftime('%Y-%m-%d_%H-%M')
    backup_file = '{}{}_{}_{}.{}'.format(app['cfg']['local_backup_dir'], app['cfg']['prefix'], filestamp, backup_type,
                                         'gz')

    # here create backup
    """os.makedirs(os.path.dirname(backup_file), exist_ok=True)
        f = open(backup_file, 'w')
        f.write(backup_file)
        f.close()
        """

    print("finish incremental backup_{} to '{}:{}'".format(backup_type, app['cfg']['app_name'], backup_file))

    upload_backup(app, backup_file)


if __name__ == "__main__":
    do_backup()
