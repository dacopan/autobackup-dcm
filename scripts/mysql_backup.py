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

# External dependencies.

# Semi-standard module versioning.
__version__ = '1.0'

# Initialize a logger for this module.
logger = logging.getLogger(__name__)

config_file = '../config/mysql_config.json'


def read_config():
    global config_file
    with open(config_file, 'r') as f:
        cfg = json.load(f)
        return cfg


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
            backup_yearly(app)  # now create backup to current app
            # if yearly full backup was created so not need create full backup of this month and week and daily
            app['bk']['last_year'] = current_year
            app['bk']['last_month'] = current_month
            app['bk']['last_week'] = current_week
            app['bk']['last_day'] = current_day
            rotate = True

        elif current_month > app['bk']['last_month']:
            backup_monthly(app)  # now create backup to current app
            # if monthly full backup was created so not need create full backup of this week and daily
            app['bk']['last_month'] = current_month
            app['bk']['last_week'] = current_week
            app['bk']['last_day'] = current_day
            rotate = True

        elif current_week > app['bk']['last_week']:
            backup_weekly(app)  # now create backup to current app
            # if weekly full backup was created so not need create daily backup of this day
            app['bk']['last_week'] = current_week
            app['bk']['last_day'] = current_day
            rotate = True

        elif current_day > app['bk']['last_day']:
            backup_daily(app)  # now create backup to current app
            app['bk']['last_day'] = current_day
            rotate = True

        if rotate:
            rotate_backups(app)  # now rotate backups after backup created and uploaded
            #  if all are correctly now update config file to save the last backup created
            save_last_backup_datetime(app, cfg)

        else:
            print("all backups to '{}' up to date".format(app['cfg']['app_name']))

        print('end backups to \'{}\''.format(app['cfg']['app_name']))

    print('end all backups')


def backup_daily(app):
    print("starting backup_daily to '{}'".format(app['cfg']['app_name']))

    filestamp = time.strftime('%Y-%m-%d_%H-%M')
    backup_file = '{}_{}_{}.{}'.format(app['cfg']['prefix'], filestamp, 'daily', 'gz')
    """
    os.makedirs(os.path.dirname(backup_file), exist_ok=True)
    f = open(backup_file, 'w')
    f.write(backup_file)
    f.close()
    # """
    print("finish backup_daily to '{}': {}".format(app['cfg']['app_name'], backup_file))


def backup_monthly(app):
    print("starting backup_monthly to '{}'".format(app['cfg']['app_name']))

    filestamp = time.strftime('%Y-%m-%d_%H-%M')
    backup_file = '{}_{}_{}.{}'.format(app['cfg']['prefix'], filestamp, 'monthly', 'gz')

    print("finish backup_monthly to '{}': {}".format(app['cfg']['app_name'], backup_file))


def backup_weekly(app):
    print("starting backup_weekly to '{}'".format(app['cfg']['app_name']))

    filestamp = time.strftime('%Y-%m-%d_%H-%M')
    backup_file = '{}_{}_{}.{}'.format(app['cfg']['prefix'], filestamp, 'weekly', 'gz')

    print("finish backup_weekly to '{}': {}".format(app['cfg']['app_name'], backup_file))


def backup_yearly(app):
    print("starting backup_yearly to '{}'".format(app['cfg']['app_name']))

    filestamp = time.strftime('%Y-%m-%d_%H-%M')
    backup_file = '{}_{}_{}.{}'.format(app['cfg']['prefix'], filestamp, 'yearly', 'gz')

    print("finish backup_yearly to '{}': {}".format(app['cfg']['app_name'], backup_file))


def rotate_backups(app):
    print("starting rotate_backups to '{}'".format(app['cfg']['app_name']))

    print("finish rotate_backups to '{}'".format(app['cfg']['app_name']))


def save_last_backup_datetime(app, cfg):
    print("starting save_last_backup_datetime to '{}'".format(app['cfg']['app_name']))

    filestamp = time.strftime('%Y-%m-%d_%H-%M')
    global config_file
    with open(config_file, 'w') as f:
        json.dump(cfg, f, indent=2, sort_keys=False)

    print("finish save_last_backup_datetime to '{}': {}".format(app['cfg']['app_name'], filestamp))


if __name__ == "__main__":
    do_backup()
