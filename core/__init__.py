# autobackup-dcm: core
# this module contains scripts Helper to Rotate Backups and Google Drive
#
# backups should be created and named as: app_name_DATE_HOUR_BACKTYPE.EXTENSION for example:
# jom_2015-12-25_09-58_daily.gz
# Author: dacopanCM <dacopan.bsc@gmail.com>
# URL: https://github.com/dacopan/autobackup-dcm


# Standard library modules.
import json
import logging.config

# Initialize a logger for this module.
with open('../config/logging.json', 'rt') as f:
    config = json.load(f)
    logging.config.dictConfig(config)
