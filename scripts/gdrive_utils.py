#!/usr/bin/env python
# autobackup-dcm: Simple python script to get folder id required to configure automate backup-dcm
# This script help you to retrieve google folder ID necessary to configure autobackup-dcm
# You can retrieve this folder Id trough Google Drive Url
#
# Author: dacopanCM <dacopan.bsc@gmail.com>
# URL: https://github.com/dacopan/autobackup-dcm

# Standard library modules.
import json
import logging.config

# Modules included in our package.
from core.gdrive_dcm import GDriveCM

# Initialize a logger for this module.
log = logging.getLogger('dacopancm.gdrive_utils')

# file to store credentials in ~/.credentials
GOOGLE_CREDENTIALS_NAME = "setup-autobackup-dcm.json"


def main():
    print('Google drive folders in root')
    gdrive = GDriveCM(google_credentials_name=GOOGLE_CREDENTIALS_NAME,
                      google_authorized=False)

    flag = 1
    folder_id = 'root'
    old_folder_id = 'none'
    gfolders = {}
    while flag != 0:

        if folder_id in gfolders.keys():
            folders = gfolders[folder_id]['f']

        else:
            folders = gdrive.get_folders(folder_id)
            gfolders[folder_id] = {'f': folders, 'p': old_folder_id}

        print('\n\n**********Folder:{}**********'.format(folder_id))
        if folder_id != 'root':
            print('Folder:{} {} ({})'.format('[b]', '..', 'go back'))
        if len(folders) < 1:
            print('empty folder')

        for index, folder in enumerate(folders):
            print('Folder:[{}] {} ({})'.format(index, folder.get('name'), folder.get('id')))

        value = input('\n\nEnter number of folder to view child folders, \'b\':back, otherwise exit.\n')
        value = value.strip()
        if value.isnumeric():
            ix = int(value, 10)
            if 0 <= ix < len(folders):
                old_folder_id = folder_id
                folder_id = folders[ix].get("id")
            else:
                flag = 0
        elif value == "b" or value == "back":
            folder_id = old_folder_id
            old_folder_id = gfolders[folder_id]['p']
        else:
            flag = 0


if __name__ == '__main__':
    main()
