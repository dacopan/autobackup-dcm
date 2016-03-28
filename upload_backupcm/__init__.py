#!/usr/bin/env python
# upload-backupcm: Simple python script to get, delete and upload files to google drive.
#
# Author: dacopanCM <peter@peterodding.com>
# Last Change: March 27, 2016
# URL: https://github.com/dacopan/autobackup-dcm

from __future__ import print_function
# Standard library modules.
import os
import sys
import logging
import httplib2

# google library
from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
from apiclient.http import MediaFileUpload

try:
    import argparse

    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# Semi-standard module versioning.
__version__ = '1.0'

# Initialize a logger for this module.
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger(__name__)

SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = '../config/google-client_secret.json'
APPLICATION_NAME = 'Autobackup DCM'


class GDriveCM(object):
    """Python API for the ``GDriveCM`` program."""

    def __init__(self, app):
        """
        Construct a :class:`RotateBackups` object.

        :param include_list: A list of strings with :mod:`fnmatch` patterns. If a
                             nonempty include list is specified each backup must
                             match a pattern in the include list, otherwise it
                             will be ignored.
        :param exclude_list: A list of strings with :mod:`fnmatch` patterns. If a
                             backup matches the exclude list it will be ignored,
                             *even if it also matched the include list* (it's the
                             only logical way to combine both lists).
        :param dry_run: If this is ``True`` then no changes will be made, which
                        provides a 'preview' of the effect of the rotation scheme
                        (the default is ``False``). Right now this is only useful
                        in the command line interface because there's no return
                        value.
        :param io_scheduling_class: Use ``ionice`` to set the I/O scheduling class
                                    (expected to be one of the strings 'idle',
                                    'best-effort' or 'realtime').
        """
        self.app = app
        self.google_credentials_name = app['cfg']['google_credentials_name']
        self.remote_folder = app['cfg']['remote_backup_dir']
        self.google_authorized = app['cfg']['google_authorized']

    def get_credentials(self):
        """Gets valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.

        Returns:
            Credentials, the obtained credential.
        """
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir,
                                       self.google_credentials_name)

        store = oauth2client.file.Storage(credential_path)
        credentials = store.get()
        if (not credentials or credentials.invalid) and not self.google_authorized:
            log.info("requesting credentials to Google Drive")
            flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
            flow.user_agent = APPLICATION_NAME
            flow.params['access_type'] = 'offline'
            if flags:
                credentials = tools.run_flow(flow, store, flags)
            else:  # Needed only for compatibility with Python 2.6
                credentials = tools.run(flow, store)
            log.info('Storing credentials to ' + credential_path)
        return credentials

    def get_service(self):
        credentials = self.get_credentials()
        http = credentials.authorize(httplib2.Http())
        service = discovery.build('drive', 'v3', http=http)
        return service

    def get_folders(self, parent_id='root'):
        log.info('get_folders Gdrive Api')

        service = self.get_service()
        page_token = None
        folders = []
        while True:
            response = service.files().list(
                q="mimeType='application/vnd.google-apps.folder' and '{}' in parents".format(parent_id),
                spaces='drive',
                fields='nextPageToken, files(id, name)',
                pageToken=page_token).execute()
            files = response.get('files', [])
            for file in files:
                # Process change
                folders.append(file)

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break;

        return folders

    def upload_file(self, file):
        service = self.get_service()
        filename = os.path.basename(file)
        self.insert_file(service, filename, file)

    def insert_file(self, service, filename, file, mime_type=None, description=None):
        """Insert new file.

        Args:
          service: Drive API service instance.
          description: Description of the file to insert.
          mime_type: MIME type of the file to insert.
          filename: Filename of the file to insert.
          file: backup file to upload
        Returns:
          Inserted file metadata if successful, None otherwise.
        """
        try:
            metadata = {'name': filename, 'parents': [self.remote_folder]}

            if mime_type:
                metadata['mimeType'] = mime_type

            media = MediaFileUpload(file, mime_type, resumable=True)

            res = service.files().create(body=metadata, media_body=media, fields='id').execute()

            if res:
                print('Uploaded "%s" (%s)' % (filename, res['id']))

                # Uncomment the following line to print the File ID
                # print 'File ID: %s' % file['id']

        except:
            e = sys.exc_info()[0]
            log.error('An error occurred: %s', e)


def main():
    GDriveCM(
        {
            "cfg": {
                "local_backup_dir": "../backup_test_files/jom_backups/",
                "prefix": "jom",
                "remote_backup_dir": "/backups/jom_backups/",
                "app_name": "jom",
                "google_credentials_name": "jom_google_credentials.json",
                "google_authorized": False
            }
        }
    ).testx()


if __name__ == '__main__':
    main()
