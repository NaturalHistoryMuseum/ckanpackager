"""Ckanpackager service

Run a web service to access statistics, perform actions and queue tasks
"""
import os

from flask import Flask, g
from ckanpackager.controllers.status import status
from ckanpackager.controllers.actions import actions
from ckanpackager.controllers.packagers import packagers
from ckanpackager.controllers.error_handlers import error_handlers

import os
import time

# Create the application
app = Flask(__name__)

# Read configuration
app.config.from_object('ckanpackager.config_defaults')
app.config.from_envvar('CKANPACKAGER_CONFIG')


class Caretaker(object):
    '''
    Script to remove expired packages after a few days
    '''

    expiry_date = time.time() - (7 * 86400)

    def __init__(self):
        global app
        self.dir = app.config['STORE_DIRECTORY']

    def _get_symlinked_files(self):
        """
        Get list of all symlinked files
        @return:
        @rtype:
        """
        symlinked_files = []
        for f in self._list_files():
            try:
                l = os.readlink(f)
            except OSError:
                continue
            else:
                # Append both the symlink and target to the symlinked files list
                # These will not be deleted by the caretaker script
                symlinked_files.append(f)
                symlinked_files.append(os.path.join(self.dir, l))

        return symlinked_files

    def _list_files(self):
        """
        List all ckanpackager files in directory
        @return:
        @rtype:
        """
        for fn in os.listdir(self.dir):
            f = os.path.join(self.dir, fn)
            if os.path.isfile(f):
                yield f

    def delete_expired_files(self):
        """
        Loop through all files, deleting if they are:
          1. older than EXPIRY_TIME
          2. Not symlinked - i.e. GBIF Dump
        @return:
        @rtype:
        """
        symlinked_files = self._get_symlinked_files()
        for f in self._list_files():
            # Delete all files with creation date greater than expires after
            # And not a symlink (the gbif export is symlinked and stays until reproduced)
            if os.stat(f).st_mtime < self.expiry_date and f not in symlinked_files:
                os.remove(f)


def run():
    """
    Initiate and call delete expired files
    """
    Caretaker().delete_expired_files()


if __name__ == '__main__':
    run()
