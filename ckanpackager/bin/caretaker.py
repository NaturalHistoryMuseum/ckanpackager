#!/usr/bin/env python
# encoding: utf-8
"""
CKAN packager caretaker script - delete files older than 7 days
"""

import os
import time

config = os.environ.get('CKANPACKAGER_CONFIG')

# Time file expires - 7 days ago
EXPIRY_TIME = time.time() - (7 * 86400)

print(config)


def get_symlinked_files():
    """
    Get list of all symlinked files
    @return:
    @rtype:
    """
    symlinked_files = []
    for f in list_files():
        try:
            l = os.readlink(f)
        except OSError:
            continue
        else:
            # Append both the symlink and target to the symlinked files list
            # These will not be deleted by the caretaker script
            symlinked_files.append(f)
            symlinked_files.append(os.path.join(STORE_DIRECTORY, l))

    return symlinked_files


def list_files():
    """
    List all ckanpackager files in directory
    @return:
    @rtype:
    """
    for fn in os.listdir(STORE_DIRECTORY):
        f = os.path.join(STORE_DIRECTORY, fn)
        if os.path.isfile(f):
            yield f


def delete_expired_files():
    """
    Loop through all files, deleting if they are:
      1. older than EXPIRY_TIME
      2. Not symlinked - i.e. GBIF Dump
    @return:
    @rtype:
    """
    symlinked_files = get_symlinked_files()
    for f in list_files():
        if os.stat(f).st_mtime < EXPIRY_TIME and f not in symlinked_files:
            os.remove(f)


if __name__ == "__main__":
    delete_expired_files()
