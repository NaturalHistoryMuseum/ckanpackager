import os
import sys
import hashlib

activate_this = os.path.join('/usr/lib/ckan/ckanpackager/bin/activate_this.py')
execfile(activate_this, dict(__file__=activate_this))

os.environ['CKANPACKAGER_CONFIG'] = '/etc/ckan/ckanpackager_settings.py'
from ckanpackager.application import app as application
