import os
import sys
import hashlib

activate_this = os.path.join('/usr/lib/ckanpackager/bin/activate_this.py')
execfile(activate_this, dict(__file__=activate_this))

os.environ['CKANPACKAGER_CONFIG'] = '/etc/ckanpackager/ckanpackager_settings.py'
from ckanpackager.application import app as application
