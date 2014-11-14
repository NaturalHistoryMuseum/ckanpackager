ckanpackager
============
[![Build Status](https://travis-ci.org/NaturalHistoryMuseum/ckanpackager.svg?branch=master)](https://travis-ci.org/NaturalHistoryMuseum/ckanpackager) [![Coverage Status](https://img.shields.io/coveralls/NaturalHistoryMuseum/ckanpackager.svg)](https://coveralls.io/r/NaturalHistoryMuseum/ckanpackager)

Overview
--------

A stand-alone service that can be instructed to fetch data on a [CKAN](http://ckan.org) site (either a file or data fetched using the datastore API), pack the data in a ZIP file and email the link to a given address.

This is useful for allowing people to download very large datasets (tested on a dataset of 3,000,000 rows), and there is a corresponding CKAN extension, [ckanext-ckanpackager](http://github.com/NaturalHistoryMuseum/ckanext-ckanpackager), to provide an alternative download button on resources.

Features:
- Works for file resources as well resources uploaded to the datastore;
- Can apply filters and full text queries when fetching from the datastore;
- Configurable number of workers and a queuing system ensures administrators can control the resources used;
- Data is processed as it is streamed from CKAN, so the memory usage is kept low;

Note: ckanpackager streams the info and drops the connection as soon as it has the data it wants. This means your CKAN server might show broken pipe errors. That's fine.


Deployment
----------

If you want to test ckanpackager, you can run it manually by doing:

`CKANPACKAGER_CONFIG=[path to config file] python ckanpackager/application.py`

For production, you will want to use an Apache server with mod_wsgi enabled, and use the following files:

- `deployment/ckanpackager`: An example Apache2 virtual host file. Typically goes under `/etc/apache2/sites-available`;
- `deployment/ckanpackager.wsgi`: A WSGI wrapper for ckanpackager. If using the default virtual host example this would be placed in `/etc/ckan/ckanpackager.wsgi`;
- `deployment/ckanpackager_settings.py`: An example configuration file (see below for options). If using the default wsgi wrapper, this would be placed in `/etc/ckan/ckanpackager_settings.py`

Usage
-----

The service provides two HTTP access points:

### /

Return the current status of the packager service. This expects a POST request, and returns a JSON dictionary.

Parameters:
- `secret`: The shared secret (required). This is only secure over HTTPS.

JSON result fields:
- `worker_count`: Number of workers;
- `queue_length`: Number of items in the queue (items that have not been completed);
- `processed_requests`: Number of requests that were processed.

Example usage (Python):
```python
  import urllib2
  import json
  
  request = urllib2.Request('http://ckanpackager.example.com/status')
  response = urllib2.urlopen(request, urllib.quote(json.dumps({
      'secret': '...'
  })))
  result = json.loads(response.read())
  response.close()
```

### package_datastore

This expects a POST request, and returns a JSON dictionary. If the request is successful, the task is queued up. When the tasks gets to run, it will fetch the given resource (with filters applied), pack it into a ZIP file and email the link to the given email address.

Parameters:
- `secret`: The shared secret (required). This is only secure over HTTPS;
- `api_url`: The CKAN datastore_search API URL (required);
- `resource_id`: The resource to package (required);
- `email`: The email to send the resource to (required)l
- `filters`: JSON encoded filter dictionary, as expected by datastore_search (optional, default is no filters);
- `q`: Full text search (optional, default is no full text search);
- `offset`: Offset to start the search at (optional, default is 0);
- `limit`: Maximum number of items to fetch (optional, default is to fetch all entries);
- `key`: CKAN API key (optional, default if to do anonymous request)
      
JSON result fields:      
- `success`: True or False;
- `msg`: If the query failed, may contain an error message. If the query was successful, contains a message that may be displayed to the end user (such as "please be patient!");

Example usage (Python):
```python
  import urllib2
  import json
  
  request = urllib2.Request('http://ckanpackager.example.com/package_datastore')
  response = urllib2.urlopen(request, urllib.quote(json.dumps({
      'secret': '...',
      'api_url': 'http://ckan.example.com/api/action/datastore_search',
      'resource_id': '...',
      'email': 'recipient@example.com',
      'filters': json.dumps({'somefield': 'somevalue'}),
      'q': 'a search',
      'offset': 1000,
      'limit': 1000000,
      'key' : '...'
  })))
  result = json.loads(response.read())
  response.close()
```

### package_url
This expects a POST request, and returns a JSON dictionary. If the request is successful, the task is queued up. When the tasks gets to run, it will fetch the given resource file, put it into a ZIP file and email the link to the given email address.

Parameters:
- `secret`: The shared secret (required). This is only secure over HTTPS;
- `resource_id`: The resource to package (required);
- `resource_url`: The URL at which the file can be found(required);
- `email`: The email to send the resource to (required);
- `key`: CKAN API key (optional, default if to do anonymous request)
      
JSON result fields:      
- `success`: True or False;
- `msg`: If the query failed, may contain an error message. If the query was successful, contains a message that may be displayed to the end user (such as "please be patient!");

Example usage (Python):
```python
  import urllib2
  import json
  
  request = urllib2.Request('http://ckanpackager.example.com/package_url')
  response = urllib2.urlopen(request, urllib.quote(json.dumps({
      'secret': '...',
      'resource_id': '...',
      'resource_url': 'http://ckan.example.com/resoure_file',
      'email': 'recipient@example.com',
      'key' : '...'
  })))
  result = json.loads(response.read())
  response.close()
```

### ckanpackager command line tool

Ckanpackager also comes with a command line tool for sending requests to a ckanpackager instance:

```
# ckanpackager -h
Ckanpackager command line utility

This helps build and send requests to a ckanpackager instance.

Usage: ckanpackager [options] status
       ckanpackager [options] (cc|clear-cache)
       ckanpackager [options] queue TASK [PARAM:VALUE ...]

Options:
    -h --help       Show this screen.
    --version       Show version.
    -q              Quiet. Don't output anything.
    -p HOST         The ckanpackager host [default: http://127.0.0.1:8765]
    -d FILE         Path to JSON file containing default values for
                    parameters. Useful for specifying the secret and
                    api_url. If not specified, then ckanpackager-cli will look
                    for /etc/ckan/ckanpackager-cli.json and use that if present.
                    Example:
                    {"secret": "...", "api_url": "http://.../api/3/action/datastore_search"}
    -s SECRET       The secret key. If present this will override the secret
                    key in the default file (but any secret defined in the
                    PARAM:VALUE parameters will override this one)
```

Configuration
-------------

The configuration file is a python file. Here are the available options and default values:

```python
# Example configuration file

# Debug mode
DEBUG = False

# host and port. 127.0.0.1 will only serve locally - set to 0.0.0.0 (or iface IP) to have available externally.
HOST = '127.0.0.1'
PORT = 8765

# Secret key. This ensures only approved applications can use this. YOU MUST CHANGE THIS VALUE TO YOUR OWN SECRET!
# Note that this is only as secure as your communication chanel. If security is an issue, ensure all traffic
# goes over HTTPS.
SECRET = '8ba6d280d4ce9a416e9b604f3f0ebb'

# Database to store request log, error log and statistics. URL as per
# http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
# This is opened multiple times, so you can not use sqlite:///:memory:
STATS_DB = 'sqlite:////var/lib/ckanpackager/stats.db'

# Number of workers. Each worker processes one job at a time.
WORKERS = 1

# Number of requests each worker should process before being restarted.
REQUESTS_PER_WORKER = 1000

# Directory where the zip files are stored
STORE_DIRECTORY = "/tmp/ckanpackager"

# Temp Directory used when creating the files
TEMP_DIRECTORY = "/tmp"

# Amount of time (in seconds) for which a specific request (matching same parameters) is cached. This will always be at
# least 1s.
CACHE_TIME = 60*60*24

# Page Size. Number of rows to fetch in a single CKAN request. Note that CKAN will timeout requests at 60s, so make sure
# to stay comfortably below that line.
PAGE_SIZE = 5000

# Shell command used to zip the file. {input} gets replaced by the input file name, and {output} by the output file
# name. You do not need to put quotes around those.
ZIP_COMMAND = "/usr/bin/zip -j {output} {input}"

# Email subject line. Available placeholders:
# {resource_id}: The resource id,
# {zip_file_name}: The file name,
# {ckan_host}: The hostname of the CKAN server the query was made to,
EMAIL_SUBJECT = "Resource from {ckan_host}"

# Email FROM line. This should be an actual email address. See Email subject for placeholders.
EMAIL_FROM = "(nobody)"

# Email body. See Email subject for placeholders.
EMAIL_BODY = """Hello,

The link to the resource you requested on {ckan_host} is available at:
http://{ckan_host}/{zip_file_name}

Best Wishes,
The Data Portal Bot
"""

# SMTP host
SMTP_HOST = 'localhost'

# SMTP port
SMTP_PORT = 25

# SMTP username (Optional, if required)
#SMTP_LOGIN = ''

# SMTP password (Optional, if required)
#SMTP_PASSWORD = ''
```

Domain Specific packaging
-------------------------
The current version includes a domain specific backend, for packaging data as a [Darwin Core Archive](http://en.wikipedia.org/wiki/Darwin_Core_Archive). This will be moved into a separate extension in the future.

This extension works like the `package_datastore` backend, but the URL is `package_dwc_archive`. It is expected that the fields returned from the datastore query are space formatted versions of Darwin Core fields (ie. 'Taxon resource ID' for 'taxonResourceID'). Fields that cannot be matched into a Darwin Core field are added as field/value pair in the dynamic properties term.

Additional configuration options:

```
# Path to the Darwin Core Archive extensions. The first one listed will be the
# core extension (as downloaded from http://rs.gbif.org/core/dwc_occurrence.xml),
# followed by additional extensions (as obtained from http://rs.gbif.org/extension/)
DWC_EXTENSION_PATHS = ['/etc/ckan/gbif_dwca_extensions/core/dwc_occurrence.xml']

# Name of the dynamic term in the darwin core. This is used to store all 
# name/value pairs that do not match into an existing Darwin Core field
DWC_DYNAMIC_TERM = 'dynamicProperties'

# The id field (from the list of fields received by the datastore) to use as
# common identifier across Darwin Core Archive extensions.
DWC_ID_FIELD = '_id'
```
