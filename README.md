ckanpackager
============

Overview
--------

A stand-alone service that can be instructed to fetch data on a [CKAN](http://ckan.org) site using the datastore API, pack the data in a ZIP file and email the link to a given address.

This is useful for allowing people to download very large datasets (tested on a dataset of 3,000,000 rows), and there is a corresponding CKAN extension, [ckanext-ckanpackager](http://github.com/NaturalHistoryMuseum/ckanext-ckanpackager), to provide an alternative download button on resources.

Features:
- Can apply filters and full text queries to the dataset;
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

### status

This expects a POST request, and returns a JSON dictionary.

Parameters:
- `secret`: The shared secret

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

### package

This expects a POST request, and returns a JSON dictionary. If the request is successful, the task is queued up. When the tasks gets to run, it will fetch the given resource (with filters applied), pack it into a ZIP file and email the link to the given email address.

Parameters:
- `secret`: The shared secret (required);
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
  
  request = urllib2.Request('http://ckanpackager.example.com/status')
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
SECRET = '8ba6d280d4ce9a416e9b604f3f0ebb'

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

# SMTP host:port
SMTP_HOST = 'localhost'

# SMTP username (Optional, if required)
#SMTP_LOGIN = ''

# SMTP password (Optional, if required)
#SMTP_PASSWORD = ''
```