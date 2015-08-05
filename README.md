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
- Uses [celery](http://www.celeryproject.org/) for the task queue;
- Data is processed as it is streamed from CKAN, so the memory usage is kept low;

Note: ckanpackager streams the info and drops the connection as soon as it has the data it wants. This means your CKAN server might show broken pipe errors. That's fine.


Deployment
----------

**Ckanpackager service, standalone**

You can run the ckanpackager service on it's own by running:

`CKANPACKAGER_CONFIG=[path to config file] ckanpackager-service`

This will output all the logs directly to the terminal, so it is useful for debugging.

**Ckanpackager service under Apache**

Using an Apache2 server with mod_wsgi enabled, you can use the following files:

- `deployment/ckanpackager`: An example Apache2 virtual host file. Typically goes under `/etc/apache2/sites-available`;
- `deployment/ckanpackager.wsgi`: A WSGI wrapper for ckanpackager. If using the default virtual host example this would be placed in `/etc/ckanpackager/ckanpackager.wsgi`;
- `deployment/ckanpackager_settings.py`: An example configuration file (see below for options). If using the default wsgi wrapper, this would be placed in `/etc/ckanpackager/ckanpackager_settings.py`

Note that the default setup runs a single instance of ckanpackager. 

**Task worker (Celery)**

The tasks are performed by a separate process which needs to be started separately. This can be done by doing:

`CKANPACKAGER_CONFIG=[path to config file] celery -A ckanpackager.task_setup.app --queues=fast,slow worker`

Note that there are two queues - one for slow tasks (as configured by number of records) and one for fast tasks. You can process both using a single celery worker, or use two separate workers allowing fast tasks to not wait for the slower ones. Some usefull celery options:

- `--events`: Ensures events are sent by the worker, allowing monitoring tools such as flower to report on activity;
- `--concurrency=N`: Number of worker processes;
- `--maxtasksperchild=N`: Number of tasks to process before restarting worker processes
- `--hostname=NAME`: Name of the worker;
- `--loglevel=INFO`: Log level
- `--detach`: Daemonize.

So starting two workers, one for each queue, could be done as:

```
 CKANPACKAGER_CONFIG=/etc/ckanpackager/ckanpackager_settings.py celery -A ckanpackager.task_setup.app --detach --events --concurrency=1 --maxtasksperchild=1000 --queues=slow --hostname=slow.%h worker --loglevel=INFO
 CKANPACKAGER_CONFIG=/etc/ckanpackager/ckanpackager_settings.py celery -A ckanpackager.task_setup.app --detach --events --concurrency=1 --maxtasksperchild=1000 --queues=fast --hostname=fast.%h worker --loglevel=INFO
```
 
**Docker**

We provide a base docker image for ckanpackager. The docker image contains:
- One instance of the queuing service;
- One Redis server for storing tasks;
- Two celery workers (one for the slow queue and one for the fast queue);
- [Flower](http://flower.readthedocs.org/en/latest/), a Celery monitoring tool which can be accessed on port 5555.

You can get the docker image by doing:

```
docker pull aliceh75/ckanpackager
```

You will need to create your own image that adds the configuration and CLI defaults. Here is an example Dockerfile you can use to do this:

```
FROM aliceh75/ckanpackager:0.2.1
COPY ckanpackager_settings.py /etc/ckanpackager/ckanpackager_settings.py
COPY ckanpackager-cli.json /etc/ckanpackager/ckanpackager-cli.json
```

**Command line**

ckanpackager provides a command line interface to it's rest API for easy local administration. You can run it simply by doing:

```
ckanpackager
```

Usage
-----

The service provides multiple HTTP access points:

### / and /status

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

### /package_datastore

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

### /package_url
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

### /statistics
This expects a POST request, and returns a JSON dictionary.

Parameters:
- `secret`: The shared secret (required). This is only secure over HTTPS;
- `resource_id`: Optionally only get statistics for the given resource id;

JSON result fields:
- `success`: True or False;
- `totals`: A dictionary with one entry per resource (keyed by resource id)
            with a special resource '*' containing the grand totals. If
            `resource_id` was specified, only that resource is included.
            Each entry contains thee fields:
            - `emails`: Number of unique emails that requested this resource;
            - `requests`: Number of requests made for this resource;
            - `errors`: Number of errors that happened while generating this
                        resource.

### /statistics/requests
This expects a POST request, and returns a JSON dictionary.

Parameters:
- `secret`: The shared secret (required). This is only secure over HTTPS;
- `offset`: Offset to start fetching (defaults to 0);
- `limit`: Number of entries to fetch (defaults to 100);
- `resource_id`: Optionally only get requests for the given resource  id;
- `email`: Optionally only get requests for the given email address.

JSON result fields:
- `success`: True or False;
- `requests`: A list of individual requests (ordered by timestamp descending),
              optionally filtered by resource_id and email. Each entry in
              the list is a dictionary defining:
              - `email`: The email that made the request;
              - `resource_id`: The resource id of the request;
              - `timestamp`: A UNIX timestamp of when the request was made.

### /statistics/errors
This expects a POST request, and returns a JSON dictionary.

Parameters:
- `secret`: The shared secret (required). This is only secure over HTTPS;
- `offset`: Offset to start fetching (defaults to 0);
- `limit`: Number of entries to fetch (defaults to 100);
- `resource_id`: Optionally only get errors for the given resource id;
- `email`: Optionally only get errors for the given email address.

JSON result fields:
- `success`: True or False;
- `errors`: A list of individual errors (ordered by timestamp descending),
              optionally filtered by resource_id and email. Each entry in
              the list is a dictionary defining:
              - `email`: The email that made the request;
              - `resource_id`: The resource id of the request;
              - `timestamp`: A UNIX timestamp of when the request was made.

### ckanpackager command line tool

Ckanpackager also comes with a command line tool for sending requests to a ckanpackager instance:

```
$ ckanpackager -h
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
                    for /etc/ckanpackager/ckanpackager-cli.json and use that if present.
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

# Celery (message queue) broker. See http://celery.readthedocs.org/en/latest/getting-started/first-steps-with-celery.html#choosing-a-broker
# Defaults to 'redis://localhost:6379/0' . To test this (but not for production)
# you can use sqlite with 'sqla+sqlite:////tmp/celery.db'
CELERY_BROKER = 'redis://localhost:6379/0'

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

# Slow request. Number of rows from which a request will be assumed to be slow,
# and put on the slow queue.
SLOW_REQUEST = 50000

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
# Define the field from the ckan result that will be used as an internal
# identifier for each row within the archive.
DWC_ID_FIELD = '_id'

# Define the core extension, which specifies what type of information the archive
# contains. All the fields returned from ckan will be matched into this extension
# if possible (name, or camel cased version of the name, must match).
DWC_CORE_EXTENSION = '/etc/ckanpackager/gbif_dwca_extensions/core/dwc_occurrence.xml'

# Define additional extensions. All the fields returned from ckan which cannot be
# matched into the core extension will attempt to match into one of the
# additional extensions.
DWC_ADDITIONAL_EXTENSIONS = []

# Define a dynamic field on the core extension used to store (as a JSON object)
# all the ckan fields that could not be matched into an extension
DWC_DYNAMIC_TERM = 'dynamicProperties'

# Define a list of ckan fields which will contain a JSON list of objects, such
# that each object is a row in a separate extension. This can be used to create
# many to one relationships to the core extension. For each such field, we must
# define the extension AND the list of expected fields with default values
# should they be missing. (we could not, otherwise, get this without first
# parsing the whole result set). Additional fields will be ignored silently.
DWC_EXTENSION_FIELDS = {
    'associatedMedia': {
        'extension': '/etc/ckanpackager/gbif_dwca_extensions/extensions/multimedia.xml',
        'fields': {
            'type': 'StillImage',
            'format': 'image/jpeg',
            'identifier': '',
            'title': '',
            'license': 'http://creativecommons.org/licenses/by/4.0/',
            'rightsHolder': ''
        }
    }
}
```

TODO
----

Set up GBIF to run on cron.  Check if data created is after last import.  If it is, then recreate
