ckanpackager
============

Overview
--------

A stand-alone service that can be instructed to fetch data on a [CKAN](http://ckan.org) site using the datastore API, pack the data in a ZIP file and email the link to a given address.

**This is work in progress and is not currently functional**

This is useful for allowing people to download very large datasets (tested on a dataset of 3,000,000 rows), and there is a corresponding CKAN extension, [ckanext-ckanpackager](http://github.com/NaturalHistoryMuseum) ( **currently not implemented** ), which replaces the download button on resources to use the ckanpackager.

Features:
- Can apply filters and full text queries to the dataset;
- Configurable number of workers and a queuing system ensures users can control the resources used;
- Data is processed as it is streamed from CKAN, so the memory usage is kept low;

Note: ckanpackager streams the info and drops the connection as soon as it has the data it wants. This means your CKAN server might show broken pipe errors. That's fine.


Usage
-----

The application is under development and does yet contain a WSGI wrapper. Run manually by doing:

`CKANPACKAGER_CONFIG=[path to config file] python ckanpackager/application.py`

Once started you can make HTTP request on the host/port with the following URLs:

- _/status_ to obtain the status as a json object. Required parameters: `secret`;
- _/package_ to get a package created and a link emailed. Required POST parameters: `secret`, `resource_id` and `email`. Optional POST parameters: `filters`, `q`, `limit`, `offset`.


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