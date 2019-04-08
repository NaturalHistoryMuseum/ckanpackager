# Example configuration file

# Debug mode
DEBUG = False

# host and port. 127.0.0.1 will only serve locally - set to 0.0.0.0 (or iface IP) to have available externally.
HOST = '0.0.0.0'
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
STORE_DIRECTORY = "/var/www/ckanpackager/resources"

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
SLOW_REQUEST = 10000

# Shell command used to zip the file. {input} gets replaced by the input file name, and {output} by the output file
# name. You do not need to put quotes around those.
ZIP_COMMAND = "/usr/bin/zip -j {output} {input}"

# Message sent back when returning success
SUCCESS_MESSAGE = "The resource will be emailed to you shortly. This make take a little longer if our servers are busy, so please be patient!"

# Email subject line. Available placeholders:
# {resource_id}: The resource id,
# {zip_file_name}: The file name,
# {ckan_host}: The hostname of the CKAN server the query was made to,
# {doi}: The DOI for this download, if there is one (if the DOI is missing, this will be blank)
# {doi_body}: The result of formatting the DOI_BODY below with these placeholders (if the DOI is missing, will be blank)
EMAIL_SUBJECT = "Resource from {ckan_host}"

# Email FROM line. See Email subject for placeholders.
EMAIL_FROM = "(nobody)"

# Email body. See Email subject for placeholders.
EMAIL_BODY = """Hello,

The link to the resource you requested on {ckan_host} is available at:
http://www.example.com/resources/{zip_file_name}

{doi_body}

Best Wishes,
The Data Portal Bot
"""

# DOI body. See Email subject for placeholders.
DOI_BODY = """A DOI has been created for this data: https://doi.org/{doi} (this may take a few hours to become active).
Please ensure you reference this DOI when citing this data.
For more information, follow the DOI link.
"""

# SMTP host
SMTP_HOST = 'smtp.example.com'

# SMTP port
SMTP_PORT = 25

# SMTP username (Optional, if required)
#SMTP_LOGIN = ''

# SMTP password (Optional, if required)
#SMTP_PASSWORD = ''

#
# Following configuration options are for the generation of DarwinCore Archives
#

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
        },
        # this defines a mapping from DwC extension term name to the field name used
        # in the datastore search result (i.e. associatedMedia.mime -> format
        'mappings': {
            # in the associatedMedia field that comes back from solr, the
            # format is stored under the key mime
            'format': 'mime'
        },
        # this defines formatting functions that should be used on any given terms
        'formatters': {
            # the format needs to start with image/
            'format': lambda v: 'image/{}'.format(v) if v else v
        }
    }
}
