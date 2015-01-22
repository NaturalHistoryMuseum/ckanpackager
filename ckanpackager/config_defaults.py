DEBUG = False
HOST = '127.0.0.1'
PORT = 8765
SECRET = '8ba6d280d4ce9a416e9b604f3f0ebb'
STATS_DB = 'sqlite:////var/lib/ckanpackager/stats.db'
WORKERS = 1
REQUESTS_PER_WORKER = 1000
PAGE_SIZE = 5000
STORE_DIRECTORY = "/tmp/ckanpackager"
TEMP_DIRECTORY = "/tmp"
CACHE_TIME = 60*60*24
ZIP_COMMAND = "/usr/bin/zip {output} {input}"
SMTP_HOST = "localhost"
SMTP_PORT = 25
SUCCESS_MESSAGE = "The resource will be emailed to you shortly. This make take a little longer if our servers are busy, so please be patient!"
EMAIL_SUBJECT = "Resource from {ckan_host}"
EMAIL_FROM = "(nobody)"
EMAIL_BODY = """Hello,

The link to the resource you requested on {ckan_host} is available at:
http://{ckan_host}/{zip_file_name}

Best Wishes,
The Data Portal Bot
"""
DWC_ID_FIELD = '_id'
DWC_CORE_EXTENSION = '/etc/ckanpackager/gbif_dwca_extensions/core/dwc_occurrence.xml'
DWC_ADDITIONAL_EXTENSIONS = []
DWC_DYNAMIC_TERM = 'dynamicProperties'
DWC_EXTENSION_FIELDS = {
    'associatedMedia': {
        'extension': '/etc/ckanpackager/gbif_dwca_extensions/extensions/multimedia.xml',
        'fields': {
            'type': '',
            'format': '',
            'identifier': '',
            'title': '',
            'license': '',
            'rightsHolder': ''
        }
    }
}
