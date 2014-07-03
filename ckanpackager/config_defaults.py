DEBUG = False
HOST = '127.0.0.1'
PORT = 8765
SECRET = '8ba6d280d4ce9a416e9b604f3f0ebb'
WORKERS = 1
REQUESTS_PER_WORKER = 1000
PAGE_SIZE = 5000
STORE_DIRECTORY = "/tmp/ckanpackager"
TEMP_DIRECTORY = "/tmp"
CACHE_TIME = 60*60*24
ZIP_COMMAND = "/usr/bin/zip {output} {input}"
SMTP_HOST = "localhost"
EMAIL_SUBJECT = "Resource from {ckan_host}"
EMAIL_FROM = "(nobody)"
EMAIL_BODY = """Hello,

The link to the resource you requested on {ckan_host} is available at:
http://{ckan_host}/{zip_file_name}

Best Wishes,
The Data Portal Bot
"""