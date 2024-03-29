import os
import smtplib
import hashlib
import logging
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from ckanpackager.lib.utils import BadRequestError
from ckanpackager.lib.resource_file import ResourceFile
from ckanpackager.lib.statistics import statistics
from raven import Client

class PackageTask(object):
    """Base class for DatastorePackageTask and UrlPackageTask

    Note that all methods may be called from the web service or the task
    consumer.

    Derived classes must implement:

    - schema(): Return a dictionary of all possible request parameters to tuples defining (required,
                process function). Note that classes may define additional entries for their own use.
                'email' and 'resource_id' parameters are always required, so both are added to the schema
                as (True, None) if not defined;
    - host(): Return the hostname for the current request;
    - create_zip(ResourceFile): Create the ZIP file associated with the given resource file;

    In addition, derived class should implement:
    - speed(): Return 'slow' or 'fast' depending on the expected duration of the
               task. If not implemented, this always returns 'slow'.
    """
    def __init__(self, params, config):
        self.config = config
        self.sentry = Client(self.config.get('SENTRY_DSN'))
        self.time = str(datetime.now())
        self.request_params = {}
        self.log = logging.getLogger(__name__)
        schema = self.schema()
        if 'email' not in schema:
            schema['email'] = (True, None)
        if 'resource_id' not in schema:
            schema['resource_id'] = (True, None)
        for field, definition in schema.items():
            if definition[0] and field not in params:
                raise BadRequestError("Parameter {} is required".format(field))
            if field in params:
                if definition[1] is not None:
                    self.request_params[field] = definition[1](params.get(field, None))
                else:
                    self.request_params[field] = params.get(field, None)

    def schema(self):
        raise NotImplementedError

    def create_zip(self, resource):
        raise NotImplementedError

    def host(self):
        raise NotImplementedError
  
    def speed(self):
        """ Return the task estimated time as either 'fast' or 'slow'.

        If the file exists in the cache, then this returns 'fast'. It returns
        'slow' otherwise.
        """
        resource = ResourceFile(
            self.request_params,
            self.config['STORE_DIRECTORY'],
            self.config['TEMP_DIRECTORY'],
            self.config['CACHE_TIME']
        )
        if resource.zip_file_exists():
            return 'fast'
        else:
            return 'slow'

    def run(self, logger=None):
        """Run the task."""
        # create a stats object for database access
        stats = statistics(self.config['STATS_DB'], self.config.get(u'ANONYMIZE_EMAILS'))
        try:
            if logger is not None:
                self.log = logger
            else:
                self.log = logging.getLogger(__name__)
            self._run()
            stats.log_request(
                self.request_params['resource_id'],
                self.request_params['email'],
                self.request_params.get('limit', None)
            )
        except Exception as e:
            stats.log_error(
                self.request_params['resource_id'],
                self.request_params['email'],
                traceback.format_exc()
            )
            self.sentry.captureException()
            raise e

    def _run(self):
        """Run the task"""
        self.log.info("Task parameters: {}".format(str(self.request_params)))
        # Get/create the file
        resource = ResourceFile(
            self.request_params,
            self.config['STORE_DIRECTORY'],
            self.config['TEMP_DIRECTORY'],
            self.config['CACHE_TIME']
        )
        if not resource.zip_file_exists():
            self.create_zip(resource)
        else:
            self.log.info("Found file in cache")
        zip_file_name = resource.get_zip_file_name()
        self.log.info("Got ZIP file {}. Emailing link.".format(zip_file_name))
        # Email the link
        place_holders = {
            'resource_id': self.request_params['resource_id'],
            'zip_file_name': os.path.basename(zip_file_name),
            'ckan_host': self.host(),
            # retrieve a doi from the request params, if there is one, otherwise default to the empty string
            'doi': self.request_params.get('doi', ''),
            # default the doi_body to the empty string, we'll fill it in below if necessary
            'doi_body': '',
            'doi_body_html': '',
        }
        if place_holders['doi']:
            if 'DOI_BODY' in self.config:
                place_holders['doi_body'] = self.config['DOI_BODY'].format(**place_holders)
            if 'DOI_BODY_HTML' in self.config:
                place_holders['doi_body_html'] = \
                    self.config['DOI_BODY_HTML'].format(**place_holders)

        from_addr = self.config['EMAIL_FROM'].format(**place_holders)

        msg = MIMEMultipart('alternative')
        # add the basics
        msg['Subject'] = self.config['EMAIL_SUBJECT'].format(**place_holders)
        msg['From'] = from_addr
        msg['To'] = self.request_params['email']
        # add the body as html and text
        text = MIMEText(self.config['EMAIL_BODY'].format(**place_holders), 'plain')
        html = MIMEText(self.config['EMAIL_BODY_HTML'].format(**place_holders), 'html')
        msg.attach(text)
        msg.attach(html)
        # send the email
        server = smtplib.SMTP(self.config['SMTP_HOST'], self.config['SMTP_PORT'])
        try:
            if 'SMTP_LOGIN' in self.config:
                server.login(self.config['SMTP_LOGIN'], self.config['SMTP_PASSWORD'])
            server.sendmail(from_addr, self.request_params['email'], msg.as_string())
        finally:
            server.quit()

    def __str__(self):
        """Return a unique representation of this task"""
        md5 = hashlib.md5()
        md5.update(str(self.request_params))
        md5.update(self.time)
        return md5.hexdigest()
