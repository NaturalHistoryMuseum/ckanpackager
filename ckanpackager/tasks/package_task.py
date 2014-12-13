import os
import smtplib
import hashlib
import multiprocessing
import traceback
from datetime import datetime
from email.mime.text import MIMEText
from ckanpackager.lib.utils import BadRequestError
from ckanpackager.lib.resource_file import ResourceFile
from ckanpackager.lib.statistics import statistics


class PackageTask(object):
    """Base class for DatastorePackageTask and UrlPackageTask

    Note that __init__ is run in the main thread, under flask, while 'run' and 'str' are typically
    run in a sub-process. __str__ and run should not access Flask API.

    Derived classes must implement:

    - schema(): Return a dictionary of all possible request parameters to tuples defining (required,
                process function). Note that classes may define additional entries for their own use.
                'email' and 'resource_id' parameters are always required, so both are added to the schema
                as (True, None) if not defined;
    - host(): Return the hostname for the current request;
    - create_zip(ResourceFile): Create the ZIP file associated with the given resource file;
    """
    def __init__(self, params, config):
        self.config = config
        self.time = str(datetime.now())
        self.request_params = {}
        schema = self.schema()
        if 'email' not in schema:
            schema['email'] = (True, None)
        if 'resource_id' not in schema:
            schema['resource_id'] = (True, None)
        for (field, definition) in schema.items():
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

    def run(self):
        """Run the task.

        Note that this is run in a separate process - we shouldn't attempt to use flask api from here.
        """
        try:
            self._run()
            statistics(self.config['STATS_DB']).log_request(
                self.request_params['resource_id'],
                self.request_params['email']
            )
        except Exception as e:
            statistics(self.config['STATS_DB']).log_error(
                self.request_params['resource_id'],
                self.request_params['email'],
                traceback.format_exc()
            )
            raise e

    def _run(self):
        """Run the task.

        Note that this is run in a separate process - we shouldn't attempt to use flask api from here.
        """
        logger = multiprocessing.get_logger()
        logger.info("Task {} parameters: {}".format(self, str(self.request_params)))
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
            logger.info("Task {} found file in cache".format(self))
        zip_file_name = resource.get_zip_file_name()
        logger.info("Task {} got ZIP file {}. Emailing link.".format(self, zip_file_name))
        # Email the link
        place_holders = {
            'resource_id': self.request_params['resource_id'],
            'zip_file_name': os.path.basename(zip_file_name),
            'ckan_host': self.host()
        }
        from_addr = self.config['EMAIL_FROM'].format(**place_holders)
        msg = MIMEText(self.config['EMAIL_BODY'].format(**place_holders))
        msg['Subject'] = self.config['EMAIL_SUBJECT'].format(**place_holders)
        msg['From'] = from_addr
        msg['To'] = self.request_params['email']
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
