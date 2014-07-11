import os
import hashlib
import multiprocessing
import json
import ijson
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from urlparse import urlparse
from ckanpackager.lib.utils import BadRequestError
from ckanpackager.lib.resource_file import ResourceFile
from ckanpackager.lib.ckan_resource import CkanResource


class StreamError(Exception):
    """Exception raised when connecting to CKAN"""
    pass


class CkanFailure(Exception):
    """Exception raised when CKAN returns unsuccessfull request"""
    pass


class ArchiveError(Exception):
    """Exception raised when failing to build the archive"""
    pass


class PackageTask():
    """Represents a packager task.

    Note that __init__ is run in the main thread, under flask, while 'run' and 'str' are typically
    run in a sub-process. __str__ and run should not access Flask API.
    """
    def __init__(self, params, config):
        """Create a packager task from request parameters.

        Required parameters are:
        - api_url: CKAN datastore_search action URL;
        - resource_id: Resource to query;
        - email: The email to send the link to.

        Optional parameters are:
        - key: CKAN API key to make the request as;
        - filters: Filters to apply on resource, as JSON encoded dictionary of field name to value;
        - q: Full text search to perform on resource;
        - plain: Treat as plain text query;
        - language: Language of the full text query;
        - limit: Limit to apply on query. Note: while the ckan datastore would default this value to 100, the default
                 in CKAN Packager is to return all rows;
        - offset: Offset to apply on query;
        - fields: fields to return;
        - sort: sort for the query;

        Apart from limit and offset, all parameters are transmitted as is to CKAN. See
        http://docs.ckan.org/en/latest/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_search
        for default values, etc.

        Required configuration items are:
        - PAGE_SIZE, TEMP_DIRECTORY, STORE_DIRECTORY, CACHE_TIME, ZIP_COMMAND, EMAIL_SUBJECT, EMAIL_FROM, EMAIL_BODY,
          SMTP_HOST

        Optional configuration items are:
        - SMTP_LOGIN, SMTP_PASSWORD
        """
        for req in ['api_url', 'resource_id', 'email']:
            if req not in params:
                raise BadRequestError("Parameter {} is required".format(req))
        self.api_url = params.get('api_url')
        self.key = params.get('key', None)
        self.email = params.get('email')
        self.request_params = {}
        for item in ['resource_id', 'filters', 'q', 'plain', 'language', 'limit', 'offset', 'fields', 'sort']:
            self.request_params[item] = params.get(item, None)
        if self.request_params['filters']:
            print self.request_params['filters']
            try:
                self.request_params['filters'] = json.loads(self.request_params['filters'])
            except (ValueError, TypeError):
                raise BadRequestError("filters should be a JSON encoded dictionary")
        self.time = str(datetime.now())
        self.config = config

    def run(self):
        """Run the task.

        Note that this is run in a separate process - we shouldn't attempt to use flask api from here.
        """
        logger = multiprocessing.get_logger()
        # Get/create the file
        resource = ResourceFile(self.request_params, self.config['STORE_DIRECTORY'], self.config['CACHE_TIME'])
        if not resource.zip_file_exists():
            self._create_zip(resource)
        else:
            logger.info("Task {} found file in cache".format(self))
        zip_file_name = resource.get_zip_file_name()
        logger.info("Task {} got ZIP file {}. Emailing link.".format(self, zip_file_name))
        # Email the link
        parsed_url = urlparse(self.api_url)
        place_holders = {
            'resource_id': self.request_params['resource_id'],
            'zip_file_name': os.path.basename(zip_file_name),
            'ckan_host': parsed_url.netloc
        }
        from_addr = self.config['EMAIL_FROM'].format(**place_holders)
        msg = MIMEText(self.config['EMAIL_BODY'].format(**place_holders))
        msg['Subject'] = self.config['EMAIL_SUBJECT'].format(**place_holders)
        msg['From'] = from_addr
        msg['To'] = self.email
        server = smtplib.SMTP(self.config['SMTP_HOST'])
        try:
            if 'SMTP_LOGIN' in self.config:
                server.login(self.config['SMTP_LOGIN'], self.config['SMTP_PASSWORD'])
            server.sendmail(from_addr, self.email, msg.as_string())
        finally:
            server.quit()

    def _create_zip(self, resource):
        """Create the ZIP file matching the current request

        @return: The ZIP file name
        """
        ckan_resource = CkanResource(self.api_url, self.key, self.request_params)
        try:
            logger = multiprocessing.get_logger()
            # Generate the CSV file
            with resource.get_csv_writer(self.config['TEMP_DIRECTORY']) as output_stream:
                # Get the headers first. This is needed as we must have them before the data to ensure we can
                # stream the output.
                logger.info("Task {} fetching field list".format(self))
                with ckan_resource.get_stream(0, 0) as input_stream:
                    fields = self._save_fields(input_stream, output_stream)
                if not fields:
                    raise CkanFailure("CKan query failed")
                # Now get the records, page by page
                start = 0
                saved = -1
                while saved == self.config['PAGE_SIZE'] or saved < 0:
                    logger.info("Task {} processing page ({},{})".format(self, start, self.config['PAGE_SIZE']))
                    with ckan_resource.get_stream(start, self.config['PAGE_SIZE']) as input_stream:
                        saved = self._save_records(input_stream, output_stream, fields)
                    start += self.config['PAGE_SIZE']
            # Zip the file
            resource.create_zip(self.config['ZIP_COMMAND'])
        finally:
            resource.clean_work_files()

    def _save_fields(self, input_stream, output_stream):
        """Save the list of fields to the output stream.

        @param input_stream: file-like object representing the input stream
        @param output_stream: CSV Writer object we send the output to
        @return: The list of fields defined
        """
        fields = []
        for field_id in ijson.items(input_stream, 'result.fields.item.id'):
            fields.append(field_id)
        output_stream.writerow(fields)
        return fields

    def _save_records(self, input_stream, output_stream, fields):
        """Save the records to the output stream

        @param input_stream: file-like object representing the input stream
        @param output_stream: CSV writer object we send the output to
        @return: Number of rows saved
        """
        saved = 0
        for json_row in ijson.items(input_stream, 'result.records.item'):
            row = []
            for field_id in fields:
                row.append(json_row.get(field_id, None))

            output_stream.writerow(row)
            saved += 1
        return saved

    def __str__(self):
        """Return a unique representation of this task"""
        md5 = hashlib.md5()
        md5.update(str(self.request_params))
        md5.update(self.time)
        return md5.hexdigest()
