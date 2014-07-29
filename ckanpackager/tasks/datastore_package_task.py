import multiprocessing
import json
import ijson
from urlparse import urlparse
from ckanpackager.lib.ckan_resource import CkanResource
from ckanpackager.tasks.package_task import PackageTask


class CkanFailure(Exception):
    """Exception raised when CKAN returns unsuccessful request"""
    pass


class DatastorePackageTask(PackageTask):
    """Represents a datastore packager task."""
    def schema(self):
        """Define the schema for datastore package tasks

        Each field is a tuple defining (required, processing function, forward to ckan)
        """
        return {
            'api_url': (True, None, False),
            'resource_id': (True, None, True),
            'email': (True, None, False),
            'filters': (False, json.loads, True),
            'q': (False, None, True),
            'plain': (False, None, True),
            'language': (False, None, True),
            'limit': (False, None, True),
            'offset': (False, None, True),
            'fields': (False, None, True),
            'sort': (False, None, True)
        }

    def host(self):
        """Return the host name for the request"""
        return urlparse(self.request_params['api_url']).netloc

    def create_zip(self, resource):
        """Create the ZIP file matching the current request

        @return: The ZIP file name
        """
        schema = self.schema()
        ckan_params = dict([(k, v) for (k, v) in self.request_params.items() if schema[k][2]])
        ckan_resource = CkanResource(self.request_params['api_url'], self.request_params.get('key', None), ckan_params)
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
