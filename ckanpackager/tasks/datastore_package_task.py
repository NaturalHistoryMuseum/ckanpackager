import multiprocessing
import json
import ijson
from urlparse import urlparse
from ckanpackager.lib.ckan_resource import CkanResource
from ckanpackager.lib.resource_file import ResourceFile
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
            # Read the datastore fields, and generate the package structure.
            logger.info("Task {} fetching field list".format(self))
            with ckan_resource.get_stream(0, 0) as input_stream:
                fields = self._stream_headers(input_stream, resource)
                #FIXME: Test for failure
            # Get the records, page by page
            start = 0
            saved = -1
            while saved == self.config['PAGE_SIZE'] or saved < 0:
                logger.info("Task {} processing page ({},{})".format(self, start, self.config['PAGE_SIZE']))
                with ckan_resource.get_stream(start, self.config['PAGE_SIZE']) as input_stream:
                    saved = self._stream_records(input_stream, fields, resource)
                start += self.config['PAGE_SIZE']
            # Finalize the resource
            self._finalize_resource(fields, resource)
            # Zip the file
            resource.create_zip(self.config['ZIP_COMMAND'])
        finally:
            resource.clean_work_files()

    def _stream_headers(self, input_stream, resource):
        """Stream the list of fields and save the headers in the resource.

        @param input_stream: file-like object representing the input stream
        @param resource: A resource file
        @type resource: ResourceFile
        @returns: List (or other structure) representing the fields saved to
                  the headers, to allow _stream_records to save rows in a
                  matching format.
        """
        fields = []
        for field_id in ijson.items(input_stream, 'result.fields.item.id'):
            fields.append(field_id)
        w = resource.get_csv_writer('resource.csv')
        w.writerow(fields)
        return fields

    def _stream_records(self, input_stream, fields, resource):
        """Stream the records from the input stream to the resource files

        @param input_stream: file-like object representing the input stream
        @param fields: List (or other structure) representing the fields
                       as returned by _stream_headers
        @param resource: Resource file
        @type resource: ResourceFile
        @returns: Number of rows saved
        """
        saved = 0
        w = resource.get_csv_writer('resource.csv')
        for json_row in ijson.items(input_stream, 'result.records.item'):
            row = []
            for field_id in fields:
                row.append(json_row.get(field_id, None))
            w.writerow(row)
            saved += 1
        return saved

    def _finalize_resource(self, fields, resource):
        """Finalize the resource before ZIPing it.

        This implementation does nothing - this is available as a hook for
        sub-classes who wish to add other files in the .zip file

        @param fields: List (or other structure) representing the fields
                       as returned by _stream_headers
        @param resource: The resource we are creating
        @type resource: ResourceFile
        """
        pass