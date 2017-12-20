import shutil
import urllib2
from urlparse import urlparse
from ckanpackager.tasks.package_task import PackageTask


class UrlPackageTask(PackageTask):
    """Represents a url packager task."""

    def schema(self):
        """Define the schema for datastore package tasks"""
        return {
            'resource_id': (True, None),
            'email': (True, None),
            'resource_url': (True, None),
            'key': (False, None)
        }

    def host(self):
        """Return the host name for the request"""
        return urlparse(self.request_params['resource_url']).netloc

    def speed(self):
        """ Return the expected task duration """
        return 'fast'

    def create_zip(self, resource):
        """Create the ZIP file matching the current request

        @return: The ZIP file name
        """
        headers = {}
        if 'key' in self.request_params:
            headers['Authorization'] = self.request_params['key']
        try:
            output_stream = resource.get_writer()
            request = urllib2.Request(self.request_params['resource_url'],
                                      headers=headers)
            input_stream = urllib2.urlopen(request)
            self.log.info("Fetching and saving file.")
            shutil.copyfileobj(input_stream, output_stream)
            resource.create_zip(self.config['ZIP_COMMAND'])
        finally:
            resource.clean_work_files()
