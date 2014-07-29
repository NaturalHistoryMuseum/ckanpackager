import shutil
import urllib2
import multiprocessing
from urlparse import urlparse
from ckanpackager.tasks.package_task import PackageTask


class UrlPackageTask(PackageTask):
    """Represents a url packager task."""

    def schema(self):
        """Define the schema for datastore package tasks"""
        return {
            'resource_id': (True, None),
            'email': (True, None),
            'resource_url': (True, None)
        }

    def host(self):
        """Return the host name for the request"""
        return urlparse(self.request_params['resource_url']).netloc

    def create_zip(self, resource):
        """Create the ZIP file matching the current request

        @return: The ZIP file name
        """
        logger = multiprocessing.get_logger()
        try:
            with resource.get_writer(self.config['TEMP_DIRECTORY']) as output_stream:
                input_stream = urllib2.urlopen(self.request_params['resource_url'])
                logger.info("Task {} fetching and saving file.".format(self))
                shutil.copyfileobj(input_stream, output_stream)
                resource.create_zip(self.config['ZIP_COMMAND'])
        finally:
            resource.clean_work_files()