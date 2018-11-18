"""Test the DatastorePackageClass class"""

import json
import httpretty
import urlparse
import tempfile
from nose.tools import assert_raises, assert_equals, assert_true
from ckanpackager.lib.utils import BadRequestError
from ckanpackager.tasks.datastore_package_task import DatastorePackageTask


class FakeCSVWriter(object):
    """
    A Fake CSV Writer
    """

    def __init__(self, rows):
        self.rows = rows

    def writerow(self, row):
        self.rows.append(row)


class DummyResource(object):
    """
    Fake Resourcefile object
    """

    def __init__(self):
        self.rows = []
        self.create_invoked = False
        self.clean_invoked = False

    def get_csv_writer(self, file_name):
        return FakeCSVWriter(self.rows)

    def create_zip(self, command):
        self.create_invoked = True

    def clean_work_files(self):
        self.clean_invoked = True

    def zip_file_exits(self):
        return self.create_invoked


class TestDatastorePackageTask(object):

    def setUp(self):
        """
        Setup up test config&folders
        """
        self._config = {
            'ZIP_COMMAND': "/usr/bin/zip -j {output} {input}",
            'PAGE_SIZE': 3,
            'SLOW_REQUEST': 2,
            'STORE_DIRECTORY': tempfile.gettempdir(),
            'TEMP_DIRECTORY': tempfile.gettempdir(),
            'CACHE_TIME': 60
        }
        self._task = DatastorePackageTask({
            'resource_id': 'the-resource-id',
            'email': 'someone@0.0.0.0',
            'api_url': 'http://example.com/datastore/search'
        }, self._config)

    def _register_uri(self):
        """
        Register the httpretty URI with fake data
        """
        httpretty.register_uri(
            httpretty.POST,
            'http://example.com/datastore/search',
            responses=[
                # just return the fields in the first response
                httpretty.Response(json.dumps(
                    {'result': {'fields': [{'id': 'field1'}, {'id': 'field2'}, ]}})),
                httpretty.Response(json.dumps({
                    'result': {
                        'records': [{
                            'field1': 'field1-' + str(i),
                            'field2': 'field2-' + str(i)
                        } for i in range(2)]
                    }
                })),
                httpretty.Response(json.dumps({
                    'result': {
                        'records': []
                    }
                }))
            ]
        )

    def test_required_parameters(self):
        """
        Ensure api_url is required parameter
        """
        with assert_raises(BadRequestError):
            DatastorePackageTask({'resource_id': 'a', 'email': 'a'}, {})

    def test_host(self):
        """
        Ensure host is taken from api_url
        """
        assert_equals(self._task.host(), 'example.com')

    @httpretty.activate
    def test_api_url_invoked(self):
        """
        Test the the API url is invoked
        """
        self._register_uri()
        self._task.create_zip(DummyResource())
        url = urlparse.urlparse(httpretty.last_request().path)
        assert_equals(url.path, '/datastore/search')

    @httpretty.activate
    def test_zip_created(self):
        """
        Ensure that the resource ZIP file is created
        """
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_true(r.create_invoked)

    @httpretty.activate
    def test_work_folder_cleaned(self):
        """
        Ensure that the work folder is cleaned
        """
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_true(r.clean_invoked)

    def test_speed_is_fast_with_few_rows(self):
        """
        Ensure the speed is fast when few rows are present
        """
        task = DatastorePackageTask({
            'resource_id': 'the-resource-id',
            'email': 'someone@0.0.0.0',
            'api_url': 'http://example.com/datastore/search',
            'limit': 1
        }, self._config)
        assert_equals('fast', task.speed())

    def test_speed_is_slow_with_many_rows(self):
        """
        Ensure the speed is fast when many rows are present
        """
        task = DatastorePackageTask({
            'resource_id': 'the-resource-id',
            'email': 'someone@0.0.0.0',
            'api_url': 'http://example.com/datastore/search',
            'limit': 4
        }, self._config)
        assert_equals('slow', task.speed())
