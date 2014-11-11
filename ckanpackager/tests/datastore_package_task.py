"""Test the DatastorePackageClass class"""

import json
import httpretty
import urllib
from nose.tools import assert_raises, assert_equals, assert_true
from ckanpackager.lib.utils import BadRequestError
from ckanpackager.tasks.datastore_package_task import DatastorePackageTask


class FakeCSVWriter(object):
    """ A Fake CSV Writer"""
    def __init__(self, rows):
        self.rows = rows

    def writerow(self, row):
        self.rows.append(row)

class DummyResource(object):
    """Fake Resourcefile object """
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


class TestDatastorePackageTask(object):
    def setUp(self):
        """Setup up test config&folders"""
        self._config = {
            'ZIP_COMMAND': "/usr/bin/zip -j {output} {input}",
            'PAGE_SIZE': 3
        }
        self._task = DatastorePackageTask({
            'resource_id': 'the-resource-id',
            'email': 'someone@0.0.0.0',
            'api_url': 'http://example.com/datastore/search'
        }, self._config)

    def _ckan_response(self, request, uri, headers):
        parameters = json.loads(urllib.unquote(request.body))
        data = {
            'result': {
                'fields': [
                    {'id': 'field1'},
                    {'id': 'field2'},
                ],
                'records': []
            }
        }
        start = parameters['offset']
        limit = parameters['limit']
        if start >= self._config['PAGE_SIZE']*2:
            limit = limit - 1
        for i in range(start, start + limit):
            data['result']['records'].append({
                'field1': 'field1-' + str(i),
                'field2': 'field2-' + str(i)
            })
        return 200, headers, json.dumps(data)

    def _register_uri(self):
        """Register the httpretty URI with fake data"""
        s = self._config['PAGE_SIZE']
        httpretty.register_uri(
            httpretty.POST,
            'http://example.com/datastore/search',
            body=self._ckan_response
        )

    def test_required_parameters(self):
        """Ensure api_url is required parameter"""
        with assert_raises(BadRequestError) as context:
            p = DatastorePackageTask({'resource_id': 'a', 'email': 'a'}, {})

    def test_host(self):
        """Ensure host is taken from api_url"""
        assert_equals(self._task.host(), 'example.com')

    @httpretty.activate
    def test_api_url_invoked(self):
        """Test the the API url is invoked"""
        self._register_uri()
        self._task.create_zip(DummyResource())
        assert_equals(httpretty.last_request().path, '/datastore/search')

    @httpretty.activate
    def test_multi_page_data_fetched(self):
        """Test that the data is correctly fetched (and saved) across multiple
           pages.
        """
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(r.rows, [
            [u'field1', u'field2'], [u'field1-0', u'field2-0'],
            [u'field1-1', u'field2-1'], [u'field1-2', u'field2-2'],
            [u'field1-3', u'field2-3'], [u'field1-4', u'field2-4'],
            [u'field1-5', u'field2-5'], [u'field1-6', u'field2-6'],
            [u'field1-7', u'field2-7']
        ])

    @httpretty.activate
    def test_zip_created(self):
        """Ensure that the resource ZIP file is created"""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_true(r.create_invoked)

    @httpretty.activate
    def test_work_folder_cleaned(self):
        """Ensure that the work folder is cleaned"""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_true(r.clean_invoked)
