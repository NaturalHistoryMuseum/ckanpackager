"""Test the CkanResource class"""
import copy
import json

import httpretty
from nose.tools import assert_equals, assert_raises, assert_false

from ckanpackager.lib.ckan_resource import CkanResource, StreamError

# for convenience, here's an empty CKAN body response
EMPTY_BODY = json.dumps({'result': {'records': []}})


class TestCkanResource(object):

    @httpretty.activate
    def test_request_url(self):
        """
        Ensure we get a stream queried with the given URL
        """
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test', body=EMPTY_BODY)
        r = CkanResource('http://somewhere.com/test', None, 42, {})
        list(r.get_records())
        assert_equals(httpretty.last_request().path, '/test')
        assert_equals(json.loads(httpretty.last_request().body), {'offset': 0, 'limit': 42})

    @httpretty.activate
    def test_request_parameters(self):
        """
        Ensure request parameters are passed to the request
        """
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test', body=EMPTY_BODY)
        r = CkanResource('http://somewhere.com/test', None, 34, {'carrot': 'cake'})
        list(r.get_records())
        assert_equals(json.loads(httpretty.last_request().body), {'offset': 0, 'limit': 34,
                                                                  'carrot': 'cake'})

    @httpretty.activate
    def test_request_no_limit(self):
        """
        Ensure the page size is used when there is no limit specified
        """
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test', body=EMPTY_BODY)
        page_size = 57
        r = CkanResource('http://somewhere.com/test', None, page_size,
                         {'offset': None, 'limit': None})
        list(r.get_records())
        assert_equals(json.loads(httpretty.last_request().body), {'offset': 0, 'limit': page_size})

    @httpretty.activate
    def test_request_limit_size_lower(self):
        """
        If a limit is present it should be used as limit for the number of records to download and
        the overall page size limit should be used to determine how many records should be
        downloaded. However, the lower of the page size and the limit should be used as the CKAN
        request limit to avoid getting far more records than needed. This tests the scenario when
        the page size is lower than the requested limit.
        """
        page_size = 2
        responses = [
            httpretty.Response(json.dumps({'result': {'records': list(range(page_size))}})),
            httpretty.Response(json.dumps({'result': {'records': list(range(page_size))}})),
            httpretty.Response(json.dumps({'result': {'records': list(range(page_size))}})),
        ]
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test', responses=responses)

        r = CkanResource('http://somewhere.com/test', None, page_size,
                         {'offset': 4, 'limit': 10})
        records = list(r.get_records())
        assert_equals(len(records), 10)
        # the last request's offset should be 12 because we're requesting 10 records starting at 4
        # and each request size is 2, therefore the requests should be from offsets 4, 6, 8, 10 and
        # 12 at which point the target limit is reached and our work is done
        assert_equals(json.loads(httpretty.last_request().body), {'offset': 12, 'limit': 2})

    @httpretty.activate
    def test_request_limit_limit_lower(self):
        """
        If a limit is present it should be used as limit for the number of records to download and
        the overall page size limit should be used to determine how many records should be
        downloaded. However, the lower of the page size and the limit should be used as the CKAN
        request limit to avoid getting far more records than needed. This tests the scenario when
        the requested limit is lower than the page size.
        """
        page_size = 200
        responses = [
            httpretty.Response(json.dumps({'result': {'records': list(range(10))}})),
        ]
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test', responses=responses)

        r = CkanResource('http://somewhere.com/test', None, page_size,
                         {'offset': 4, 'limit': 10})
        records = list(r.get_records())
        assert_equals(len(records), 10)
        assert_equals(json.loads(httpretty.last_request().body), {'offset': 4, 'limit': 10})

    @httpretty.activate
    def test_request_authorization(self):
        """
        Ensure an authorization header is added
        """
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test', body=EMPTY_BODY)
        r = CkanResource('http://somewhere.com/test', 'somekey', 1, {'offset': None, 'limit': None})
        list(r.get_records())
        assert_equals(httpretty.last_request().headers['authorization'], 'somekey')

    @httpretty.activate
    def test_request_failure(self):
        """
        Ensure an exception is raised when the query returns a non-200 status code
        """
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test', status=500)
        r = CkanResource('http://somewhere.com/test', None, 1, {'offset': None, 'limit': None})

        with assert_raises(StreamError):
            list(r.get_records())

    def test_solr_before(self):
        resource = CkanResource('http://somewhere.com/test', None, 1, {})
        request_params = {'offset': 12}
        resource._solr_before(request_params)
        assert_false('offset' in request_params)
        assert_equals(request_params['cursor'], '*')

    def test_solr_after(self):
        resource = CkanResource('http://somewhere.com/test', None, 1, {})
        request_params = {'cursor': '*'}
        result = {'next_cursor': 'next one!'}
        resource._solr_after(request_params, result)
        assert_equals(request_params['cursor'], 'next one!')

    def test_versioned_datastore_before(self):
        resource = CkanResource('http://somewhere.com/test', None, 1, {})
        request_params = {'offset': 12}
        resource._versioned_datastore_before(request_params)
        assert_false('offset' in request_params)

    def test_versioned_datastore_after(self):
        resource = CkanResource('http://somewhere.com/test', None, 1, {})
        request_params = {'cursor': '*'}
        result = {'after': 'next one!'}
        resource._versioned_datastore_after(request_params, result)
        assert_equals(request_params['after'], 'next one!')

    def test_default_before(self):
        resource = CkanResource('http://somewhere.com/test', None, 1, {})
        request_params = {'offset': 10, 'limit': 32, 'banana': True}
        copy_of_request_params = copy.deepcopy(request_params)
        resource._default_before(copy_of_request_params)
        # shouldn't do anything!
        assert_equals(request_params, copy_of_request_params)

    def test_default_after(self):
        resource = CkanResource('http://somewhere.com/test', None, 1, {})
        request_params = {'offset': 10, 'limit': 32}
        resource._default_after(request_params, {})
        assert_equals(request_params['offset'], 42)
