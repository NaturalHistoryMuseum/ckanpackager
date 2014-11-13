"""Test the CkanResource class"""
import httpretty
import json
import urllib
from nose.tools import assert_equals, assert_true
from ckanpackager.lib.ckan_resource import CkanResource, StreamError


class TestCkanResource:

    @httpretty.activate
    def test_stream_url(self):
        """Ensure we get a stream queried with the given URL"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None})
        with r.get_stream(0, 0) as s:
            assert_equals(httpretty.last_request().path, '/test')

    @httpretty.activate
    def test_stream_parameters(self):
        """Ensure request parameters are passed to the request"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None, 'carrot': 'cake'})
        with r.get_stream(10, 200) as s:
            req = json.loads(urllib.unquote(httpretty.last_request().body))
            assert_equals(req, {'offset': 10, 'limit': 200, 'carrot': 'cake'})

    @httpretty.activate
    def test_stream_limit_no_outer(self):
        """Ensure inner limit is used when no outer limit is defined"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None})
        with r.get_stream(10, 200) as s:
            req = json.loads(urllib.unquote(httpretty.last_request().body))
            assert_equals(req, {'offset': 10, 'limit': 200})

    @httpretty.activate
    def test_stream_limit_inner_larger(self):
        """Ensure limits are merged when making a request. Test with inner limit larger than outer limit"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        with r.get_stream(10, 200) as s:
            req = json.loads(urllib.unquote(httpretty.last_request().body))
            assert_equals(req, {'offset': 110, 'limit': 90})

    @httpretty.activate
    def test_stream_limit_inner_larger(self):
        """Ensure limits are merged when making a request. Test with inner limit larger than outer limit"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        with r.get_stream(10, 200) as s:
            req = json.loads(urllib.unquote(httpretty.last_request().body))
            assert_equals(req, {'offset': 110, 'limit': 90})

    @httpretty.activate
    def test_stream_limit_inner_smaller(self):
        """Ensure limits are merged when making a request. Test with inner limit smaller than outer limit"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        with r.get_stream(10, 20) as s:
            req = json.loads(urllib.unquote(httpretty.last_request().body))
            assert_equals(req, {'offset': 110, 'limit': 20})

    @httpretty.activate
    def test_stream_limit_overflow(self):
        """Ensure limits are merged when making a request. Test with inner limit causing overflow of outer limit"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        with r.get_stream(200, 20) as s:
            req = json.loads(urllib.unquote(httpretty.last_request().body))
            assert_equals(req, {'offset': 0, 'limit': 0})

    @httpretty.activate
    def test_stream_authorization(self):
        """Ensure an authorization header is added"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', 'somekey', {'offset': None, 'limit': None})
        with r.get_stream(200, 20) as s:
            headers = dict(httpretty.last_request().headers)
            assert_equals(headers['authorization'], 'somekey')

    @httpretty.activate
    def test_stream_failure(self):
        """Ensure an exception is raised when the query returns a non-200 status code"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test', status=500)
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None})
        try:
            with r.get_stream(200, 20):
                assert_true(False, "Expected exception StreamError")
        except StreamError:
            pass
