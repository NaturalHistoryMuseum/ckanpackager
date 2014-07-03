"""Test the CkanResource class

FIXME: We rely on patching,which ties the test to the implementation choice. Should we use
https://github.com/gabrielfalcao/HTTPretty and mock a server instead?
"""
import mock
import json
import urllib
from nose.tools import assert_equals, assert_true
from ckanpackager.lib.ckan_resource import CkanResource, StreamError


class TestCkanResource:

    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.Request')
    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.urlopen')
    def test_stream_url(self, mock_open, mock_request):
        """Ensure we get a stream queried with the given URL"""
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None})
        mock_open.return_value.code = 200
        with r.get_stream(0, 0) as s:
            assert_true(mock_request.called)
            assert_equals(mock_request.call_args, mock.call('http://somewhere.com/test'))

    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.Request')
    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.urlopen')
    def test_stream_parameters(self, mock_open, mock_request):
        """Ensure request parameters are passed to the request"""
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None, 'carrot': 'cake'})
        mock_open.return_value.code = 200
        with r.get_stream(10, 200) as s:
            assert_true(mock_open.called)
            req = json.loads(urllib.unquote(mock_open.call_args[0][1]))
            assert_equals(req, {'offset': 10, 'limit': 200, 'carrot': 'cake'})

    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.Request')
    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.urlopen')
    def test_stream_limit_no_outer(self, mock_open, mock_request):
        """Ensure inner limit is used when no outer limit is defined"""
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None})
        mock_open.return_value.code = 200
        with r.get_stream(10, 200) as s:
            assert_true(mock_open.called)
            req = json.loads(urllib.unquote(mock_open.call_args[0][1]))
            assert_equals(req, {'offset': 10, 'limit': 200})

    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.Request')
    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.urlopen')
    def test_stream_limit_inner_larger(self, mock_open, mock_request):
        """Ensure limits are merged when making a request. Test with inner limit larger than outer limit"""
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        mock_open.return_value.code = 200
        with r.get_stream(10, 200) as s:
            assert_true(mock_open.called)
            req = json.loads(urllib.unquote(mock_open.call_args[0][1]))
            assert_equals(req, {'offset': 110, 'limit': 90})


    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.Request')
    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.urlopen')
    def test_stream_limit_inner_larger(self, mock_open, mock_request):
        """Ensure limits are merged when making a request. Test with inner limit larger than outer limit"""
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        mock_open.return_value.code = 200
        with r.get_stream(10, 200) as s:
            assert_true(mock_open.called)
            req = json.loads(urllib.unquote(mock_open.call_args[0][1]))
            assert_equals(req, {'offset': 110, 'limit': 90})

    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.Request')
    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.urlopen')
    def test_stream_limit_inner_smaller(self, mock_open, mock_request):
        """Ensure limits are merged when making a request. Test with inner limit smaller than outer limit"""
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        mock_open.return_value.code = 200
        with r.get_stream(10, 20) as s:
            assert_true(mock_open.called)
            req = json.loads(urllib.unquote(mock_open.call_args[0][1]))
            assert_equals(req, {'offset': 110, 'limit': 20})

    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.Request')
    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.urlopen')
    def test_stream_limit_overflow(self, mock_open, mock_request):
        """Ensure limits are merged when making a request. Test with inner limit causing overflow of outer limit"""
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        mock_open.return_value.code = 200
        with r.get_stream(200, 20) as s:
            assert_true(mock_open.called)
            req = json.loads(urllib.unquote(mock_open.call_args[0][1]))
            assert_equals(req, {'offset': 0, 'limit': 0})

    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.Request')
    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.urlopen')
    def test_stream_authorization(self, mock_open, mock_request):
        """Ensure an authorization header is added"""
        r = CkanResource('http://somewhere.com/test', 'somekey', {'offset': None, 'limit': None})
        mock_open.return_value.code = 200
        with r.get_stream(200, 20) as s:
            assert_true(mock_request.return_value.add_header.called)
            assert_equals(mock_request.return_value.add_header.call_args, mock.call('Authorization', 'somekey'))

    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.Request')
    @mock.patch('ckanpackager.lib.ckan_resource.urllib2.urlopen')
    def test_stream_failure(self, mock_open, mock_request):
        """Ensure an exception is raised when the query returns a non-200 status code"""
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None})
        mock_open.return_value.code = 403
        try:
            with r.get_stream(200, 20):
                assert_true(False, "Expected exception StreamError")
        except StreamError:
            pass
