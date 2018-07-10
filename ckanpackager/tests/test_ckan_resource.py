"""Test the CkanResource class"""
import httpretty
import urlparse
from nose.tools import assert_equals, assert_true
from ckanpackager.lib.ckan_resource import CkanResource, StreamError

def url_get_params(url):
    """
    Helper function to compare url to expected params
    Parse the URLs to a dict, and compare to param dict
    Will try and coerce param values into integers
    :param url:
    :param param_dict:
    :return:
    """

    def cast_int(v):
        """
        Try and convert a param to int
        :param v:
        :return:
        """
        try:
            return int(v)
        except ValueError:
            return v

    parsed_url = urlparse.urlparse(httpretty.last_request().path)
    url_params = {k: cast_int(v) for k, v in dict(urlparse.parse_qsl(parsed_url.query)).items()}
    return url_params

class TestCkanResource:

    @httpretty.activate
    def test_request_url(self):
        """Ensure we get a stream queried with the given URL"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None})
        s = r._get_response(0, 0)
        assert_equals(httpretty.last_request().path, '/test?limit=0&offset=0')

    @httpretty.activate
    def test_request_parameters(self):
        """Ensure request parameters are passed to the request"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None, 'carrot': 'cake'})
        s = r._get_response(10, 200)
        self._assert_params_equals(httpretty.last_request().path, {'offset': 2000, 'limit': 200, 'carrot': 'cake'})


    @httpretty.activate
    def test_request_limit_no_outer(self):
        """Ensure inner limit is used when no outer limit is defined"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None})
        s = r._get_response(10, 200)
        self._assert_params_equals(httpretty.last_request().path, {'offset': 2000, 'limit': 200})


    def _assert_params_equals(self, url, param_dict):
        """
        Helper function to compare url to expected params
        Parse the URLs to a dict, and compare to param dict
        Will try and coerce param values into integers
        :param url:
        :param param_dict:
        :return:
        """
        url_params = url_get_params(url)
        assert_equals(url_params, param_dict)

    @httpretty.activate
    def test_request_limit_inner_larger(self):
        """Ensure limits are merged when making a request. Test with inner limit larger than outer limit"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        r._get_response(10, 200)
        self._assert_params_equals(httpretty.last_request().path, {'offset': 2100, 'limit': 100})

    @httpretty.activate
    def test_request_limit_inner_smaller(self):
        """Ensure limits are merged when making a request. Test with inner limit smaller than outer limit"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        r._get_response(10, 20)
        self._assert_params_equals(httpretty.last_request().path, {'offset': 300, 'limit': 20})

    @httpretty.activate
    def test_request_limit_overflow(self):
        """Ensure limits are merged when making a request. Test with inner limit causing overflow of outer limit"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', None, {'offset': 100, 'limit': 100})
        r._get_response(200, 20)
        self._assert_params_equals(httpretty.last_request().path, {'offset': 4100, 'limit': 20})

    @httpretty.activate
    def test_request_authorization(self):
        """Ensure an authorization header is added"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test')
        r = CkanResource('http://somewhere.com/test', 'somekey', {'offset': None, 'limit': None})
        r._get_response(200, 20)
        headers = dict(httpretty.last_request().headers)
        assert_equals(headers['authorization'], 'somekey')

    @httpretty.activate
    def test_request_failure(self):
        """Ensure an exception is raised when the query returns a non-200 status code"""
        httpretty.register_uri(httpretty.POST, 'http://somewhere.com/test', status=500)
        r = CkanResource('http://somewhere.com/test', None, {'offset': None, 'limit': None})
        try:
            r._get_response(200, 20)
            assert_true(False, "Expected exception StreamError")
        except StreamError:
            pass