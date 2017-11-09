import json
import requests
import urllib
import urllib2
from contextlib import contextmanager


class StreamError(Exception):
    """Exception raised when there is an error parsing the stream from CKAN"""
    pass


class CkanResource():
    """Represents and queries a resource on a CKAN server"""
    def __init__(self, api_url, key, request_params):
        """Create a new Ckan Resource

        @param api_url: URL for the CKAN API call
        @param key: The CKAN Key (May be None)
        @param request_params: Request parameters to send to CKAN
        """
        self.api_url = api_url
        self.key = key
        self.request_params = request_params

    def _get_response(self, page, page_size, cursor=None):
        """
        Helper function to request via resource API
        This is abstracted from self.request to allow testing the response in unit tests
        :param offset:
        :param limit:
        :param cursor:
        :return:
        """
        request_params = dict([(k, v) for (k, v) in self.request_params.items() if v is not None])
        # User has passed in an initial offset in the download request
        offset_param = int(self.request_params.get('offset', 0))
        request_params['offset'] = page * page_size + offset_param

        # If user has passed in limit which is less than the page size,
        # limit the results to that - otherwise use page size
        limit_param = int(self.request_params.get('limit', 0))
        request_params['limit'] = limit_param if limit_param and limit_param < page_size else page_size

        if cursor:
            request_params['cursor'] = cursor
        headers = {}
        if self.key:
            headers['Authorization'] = self.key

        # Filters are parsed as a dict - encode to json
        try:
            request_params['filters'] = json.dumps(request_params['filters'])
        except KeyError:
            pass
        response = requests.post(self.api_url, params=request_params, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise StreamError("Failed fetching URL {}: {}".format(self.api_url, e))
        return response

    def request(self, start_offset, page_size, cursor=None):
        response = self._get_response(start_offset, page_size, cursor)
        return response.json()