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

    def _get_response(self, offset, limit, cursor=None):
        """
        Helper function to request via resource API
        This is abstracted from self.request to allow testing the response in unit tests
        :param offset:
        :param limit:
        :param cursor:
        :return:
        """

        (offset, limit) = self._merge_limits(self.request_params.get('offset', None),
                                             self.request_params.get('limit', None),
                                             offset, limit)

        request_params = dict([(k, v) for (k, v) in self.request_params.items() if v is not None])
        request_params['offset'] = offset
        request_params['limit'] = limit
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

    def request(self, offset, limit, cursor=None):
        response = self._get_response(offset, limit, cursor)
        return response.json()

    def _merge_limits(self, base_offset, base_limit, inner_offset, inner_limit):
        """Given data defined by an offset/limit within a dataset, apply an offset/limit within that data.

        So with a base of (100,50) and an inner limit of (10, 200) you would get (110, 40).

        @param base_offset: The base offset (str or int), or None
        @param base_limit: The base limit (str or int), or None
        @param inner_offset: The inner limit (int)
        @param inner_limit: The inner limit (int)
        @return: A tuple defining (offset, limit) to apply to the original dataset
        """
        if base_offset is None:
            offset = inner_offset
        else:
            offset = int(base_offset) + inner_offset
        if base_limit is None:
            limit = inner_limit
        else:
            limit = int(base_limit) - inner_offset
            if inner_limit < limit:
                limit = inner_limit
        if limit < 0:
            limit = 0
            offset = 0
        return offset, limit
