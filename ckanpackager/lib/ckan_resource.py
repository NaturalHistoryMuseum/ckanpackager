import json
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

    @contextmanager
    def get_stream(self, offset, limit):
        """Yield a file-like object represent the data for the current request.

        @param offset: Offset to apply to the query. Will be applied in addition to the offset defined
                       in the tasks' request parameters (see _merge_limits)
        @param limit: Limit to apply to the query. Will be applied in addition to the limit defined in the tasks'
                      request parameters (see _merge_limits)
        @yield: file-like stream object
        """
        (offset, limit) = self._merge_limits(self.request_params.get('offset', None),
                                             self.request_params.get('limit', None),
                                             offset, limit)
        request_params = dict([(k, v) for (k, v) in self.request_params.items() if v is not None])
        request_params['offset'] = offset
        request_params['limit'] = limit
        try:
            request = urllib2.Request(self.api_url)
            if self.key:
                request.add_header('Authorization', self.key)
            response = urllib2.urlopen(request, urllib.quote(json.dumps(request_params)))
        except urllib2.URLError as e:
            raise StreamError("Failed fetching URL {}: {}".format(self.api_url, e))
        if response.code != 200:
            response.close()
            raise StreamError("URL {} return status code {}".format(self.api_url, response.code))
        try:
            yield response
        finally:
            response.close()

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
