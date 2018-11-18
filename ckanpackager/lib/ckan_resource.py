import copy

import requests


class StreamError(Exception):
    """Exception raised when there is an error parsing the stream from CKAN"""
    pass


class CkanResource(object):
    """
    Represents and queries a resource on a CKAN server.
    """

    def __init__(self, api_url, key, page_size, params):
        """
        :param api_url: URL for the CKAN API call
        :param key: the CKAN authentication key (may be None)
        :param page_size: the maximum number of records to retrieve with each call
        :param params: request parameters to send to CKAN
        """
        self.api_url = api_url
        self.page_size = page_size
        # remove any parameters with no value
        self.params = {k: v for k, v in params.items() if v is not None}
        self.headers = {}
        if key:
            self.headers['Authorization'] = key

        # store other backends that we can handle the pagnination for beyond the default datastore.
        # Each has a 2-tuple containing a function to be called before making any requests to CKAN
        # and then an after function to be called after each page is retrieved from CKAN.
        self.backends = {
            'solr': (self._solr_before, self._solr_after),
            'versioned-datastore': (self._versioned_datastore_before,
                                    self._versioned_datastore_after),
        }

    def get_fields_and_backend(self):
        """
        Retrieves the fields and backend information about the resource. This is achieved through a
        single request for no records.

        :return: a 2-tuple of a fields list and a backend (can be None if the response doesn't
                 specify one)
        """
        request_params = copy.deepcopy(self.params)
        request_params['offset'] = 0
        request_params['limit'] = 0
        response = requests.post(self.api_url, json=request_params, headers=self.headers)
        response.raise_for_status()
        result = response.json()['result']
        return result['fields'], result.get('_backend', None)

    def get_records(self, backend=None):
        """
        Retrieves the all records as requested from the CKAN API URL using the appropriate paging
        mechanism dependant on the given backend.

        If an error occurs retrieving the data from the CKAN server, a StreamError will be raised.

        :param backend: the name of the backend to use (default: None)
        :return: a generator of records
        """
        request_params = copy.deepcopy(self.params)
        request_params['offset'] = int(request_params.get('offset', 0))
        requested_count = int(request_params.get('limit', 0))
        # if no limit is specified we request all the records and use the default page size
        if requested_count == 0:
            request_params['limit'] = self.page_size
        else:
            # set the limit to the smaller value so that we don't request a large number of records
            # when all we actually need is one (for example)
            request_params['limit'] = min(self.page_size, requested_count)

        # if there is an offset already in the request params then we can't fulfill this request
        # using the solr or versioned-datastore cursor/search after pagination techniques
        if request_params['offset'] > 0:
            backend = None
        before, after = self.backends.get(backend, (self._default_before, self._default_after))

        before(request_params)
        count = 0
        while True:
            try:
                response = requests.post(self.api_url, json=request_params, headers=self.headers)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise StreamError("Failed fetching URL {}: {}".format(self.api_url, e))

            result = response.json()['result']
            if not result['records']:
                return
            for record in result['records']:
                yield record
                count += 1
                if count == requested_count:
                    return
            after(request_params, result)

    @staticmethod
    def _default_before(request_params):
        """
        If using the default way of paginating the data (offset/limit), then we don't need to do
        anything before the first request.

        :param request_params: a dict of request parameters
        """
        pass

    @staticmethod
    def _solr_before(request_params):
        """
        If using the solr way of paginating the data (cursor based), then we don't need the offset
        value and we need to setup the cursor value.

        :param request_params: a dict of request parameters
        """
        del request_params['offset']
        request_params['cursor'] = '*'

    @staticmethod
    def _versioned_datastore_before(request_params):
        """
        If using the versioned-datastore way of paginating the data (elasticsearch's search after),
        then we don't need the offset value.

        :param request_params: a dict of request parameters
        """
        del request_params['offset']

    @staticmethod
    def _default_after(request_params, _result):
        """
        If using the default way of paginating the data (offset/limit), then we need to update the
        offset parameter each time a request is completed.

        :param request_params: a dict of request parameters
        :param _result: the result dict, not used here
        """
        request_params['offset'] += request_params['limit']

    @staticmethod
    def _solr_after(request_params, result):
        """
        If using the solr way of paginating the data (cursor based), then we need to update the
        cursor parameter.

        :param request_params: a dict of request parameters
        :param result: the result dict
        """
        request_params['cursor'] = result['next_cursor']

    @staticmethod
    def _versioned_datastore_after(request_params, result):
        """
        If using the versioned-datastore way of paginating the data (elasticsearch's search after),
        then we need to update the after parameter.

        :param request_params: a dict of request parameters
        :param result: the result dict
        """
        request_params['after'] = result['after']
