from ckanpackager.lib.utils import BadRequestError


class PackageTask():
    """Represents a packager task.

    Note that __init__ is run in the main thread, under flask, while 'run' and 'str' are typically
    run in a sub-process. __str__ and run should not access Flask API.
    """
    def __init__(self, params):
        """Create a packager task from request parameters.

        Required parameters are:
        - api: CKAN datastore_search action URL;
        - key: CKAN API key to make the request as;
        - resource_id: Resource to query

        Optional parameters are:
        - q: Full text search to perform on resource
        - filters: Filters to apply on resource
        - limit: Limit to apply on query
        """
        for req in ['api', 'key', 'resource_id']:
            if req not in params:
                raise BadRequestError("Parameter {} is required".format(req))

        self.request = {}
        for item in ['api', 'key', 'resource_id', 'q', 'filters', 'limit']:
            if item in params:
                self.request[item] = params[item]
            else:
                self.request[item] = None
        print self

    def run(self):
        """Run the task.

        Note that this is run in a separate process - we shouldn't attempt to use flask api from here.
        """
        pass

    def __str__(self):
        return str(self.request)
