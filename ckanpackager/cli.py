#!/usr/bin/env python
"""Ckanpackager command line utility

This helps build and send requests to a ckanpackager instance.

Usage: ckanpackager [options] status
       ckanpackager [options] (cc|clear-cache)
       ckanpackager [options] (stats|statistics) [totals|requests|errors]
       ckanpackager [options] reset (requests|errors)
       ckanpackager [options] queue TASK [PARAM:VALUE ...]

Options:
    -h --help       Show this screen.
    --version       Show version.
    -q              Quiet. Don't output anything.
    -x		    When invoking 'stats' or 'stats errors', set the exit code
                    to 1 if there are errors in the queue.
    -p HOST         The ckanpackager host [default: http://127.0.0.1:8765]
    -d FILE         Path to JSON file containing default values for the
                    parameters of the queue request. Useful for specifying the
                    secret and api_url. If not specified, then ckanpackager-cli
                    will look for /etc/ckan/ckanpackager-cli.json and use that
                    if present unless -D is specified. Example:
                    {"secret": "...", "api_url": "http://.../api/3/action/datastore_search"}
                    Status, clear-cache and statistics commands will read the
                    secret from this file, but will ignore the other parameters.
    -D              Do not use JSON file containing defaults. Overrides -d.
    -s SECRET       The secret key. If present this will override the secret
                    key in the default file (but any secret defined in the
                    PARAM:VALUE parameters will override this one)
    -o OFFSET       Offset when querying requests/errors from the stats log
                    [default: 0]
    -l LIMIT        Limit when querying requests/errors from the stats log
                    [default: 100]
    -r RESOURCE_ID  Resource ID to filter on when querying requests/errors from
                    the stats log
    -e EMAIL        Email address to filter on when querying requests/errors
                    from the stats log
"""
import os
import sys
import docopt
import json
import urllib, urllib2

from version import __version__

DEFAULT_JSON_FILE = '/etc/ckanpackager/ckanpackager-cli.json'


class CkanPackagerError(Exception):
    """Exception class for user errors"""
    pass


class Request(object):
    def __init__(self, host, default_file, use_default, secret):
        """Represents a request to send to the ckanpackager service

        @param host: The ckanpackager host name
        @param defaults_file: Path to the json-encode file containing request
                              POST parameter defaults. If this is False,
                              it will attempt to load the default file
                              at '/etc/ckan/ckanpackager-cli.json'
        @param use_default: If False, do not use the defaults file.
        @param secret: If not False, the secret to sent to ckanpackager.
                       This will override the secret set in the defaults file,
                       but not secrets that may be defined later.
        """
        global DEFAULT_JSON_FILE

        self._host = host
        self._path = ''
        self._post = {}
        self._operation = ''
        self._result_contains_errors = False

        if use_default:
            provided_default_file = default_file is not False
            if not default_file:
                default_file = DEFAULT_JSON_FILE
            if os.path.isfile(default_file):
                with open(default_file) as f:
                    try:
                        self._post = json.load(f)
                    except ValueError:
                        raise CkanPackagerError(
                            'File {} is not in JSON format'.format(default_file)
                        )
            elif provided_default_file:
                raise CkanPackagerError('No such file: {}'.format(default_file))

        if secret:
            self._post['secret'] = secret

    def operation(self):
        return self._operation

    def status_request(self):
        """Make this a status request"""
        self._path = 'status'
        self._operation = 'Fetching status.'

    def cache_clear_request(self):
        """Make this a cache clear request"""
        self._path = 'clear_caches'
        self._operation = 'Clearing caches.'

    def queue_request(self, path, parameters):
        """Make this a generic task request

         @param path: Path for the generic task request
         @param_string: List of parameters encoded as 'field:value' strings
         """
        self._path = path
        for pv in parameters:
            if ':' not in pv:
                raise CkanPackagerError(
                    'Queue parameter list must be of the form PARAM:VALUE'
                )
            (param, value) = pv.split(':', 1)
            self._post[param] = value
        self._operation = 'Adding {} task to queue.'.format(path)

    def stats_totals_request(self):
        """Make this a request for general statistics"""
        self._path = 'statistics'
        self._operation = 'Fetching statistics.'

    def statistics_request(self, fetch_requests, offset, limit, resource_id, email):
        """Make this a requests/errors statistics request

        @param fetch_requests: True to fetch requests, False to fetch errors
        @param params: Parameters to send
        """
        if fetch_requests:
            self._path = 'statistics/requests'
        else:
            self._path = 'statistics/errors'
        post = {}
        post['offset'] = offset
        post['limit'] = limit
        if resource_id is not None:
            post['resource_id'] = resource_id
        if email is not None:
            post['email'] = email
        if 'secret' in self._post:
            post['secret'] = self._post['secret']
        self._post = post
        self._operation = 'Fetching statistics.'

    def send(self):
        """Send the request and return the json response as a dict"""
        url = '/'.join([j.strip('/') for j in [self._host, self._path]])
        try:
            response = urllib2.urlopen(url, urllib.urlencode(self._post))
        except urllib2.HTTPError as e:
            raise CkanPackagerError('Request failed: ' + str(e))
        data = json.loads(response.read())
        try:
            if 'errors' in data and len(data['errors']) > 0:
                self._result_contains_errors = True
            elif 'totals' in data and data['totals']['*']['errors'] != 0:
                self._result_contains_errors = True
        except KeyError:
                self._result_contains_errors = False
        return data

    def result_contains_errors(self):
        return self._result_contains_errors

def run():
    """Setup tools entry point"""
    arguments = docopt.docopt(__doc__, help=True, version=__version__)
    quiet = arguments['-q']
    error_exit_code = arguments['-x']
    try:
        request = Request(arguments['-p'], arguments['-d'],
                          not arguments['-D'], arguments['-s'])
        if arguments['status']:
            request.status_request()
        elif arguments['cc'] or arguments['clear-cache']:
            request.cache_clear_request()
        elif arguments['queue']:
            request.queue_request(arguments['TASK'], arguments['PARAM:VALUE'])
        elif arguments['stats'] or arguments['statistics']:
            if not arguments['requests'] and not arguments['errors']:
                request.stats_totals_request()
            else:
                request.statistics_request(
                    arguments['requests'],
                    int(arguments['-o']),
                    int(arguments['-l']),
                    arguments['-r'],
                    arguments['-e']
                )
        else:
            raise CkanPackagerError('Unknown command')
        if not quiet:
            print request.operation()
        data = request.send()
        if not quiet:
            print 'Response:'
            print json.dumps(data, indent=2)
        if error_exit_code and request.result_contains_errors():
            sys.exit(1)
        sys.exit(0)
    except CkanPackagerError as e:
        if not quiet:
            sys.stderr.write(str(e) + "\n")
        sys.exit(1)

if __name__ == '__main__':
    run()
