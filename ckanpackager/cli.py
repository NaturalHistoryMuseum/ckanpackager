#!/usr/bin/env python
"""Ckanpackager command line utility

This helps build and send requests to a ckanpackager instance.

Usage: ckanpackager [options] status
       ckanpackager [options] (cc|clear-cache)
       ckanpackager [options] queue TASK [PARAM:VALUE ...]

Options:
    -h --help       Show this screen.
    --version       Show version.
    -q              Quiet. Don't output anything.
    -p HOST         The ckanpackager host [default: http://127.0.0.1:8765]
    -d FILE         Path to JSON file containing default values for
                    parameters. Useful for specifying the secret and
                    api_url. If not specified, then ckanpackager-cli will look
                    for /etc/ckan/ckanpackager-cli.json and use that if present.
                    Example:
                    {"secret": "...", "api_url": "http://.../api/3/action/datastore_search"}
    -s SECRET       The secret key. If present this will override the secret
                    key in the default file (but any secret defined in the
                    PARAM:VALUE parameters will override this one)
"""
import os
import sys
import docopt
import json
import urllib, urllib2


VERSION = 'ckanpackager CLI 0.1'
DEFAULT_JSON_FILE = '/etc/ckan/ckanpackager-cli.json'


class CkanPackagerError(Exception):
    """Exception class for user errors"""
    pass


class Request(object):
    def __init__(self, host, default_file, secret):
        """Represents a request to send to the ckanpackager service

        @param host: The ckanpackager host name
        @param defaults_file: Path to the json-encode file containing request
                              POST parameter defaults. If this is False,
                              it will attempt to load the default file
                              at '/etc/ckan/ckanpackager-cli.json'
        @param secret: If not False, the secret to sent to ckanpackager.
                       This will override the secret set in the defaults file,
                       but not secrets that may be defined later.
        """
        global DEFAULT_JSON_FILE

        self._host = host
        self._path = ''
        self._post = {}
        self._operation = ''

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

    def send(self):
        """Send the request and return the json response as a dict"""
        url = '/'.join([j.strip('/') for j in [self._host, self._path]])
        try:
            response = urllib2.urlopen(url, urllib.urlencode(self._post))
        except urllib2.HTTPError as e:
            raise CkanPackagerError('Request failed: ' + str(e))
        return json.loads(response.read())


def run():
    """Setup tools entry point"""
    arguments = docopt.docopt(__doc__, help=True, version=VERSION)
    quiet = arguments['-q']
    try:
        request = Request(arguments['-p'], arguments['-d'], arguments['-s'])
        if arguments['status']:
            request.status_request()
        elif arguments['cc'] or arguments['clear-cache']:
            request.cache_clear_request()
        elif arguments['queue']:
            request.queue_request(arguments['TASK'], arguments['PARAM:VALUE'])
        else:
            raise CkanPackagerError('Unknown command')
        if not quiet:
            print request.operation()
        data = request.send()
        if not quiet:
            print 'Response:'
            print json.dumps(data, indent=2)
    except CkanPackagerError as e:
        if not quiet:
            sys.stderr.write(str(e) + "\n")
        sys.exit(1)

if __name__ == '__main__':
    run()