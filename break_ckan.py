import json
import urllib
import urllib2

while True:
    request_params = {
        'resource_id': 'ef42d910-3e66-4df2-8b10-06f5ac07c29b',
        'offset': 0,
        'limit': 10000
    }
    request = urllib2.Request('http://ckan.local/api/action/datastore_search')
    response = urllib2.urlopen(request, urllib.quote(json.dumps(request_params)))
    str = response.read()
    print len(str)
    response.close()
