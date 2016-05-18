import time
import dataset


def statistics(database_url):
    """Create a new CkanPackagerStatistics object and return it.

    This is useful for one-liners: statistics(db).log_request(request)

    @param database_url: database url as per
           http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
    """
    return CkanPackagerStatistics(database_url)


class CkanPackagerStatistics(object):
    def __init__(self, database_url):
        """Class used to track application statistics.

        @param database_url: database url as per 
               http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
        """
        self._db = dataset.connect(database_url)

    def log_request(self, resource_id, email, count):
        """Log a new incoming request to the statistics
  
        @param resource_id: The resource id that was requested
        @param email: The email address that requested the resource
        """
        # Increase totals
        self._increase_totals('requests', resource_id='*')
        if self._db['requests'].find_one(email=email) is None:
            self._increase_totals('emails', resource_id='*')
        # Increase totals for that resource
        self._increase_totals('requests', resource_id=resource_id)
        resource_match = self._db['requests'].find_one(
            email=email,
            resource_id=resource_id
        )
        if resource_match is None:
            self._increase_totals('emails', resource_id=resource_id)

        # Store timestamped request 
        self._db['requests'].insert({
            'timestamp': int(time.time()),
            'resource_id': resource_id,
            'email': email,
            'count': count
        })
            
    def log_error(self, resource_id, email, message):
        """Log a new error to the statistics

        @param resource_id: The resource id that was requested when the
                            error happened
        @param email: The email address that requested the resource
        @param message: The error message
        """
        # Increase totals
        self._increase_totals('errors', resource_id='*')
        # Increase totals for that resource
        self._increase_totals('errors', resource_id=resource_id)
        # Store timestamped error 
        self._db['errors'].insert({
            'timestamp': int(time.time()),
            'resource_id': resource_id,
            'email': email,
            'message': message
        })

    def get_requests(self, start=0, count=100, **kargs):
        """Return requests as a list of dictionaries

        @param start: start of the query
        @param count: Number of requests to return
        @param **kargs: conditions
        @returns: List of rows (as dictionaries)
        """
        result = []
        iterator = self._db['requests'].find(
            _offset=start,
            _limit=count,
            order_by='-timestamp',
            **kargs
        )
        for row in iterator:
            del row['id']
            result.append(row)
        return result

    def get_errors(self, start=0, count=100, **kargs):
        """Return errors as a list of dicts

        @param start: start of the query
        @param count: number of requests to return
        @param **kargs: conditions
        @returns: List of rows (as dictionaries)
        """
        result = []
        iterator = self._db['errors'].find(
            _offset=start,
            _limit=count,
            order_by='-timestamp',
            **kargs
        )
        for row in iterator:
            del row['id']
            result.append(row)
        return result

    def get_totals(self, **kargs):
        """Return the overall stastitics (the totals)

        @param **kargs: conditions on the totals table
        @returns: Dictionary of rows (as dictionaries), indexed by the resource
                  id.
        """
        totals = {}
        for row in self._db['totals'].find(**kargs):
            totals[row['resource_id']] = {
                'emails': row['emails'],
                'errors': row['errors'],
                'requests': row['requests']
            }
        return totals
 
    def _increase_totals(self, counter, **kargs):
        """Increase the given counter
       
        @param counter: Name of the counter
        @param **kargs: conditions
        """
        r = self._db['totals'].find_one(**kargs)
        if r is None:
            r = {
                'resource_id': '*',
                'errors': 0,
                'requests': 0,
                'emails': 0
            }
            for key in kargs:
                r[key] = kargs[key]
        r[counter] += 1
        self._db['totals'].upsert(r, kargs.keys())
