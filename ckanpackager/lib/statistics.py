import base64
import time

import bcrypt
import dataset


def extract_domain(email_address):
    """
    Given an email address, extract the domain name from it. This is done by finding the @ and
    then splicing the email address and returning everything found after the @. If no @ is found
    then the entire email address string is returned.

    :param email_address:
    :return:
    """
    email_address = email_address.lower()
    # figure out the domain from the email address
    try:
        return email_address[email_address.index(u'@') + 1:]
    except ValueError:
        # no @ found, just use the whole string
        return email_address


def anonymize_email(email_address, domain=None):
    """
    Hash the email address securely and return it as a string. We use bcrypt to do this and use the
    domain as the salt.

    :param email_address: the hashed email address
    """
    email_address = email_address.lower()
    if domain is None:
        domain = extract_domain(email_address)
    # create a custom salt by base64 encoding the domain and then trimming the whole thing to 22
    # characters (which is bcrypt's required salt length). Note that we fill the right side of the
    # domain with dots to ensure it's at least 18 characters in length. This is necessary as we need
    # to ensure that the base64 encode result is at least 22 characters long and 18 is the minimum
    # input length necessary to create a base64 encoding result of at least 22 characters.
    salt = u'$2b$12$' + base64.b64encode(domain.zfill(18))[:22]
    return bcrypt.hashpw(email_address.encode(u'utf-8'), salt.encode(u'utf-8'))


def anonymize_kwargs(kwargs):
    """
    Given a dict of kwargs, replace the value associated with the email key (if there is one)
    with the anonymized version of the email address. Does nothing if anonymization is turned
    off or if email isn't a key in the dict. Any changes are made in place.

    :param kwargs: a dict
    """
    # note that we use get instead of in on the kwargs as we want to only anonymize the email
    # address if it exists in the kwargs and isn't None
    if kwargs.get('email', None) is not None:
        kwargs['email'] = anonymize_email(kwargs['email'])


def statistics(database_url, anonymize):
    """Create a new CkanPackagerStatistics object and return it.

    This is useful for one-liners: statistics(db).log_request(request)

    @param database_url: database url as per
           http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
    @param anonymize: boolean indicating whether the email addresses in the database should be
                      treated anonymously
    """
    return CkanPackagerStatistics(database_url, anonymize)


class CkanPackagerStatistics(object):

    def __init__(self, database_url, anonymize):
        """Class used to track application statistics.

        @param database_url: database url as per 
               http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
        @param anonymize: boolean indicating whether the email addresses in the database should be
                          treated anonymously
        """
        self._db = dataset.connect(database_url)
        self.anonymize = anonymize

    def log_request(self, resource_id, email, count=None):
        """Log a new incoming request to the statistics
  
        @param resource_id: The resource id that was requested
        @param email: The email address that requested the resource
        """
        domain = extract_domain(email)
        if self.anonymize:
            email = anonymize_email(email, domain)

        # increase totals for all resources and the resource requested
        self._increase_totals('requests', resource_id='*')
        self._increase_totals('requests', resource_id=resource_id)

        # if there isn't already a request in the requests table from the email address we need to
        # increment the unique requesters count on all resources (*)
        if self._db['requests'].find_one(email=email) is None:
            self._increase_totals('emails', resource_id='*')

        # increase totals for that resource if the email address hasn't requested this resource
        # before
        resource_match = self._db['requests'].find_one(email=email, resource_id=resource_id)
        if resource_match is None:
            self._increase_totals('emails', resource_id=resource_id)

        # store request
        self._db['requests'].insert({
            u'count': count,
            u'domain': domain,
            u'email': email,
            u'resource_id': resource_id,
            u'timestamp': int(time.time()),
        })

    def log_error(self, resource_id, email, message):
        """Log a new error to the statistics

        @param resource_id: The resource id that was requested when the
                            error happened
        @param email: The email address that requested the resource
        @param message: The error message
        """
        if self.anonymize:
            email = anonymize_email(email)

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

    def get_requests(self, start=0, count=100, **kwargs):
        """Return requests as a list of dictionaries

        @param start: start of the query
        @param count: Number of requests to return
        @param **kwargs: conditions
        @returns: List of rows (as dictionaries)
        """
        if self.anonymize:
            anonymize_kwargs(kwargs)

        result = []
        iterator = self._db['requests'].find(
            _offset=start,
            _limit=count,
            order_by='-timestamp',
            **kwargs
        )
        for row in iterator:
            del row['id']
            result.append(row)
        return result

    def get_errors(self, start=0, count=100, **kwargs):
        """Return errors as a list of dicts

        @param start: start of the query
        @param count: number of requests to return
        @param **kwargs: conditions
        @returns: List of rows (as dictionaries)
        """
        if self.anonymize:
            anonymize_kwargs(kwargs)

        result = []
        iterator = self._db['errors'].find(
            _offset=start,
            _limit=count,
            order_by='-timestamp',
            **kwargs
        )
        for row in iterator:
            del row['id']
            result.append(row)
        return result

    def get_totals(self, **kwargs):
        """Return the overall stastitics (the totals)

        @param **kwargs: conditions on the totals table
        @returns: Dictionary of rows (as dictionaries), indexed by the resource
                  id.
        """
        totals = {}
        for row in self._db['totals'].find(**kwargs):
            totals[row['resource_id']] = {
                'emails': row['emails'],
                'errors': row['errors'],
                'requests': row['requests']
            }
        return totals
 
    def _increase_totals(self, counter, **kwargs):
        """Increase the given counter
       
        @param counter: Name of the counter
        @param **kwargs: conditions
        """
        r = self._db['totals'].find_one(**kwargs)
        if r is None:
            r = {
                'resource_id': '*',
                'errors': 0,
                'requests': 0,
                'emails': 0
            }
            for key in kwargs:
                r[key] = kwargs[key]
        r[counter] += 1
        self._db['totals'].upsert(r, kwargs.keys())
