import time

from mock import patch
from nose.tools import assert_equals, assert_true, assert_not_in, assert_in, assert_not_equals
from ckanpackager.lib.statistics import CkanPackagerStatistics, statistics, extract_domain, \
    anonymize_email, anonymize_kwargs


class TestStatistics(object):
    def setUp(self):
        """Create a statistics object"""
        self._d = CkanPackagerStatistics('sqlite:///:memory:', False)

    def test_log_request(self):
        """Test that requests are logged"""
        assert_equals(0, len(self._d.get_requests()))
        self._d.log_request('abcd', 'someone@example.com')
        assert_equals(1, len(self._d.get_requests()))

    def test_log_multiple_request(self):
        """Test that multiple requests are logged"""
        assert_equals(0, len(self._d.get_requests()))
        self._d.log_request('abcd', 'someone@example.com')
        self._d.log_request('abcd', 'someone@example.com')
        self._d.log_request('abcd', 'someone@example.com')
        assert_equals(3, len(self._d.get_requests()))

    def test_request_fields(self):
        """Ensure logged request fields contain expected data"""
        self._d.log_request('abcd', 'someone@example.com')
        requests = self._d.get_requests()
        assert_equals(1, len(requests))
        assert_equals('abcd', requests[0]['resource_id'])
        assert_equals('someone@example.com', requests[0]['email'])
        assert_equals('example.com', requests[0]['domain'])
        assert_equals(type(requests[0]['timestamp']), int)
        # For the stats, an hour precision is enough - and this test
        # is unlikely to take more time so this test should be good.
        assert_true(int(time.time()) - requests[0]['timestamp'] < 60*60)

    def test_log_error(self):
        """Test that errors are logged"""
        assert_equals(0, len(self._d.get_errors()))
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        assert_equals(1, len(self._d.get_errors()))

    def test_log_multiple_error(self):
        """Test that multiple errors are logged"""
        assert_equals(0, len(self._d.get_errors()))
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        assert_equals(3, len(self._d.get_errors()))

    def test_error_fields(self):
        """Test that logged error fields contain expected data"""
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        errors = self._d.get_errors()
        assert_equals(1, len(errors))
        assert_equals('abcd', errors[0]['resource_id'])
        assert_equals('someone@example.com', errors[0]['email'])
        assert_equals('it failed', errors[0]['message'])
        assert_equals(type(errors[0]['timestamp']), int)
        # For the stats, an hour precision is enough - and this test
        # is unlikely to take more time so this test should be good.
        assert_true(int(time.time()) - errors[0]['timestamp'] < 60*60)

    def test_overall_request_totals_updated(self):
        """Test that the overall request totals are updated"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('efgh', 'someone2@example.com')
        totals = self._d.get_totals()
        assert_equals(3, totals['*']['requests'])

    def test_overall_error_totals_updated(self):
        """Test that the overall error totals are updated"""
        self._d.log_error('abcd', 'someone1@example.com', 'it failed')
        self._d.log_error('abcd', 'someone1@example.com', 'it failed')
        self._d.log_error('abcd', 'someone2@example.com', 'it failed')
        self._d.log_error('efgh', 'someone3@example.com', 'it failed')
        totals = self._d.get_totals()
        assert_equals(4, totals['*']['errors'])

    def test_per_resource_request_totals_updated(self):
        """Test that per-resource request totals are updated"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('efgh', 'someone2@example.com')
        totals = self._d.get_totals()
        assert_equals(2, totals['abcd']['requests'])

    def test_per_resource_error_totals_updated(self):
        """Test that the per-resource error totals are updated"""
        self._d.log_error('abcd', 'someone1@example.com', 'it failed')
        self._d.log_error('abcd', 'someone1@example.com', 'it failed')
        self._d.log_error('abcd', 'someone2@example.com', 'it failed')
        self._d.log_error('efgh', 'someone3@example.com', 'it failed')
        totals = self._d.get_totals()
        assert_equals(3, totals['abcd']['errors'])

    def test_overall_unique_emails_updated(self):
        """Test that the overall number of unique emails are updated"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('efgh', 'someone2@example.com')
        totals = self._d.get_totals()
        assert_equals(2, totals['*']['emails'])

    def test_per_resource_unique_emails_updated(self):
        """Test that the per-resource number of unique emails are updated"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone2@example.com')
        self._d.log_request('efgh', 'someone3@example.com')
        totals = self._d.get_totals()
        assert_equals(2, totals['abcd']['emails'])

    def test_totals_dont_include_id(self):
        """Check that the totals returned don't include an id field"""
        self._d.log_request('abcd', 'someone1@example.com')
        totals = self._d.get_totals()
        assert_not_in('id', totals['*'])
        assert_not_in('resource_id', totals['*'])
        assert_not_in('id', totals['abcd'])
        assert_not_in('resource_id', totals['abcd'])

    def test_totals_return_all_resources(self):
        """Check that, unfilterd, get_totals returns entries for all resources"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('efgh', 'someone3@example.com')
        self._d.log_request('ijkl', 'someone3@example.com')
        totals = self._d.get_totals()
        assert_in('*', totals)
        assert_in('abcd', totals)
        assert_in('efgh', totals)
        assert_in('ijkl', totals)

    def test_totals_filters(self):
        """Check it's possible to filter the rows returned by get_totals"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone2@example.com')
        self._d.log_request('efgh', 'someone3@example.com')
        totals = self._d.get_totals(resource_id='abcd')
        assert_not_in('*', totals)
        assert_not_in('efgh', totals)
        assert_in('abcd', totals)

    def test_requests_dont_include_id(self):
        """Check that the requests returned don't include an id field"""
        self._d.log_request('abcd', 'someone1@example.com')
        requests = self._d.get_requests()
        assert_not_in('id', requests[0])

    def test_errors_dont_include_id(self):
        """Check that the errors returned don't include an id field"""
        self._d.log_error('abcd', 'someone1@example.com', 'borken')
        errors = self._d.get_errors()
        assert_not_in('id', errors[0])

    def test_requests_ordered_by_timestamp_desc(self):
        """Check that the returned requests are ordered by timestamp desc"""
        self._d.log_request('abcd', 'someone1@example.com')
        time.sleep(1)
        self._d.log_request('abcd', 'someone1@example.com')
        time.sleep(1)
        self._d.log_request('abcd', 'someone2@example.com')
        requests = self._d.get_requests()
        assert_true(requests[0]['timestamp'] > requests[1]['timestamp'])
        assert_true(requests[1]['timestamp'] > requests[2]['timestamp'])

    def test_errors_ordered_by_timestamp_desc(self):
        """Check that the returned requests are ordered by timestamp desc"""
        self._d.log_error('abcd', 'someone1@example.com', 'borken')
        time.sleep(1)
        self._d.log_error('abcd', 'someone1@example.com', 'borken')
        time.sleep(1)
        self._d.log_error('abcd', 'someone2@example.com', 'borken')
        errors = self._d.get_errors()
        assert_true(errors[0]['timestamp'] > errors[1]['timestamp'])
        assert_true(errors[1]['timestamp'] > errors[2]['timestamp'])

    def test_statistics_shortcut(self):
        """Check that the 'statistics' shortcut returns an object as expected"""
        o = statistics('sqlite:///:memory:', False)
        assert_equals(CkanPackagerStatistics, type(o))


class TestStatisticsAnonymized(object):

    def setUp(self):
        """
        Create a statistics object with anonymizing turned on.
        """
        self._d = CkanPackagerStatistics('sqlite:///:memory:', True)

        self.someone_hash = anonymize_email(u'someone@example.com')

    def test_log_request(self):
        """
        Test that requests are logged
        """
        assert_equals(0, len(self._d.get_requests()))
        self._d.log_request('abcd', 'someone@example.com')
        assert_equals(1, len(self._d.get_requests()))

    def test_log_multiple_request(self):
        """Test that multiple requests are logged"""
        assert_equals(0, len(self._d.get_requests()))
        self._d.log_request('abcd', 'someone@example.com')
        self._d.log_request('abcd', 'someone@example.com')
        self._d.log_request('abcd', 'someone@example.com')
        assert_equals(3, len(self._d.get_requests()))

    def test_request_fields(self):
        """Ensure logged request fields contain expected data"""
        self._d.log_request('abcd', 'someone@example.com')
        requests = self._d.get_requests()
        assert_equals(1, len(requests))
        assert_equals('abcd', requests[0]['resource_id'])
        assert_equals(self.someone_hash, requests[0]['email'])
        assert_equals('example.com', requests[0]['domain'])
        assert_equals(type(requests[0]['timestamp']), int)
        # For the stats, an hour precision is enough - and this test
        # is unlikely to take more time so this test should be good.
        assert_true(int(time.time()) - requests[0]['timestamp'] < 60*60)

    def test_log_error(self):
        """Test that errors are logged"""
        assert_equals(0, len(self._d.get_errors()))
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        assert_equals(1, len(self._d.get_errors()))

    def test_log_multiple_error(self):
        """Test that multiple errors are logged"""
        assert_equals(0, len(self._d.get_errors()))
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        assert_equals(3, len(self._d.get_errors()))

    def test_error_fields(self):
        """Test that logged error fields contain expected data"""
        self._d.log_error('abcd', 'someone@example.com', 'it failed')
        errors = self._d.get_errors()
        assert_equals(1, len(errors))
        assert_equals('abcd', errors[0]['resource_id'])
        assert_equals(self.someone_hash, errors[0]['email'])
        assert_equals('it failed', errors[0]['message'])
        assert_equals(type(errors[0]['timestamp']), int)
        # For the stats, an hour precision is enough - and this test
        # is unlikely to take more time so this test should be good.
        assert_true(int(time.time()) - errors[0]['timestamp'] < 60*60)

    def test_overall_request_totals_updated(self):
        """Test that the overall request totals are updated"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('efgh', 'someone2@example.com')
        totals = self._d.get_totals()
        assert_equals(3, totals['*']['requests'])

    def test_overall_error_totals_updated(self):
        """Test that the overall error totals are updated"""
        self._d.log_error('abcd', 'someone1@example.com', 'it failed')
        self._d.log_error('abcd', 'someone1@example.com', 'it failed')
        self._d.log_error('abcd', 'someone2@example.com', 'it failed')
        self._d.log_error('efgh', 'someone3@example.com', 'it failed')
        totals = self._d.get_totals()
        assert_equals(4, totals['*']['errors'])

    def test_per_resource_request_totals_updated(self):
        """Test that per-resource request totals are updated"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('efgh', 'someone2@example.com')
        totals = self._d.get_totals()
        assert_equals(2, totals['abcd']['requests'])

    def test_per_resource_error_totals_updated(self):
        """Test that the per-resource error totals are updated"""
        self._d.log_error('abcd', 'someone1@example.com', 'it failed')
        self._d.log_error('abcd', 'someone1@example.com', 'it failed')
        self._d.log_error('abcd', 'someone2@example.com', 'it failed')
        self._d.log_error('efgh', 'someone3@example.com', 'it failed')
        totals = self._d.get_totals()
        assert_equals(3, totals['abcd']['errors'])

    def test_overall_unique_emails_updated(self):
        """Test that the overall number of unique emails are updated"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('efgh', 'someone2@example.com')
        totals = self._d.get_totals()
        assert_equals(2, totals['*']['emails'])

    def test_per_resource_unique_emails_updated(self):
        """Test that the per-resource number of unique emails are updated"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone2@example.com')
        self._d.log_request('efgh', 'someone3@example.com')
        totals = self._d.get_totals()
        assert_equals(2, totals['abcd']['emails'])

    def test_totals_dont_include_id(self):
        """Check that the totals returned don't include an id field"""
        self._d.log_request('abcd', 'someone1@example.com')
        totals = self._d.get_totals()
        assert_not_in('id', totals['*'])
        assert_not_in('resource_id', totals['*'])
        assert_not_in('id', totals['abcd'])
        assert_not_in('resource_id', totals['abcd'])

    def test_totals_return_all_resources(self):
        """Check that, unfilterd, get_totals returns entries for all resources"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('efgh', 'someone3@example.com')
        self._d.log_request('ijkl', 'someone3@example.com')
        totals = self._d.get_totals()
        assert_in('*', totals)
        assert_in('abcd', totals)
        assert_in('efgh', totals)
        assert_in('ijkl', totals)

    def test_totals_filters(self):
        """Check it's possible to filter the rows returned by get_totals"""
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone1@example.com')
        self._d.log_request('abcd', 'someone2@example.com')
        self._d.log_request('efgh', 'someone3@example.com')
        totals = self._d.get_totals(resource_id='abcd')
        assert_not_in('*', totals)
        assert_not_in('efgh', totals)
        assert_in('abcd', totals)

    def test_requests_dont_include_id(self):
        """Check that the requests returned don't include an id field"""
        self._d.log_request('abcd', 'someone1@example.com')
        requests = self._d.get_requests()
        assert_not_in('id', requests[0])

    def test_errors_dont_include_id(self):
        """Check that the errors returned don't include an id field"""
        self._d.log_error('abcd', 'someone1@example.com', 'borken')
        errors = self._d.get_errors()
        assert_not_in('id', errors[0])

    def test_requests_ordered_by_timestamp_desc(self):
        """Check that the returned requests are ordered by timestamp desc"""
        self._d.log_request('abcd', 'someone1@example.com')
        time.sleep(1)
        self._d.log_request('abcd', 'someone1@example.com')
        time.sleep(1)
        self._d.log_request('abcd', 'someone2@example.com')
        requests = self._d.get_requests()
        assert_true(requests[0]['timestamp'] > requests[1]['timestamp'])
        assert_true(requests[1]['timestamp'] > requests[2]['timestamp'])

    def test_errors_ordered_by_timestamp_desc(self):
        """Check that the returned requests are ordered by timestamp desc"""
        self._d.log_error('abcd', 'someone1@example.com', 'borken')
        time.sleep(1)
        self._d.log_error('abcd', 'someone1@example.com', 'borken')
        time.sleep(1)
        self._d.log_error('abcd', 'someone2@example.com', 'borken')
        errors = self._d.get_errors()
        assert_true(errors[0]['timestamp'] > errors[1]['timestamp'])
        assert_true(errors[1]['timestamp'] > errors[2]['timestamp'])

    def test_statistics_shortcut(self):
        """Check that the 'statistics' shortcut returns an object as expected"""
        o = statistics('sqlite:///:memory:', False)
        assert_equals(CkanPackagerStatistics, type(o))


def test_extract_domain():
    assert_equals(extract_domain(u'someone@nhm.ac.uk'), u'nhm.ac.uk')
    # if no @ is present, just return the whole thing
    assert_equals(extract_domain(u'someone'), u'someone')
    # if more than one @ is present, the "domain" starts at the first one
    assert_equals(extract_domain(u'someone@@nhm.ac.uk'), u'@nhm.ac.uk')
    # if only a @ is present, return empty
    assert_equals(extract_domain(u'@'), u'')
    # if the @ is at the end of the string, return empty
    assert_equals(extract_domain(u'aaa@'), u'')


def test_anonymize_email():
    assert_equals(anonymize_email(u'someone@nhm.ac.uk'), anonymize_email(u'someone@nhm.ac.uk'))
    assert_equals(anonymize_email(u'SOMEONE@nhm.ac.uk'), anonymize_email(u'someone@NHM.ac.uk'))
    assert_not_equals(anonymize_email(u'someone@nhm.ac.uk'),
                      anonymize_email(u'someone_else@nhm.ac.uk'))

    # copes with an empty input
    anonymize_email(u'')

    # we know that the domain is used as the salt so lets check that silly salts don't throw errors

    # much longer than the 22 character salt bcrypt needs
    anonymize_email(u'a@{}'.format(u'x'*40))
    # much shorter than the 22 character salt bcrypt needs
    anonymize_email(u'a@{}'.format(u''))
    anonymize_email(u'a@{}'.format(u'x'))


@patch(u'ckanpackager.lib.statistics.anonymize_email')
def test_anonymize_kwargs(mock_anonymize_email):
    mock_hash = u'hashed!'
    mock_anonymize_email.configure_mock(return_value=mock_hash)

    kwargs = {u'email': u'someone@nhm.ac.uk'}
    anonymize_kwargs(kwargs)
    assert_equals(kwargs[u'email'], u'hashed!')

    kwargs = {}
    anonymize_kwargs(kwargs)
    assert_equals(kwargs, {})

    kwargs = {u'another': u'different_thing', u'email': u'someone@nhm.ac.uk'}
    anonymize_kwargs(kwargs)
    assert_equals(kwargs[u'email'], u'hashed!')
    assert_equals(kwargs[u'another'], u'different_thing')

    kwargs = {u'email': None}
    anonymize_kwargs(kwargs)
    assert_equals(kwargs[u'email'], None)
