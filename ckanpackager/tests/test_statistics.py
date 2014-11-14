import time
from nose.tools import assert_equals, assert_true, assert_not_in
from ckanpackager.lib.statistics import CkanPackagerStatistics, statistics

class TestStatistics(object):
    def setUp(self):
        """Create a statistics object"""
        self._d = CkanPackagerStatistics('sqlite:///:memory:')

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
        o = statistics('sqlite:///:memory:')
        assert_equals(CkanPackagerStatistics, type(o))
