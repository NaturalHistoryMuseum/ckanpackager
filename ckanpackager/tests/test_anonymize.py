from mock import MagicMock
from nose.tools import assert_equals, assert_not_equals, assert_in

from ckanpackager.anonymize import Anonymizer


class TestAnonymizer(object):

    def test_hashed_email(self):
        anonymizer = Anonymizer(u'sqlite:///:memory:', True)

        # check it uses the cache
        anonymizer.hash_cache = {u'test@e.com': u'hashed!'}
        assert_equals(anonymizer.get_hashed_email(u'test@e.com'), u'hashed!')

        # check it updates the cache when a new email is hashed
        anonymizer.hash_cache = {u'test@e.com': u'hashed!'}
        hashed_email = anonymizer.get_hashed_email(u'test2@e.com')
        assert_not_equals(hashed_email, u'hashed!')
        assert_in(u'test2@e.com', anonymizer.hash_cache)
        assert_equals(anonymizer.hash_cache[u'test2@e.com'], hashed_email)

    def test_get_requests_rows(self):
        anonymizer = Anonymizer(u'sqlite:///:memory:', True)

        # mock the rows in the database
        mock_rows = [
            {u'id': 1, u'email': u'one@e.com'},
            {u'id': 2, u'email': u'two@e.com'},
            {u'id': 3, u'email': u'three@e.com'},
            # put a row in with capital letters to test they are lowered
            {u'id': 4, u'email': u'TWO@E.com'},
        ]
        anonymizer.database = MagicMock()
        anonymizer.database[u'requests'].all = MagicMock(return_value=mock_rows)

        # setup the cache the way we want it
        anonymizer.hash_cache = {
            u'one@e.com': u'hash1',
            u'two@e.com': u'hash2',
            u'three@e.com': u'hash3',
        }

        rows = anonymizer.get_requests_rows()
        assert_equals(rows[0], {u'id': 1, u'email': u'hash1', u'domain': u'e.com'})
        assert_equals(rows[1], {u'id': 2, u'email': u'hash2', u'domain': u'e.com'})
        assert_equals(rows[2], {u'id': 3, u'email': u'hash3', u'domain': u'e.com'})
        assert_equals(rows[3], {u'id': 4, u'email': u'hash2', u'domain': u'e.com'})

    def test_get_errors_rows(self):
        anonymizer = Anonymizer(u'sqlite:///:memory:', True)

        # mock the rows in the database
        mock_rows = [
            {u'id': 1, u'email': u'one@e.com'},
            {u'id': 2, u'email': u'two@e.com'},
            {u'id': 3, u'email': u'three@e.com'},
            # put a row in with capital letters to test they are lowered
            {u'id': 4, u'email': u'TWO@e.com'},
        ]
        anonymizer.database = MagicMock()
        anonymizer.database[u'errors'].all = MagicMock(return_value=mock_rows)

        # setup the cache the way we want it
        anonymizer.hash_cache = {
            u'one@e.com': u'hash1',
            u'two@e.com': u'hash2',
            u'three@e.com': u'hash3',
        }

        rows = anonymizer.get_errors_rows()
        assert_equals(rows[0], {u'id': 1, u'email': u'hash1'})
        assert_equals(rows[1], {u'id': 2, u'email': u'hash2'})
        assert_equals(rows[2], {u'id': 3, u'email': u'hash3'})
        assert_equals(rows[3], {u'id': 4, u'email': u'hash2'})

    def test_run_dry(self):
        anonymizer = Anonymizer(u'sqlite:///:memory:', True)

        anonymizer.database = MagicMock()
        anonymizer.get_requests_rows = MagicMock(return_value=[1, 2, 3, 4, 5, 6])
        anonymizer.get_errors_rows = MagicMock(return_value=[7, 8, 9, 10])

        anonymizer.run()

        assert_equals(anonymizer.database.call_count, 0)

    def test_run(self):
        anonymizer = Anonymizer(u'sqlite:///:memory:', False)

        # add some data to the database
        anonymizer.database[u'requests'].insert({
            u'count': 432,
            u'email': u'someone@test.com',
            u'resource_id': u'some-resource-id',
            u'timestamp': 12,
        })
        anonymizer.database[u'errors'].insert({
            u'message': u'something went horribly wrong :(',
            u'email': u'someone@test.com',
            u'resource_id': u'some-resource-id',
            u'timestamp': 12,
        })
        # setup the hash cache the way we want it
        anonymizer.hash_cache = {u'someone@test.com': u'hash!'}

        # run the anonymizer
        anonymizer.run()

        # check the requests have all be modified
        requests = list(anonymizer.database[u'requests'].all())
        assert_equals(len(requests), 1)
        assert_equals(requests[0], {
            u'id': 1,
            u'count': 432,
            u'email': u'hash!',
            u'resource_id': u'some-resource-id',
            u'timestamp': 12,
            u'domain': u'test.com',
        })

        # check the errors have all been modified
        errors = list(anonymizer.database[u'errors'].all())
        assert_equals(len(errors), 1)
        assert_equals(errors[0], {
            u'id': 1,
            u'email': u'hash!',
            u'resource_id': u'some-resource-id',
            u'timestamp': 12,
            u'message': u'something went horribly wrong :(',
        })
