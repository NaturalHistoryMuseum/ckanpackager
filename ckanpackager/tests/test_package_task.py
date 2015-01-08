"""Test the PackageTask class"""

import os
import tempfile
import shutil
import time
from nose.tools import assert_equals, assert_raises, assert_not_equals
from nose.tools import assert_not_in, assert_in
from ckanpackager.tasks.package_task import PackageTask
from ckanpackager.lib.utils import BadRequestError
from ckanpackager.lib.statistics import CkanPackagerStatistics
from ckanpackager.tests import smtpretty

class DummyPackageTask(PackageTask):
    """The PackageTask class has some abstract methods, this is a minimal
       implementation.
    """
    def __init__(self, *args, **kargs):
        super(DummyPackageTask, self).__init__(*args, **kargs)
        self._create_zip_invoked = False

    def schema(self):
        return {
            'carrot': (True, None),
            'cake': (False, self._process_cake)
        }

    def create_zip(self, resource):
        if self.request_params['carrot'] == 'break':
            raise Exception('this is broken')
        resource.set_zip_file_name('the-zip-file.zip')

    def host(self):
        return 'example.com'

    def _process_cake(self, cake):
        return 'nice '+ str(cake)


class TestPackageTask(object):
    """Test the DummyPackageTask task."""
    def setUp(self):
        """Setup up test config&folders"""
        self._temp_db_folder = tempfile.mkdtemp()
        self._config = {
            'STORE_DIRECTORY': tempfile.mkdtemp(),
            'TEMP_DIRECTORY': tempfile.mkdtemp(),
            'STATS_DB': 'sqlite:///' + os.path.join(self._temp_db_folder, 'db'),
            'CACHE_TIME': 60*60*24,
            'EMAIL_FROM': '{resource_id}-{zip_file_name}-{ckan_host}-from',
            'EMAIL_BODY': '{resource_id};{zip_file_name};{ckan_host} body',
            'EMAIL_SUBJECT': '{resource_id};{zip_file_name};{ckan_host} subject',
            'SMTP_HOST': '127.0.0.1',
            'SMTP_PORT': 2525
        }

    def tearDown(self):
        """Remove temp folders"""
        shutil.rmtree(self._temp_db_folder)
        shutil.rmtree(self._config['STORE_DIRECTORY'])
        shutil.rmtree(self._config['TEMP_DIRECTORY'])

    def test_config_is_available_to_subclasses(self):
        """Test that the config is available to sub-classes"""
        p = DummyPackageTask(
            {'carrot': 'a', 'email': 'b', 'resource_id':'c'},
            {'hello': 'world'}
        )
        assert_equals(p.config['hello'], 'world')

    def test_missing_required_parameters_raises(self):
        """Test an exception is raised if a required parameters is missing.
        """
        with assert_raises(BadRequestError) as context:
            p = DummyPackageTask(
                {'email': 'a', 'resource_id': 'a', 'cake': 'a'},
                {}
            )

    def test_schema_parameters_accepted(self):
        """Ensure that parameters defined in the schema are accepted and added
           to the request parameter
        """
        p = DummyPackageTask(
            {'carrot': 'a', 'email': 'b', 'resource_id':'c'},
            {'hello': 'world'}
        )
        assert_equals(p.request_params['carrot'], 'a')

    def test_unknown_parameters_ignored(self):
        """Ensure that parameters not defined in the schema are ignored"""
        p = DummyPackageTask(
            {'carrot': 'a', 'email': 'b', 'resource_id':'c', 'john': 'doe'},
            {'hello': 'world'}
        )
        assert_not_in('john', p.request_params)

    def test_parameter_process_function_invoked(self):
        """Check that the defined process functions are called"""
        p = DummyPackageTask(
            {'carrot': 'a', 'email': 'b', 'resource_id':'c',
             'cake': 'and sweet'},
            {'hello': 'world'}
        )
        assert_equals(p.request_params['cake'], 'nice and sweet')

    def test_email_and_resource_id_added_to_schema(self):
        """Test that email and resource id are added to schema, by ensuring
           they are required parameters and that parameters are passed through
        """
        with assert_raises(BadRequestError) as context:
            p = DummyPackageTask({'resource_id': 'a', 'carrot': 'a'}, {})
        with assert_raises(BadRequestError) as context:
            p = DummyPackageTask({'email': 'a', 'carrot': 'a'}, {})
        p = DummyPackageTask({'email': 'a', 'resource_id': 'b', 'carrot': 'c'}, {})
        assert_equals(p.request_params['email'], 'a')
        assert_equals(p.request_params['resource_id'], 'b')

    def test_same_task_has_same_str_rep(self):
        """Checks that the same task always returns the same str"""
        p = DummyPackageTask(
            {'carrot': 'a', 'email': 'b', 'resource_id':'c'},
            {'hello': 'world'}
        )
        assert_equals(str(p), str(p))

    def test_different_tasks_have_different_str_rep(self):
        """Checks that two different tasks have two different str"""
        p1 = DummyPackageTask(
            {'carrot': 'a', 'email': 'b', 'resource_id':'c'},
            {'hello': 'world'}
        )
        p2 = DummyPackageTask(
            {'carrot': 'a1', 'email': 'b1', 'resource_id':'c1'},
            {'hello': 'world1'}
        )
        assert_not_equals(str(p1), str(p2))

    def test_same_task_at_different_time_have_different_str_rep(self):
        """Checks that two different tasks have two different str"""
        p1 = DummyPackageTask(
            {'carrot': 'a', 'email': 'b', 'resource_id':'c'},
            {'hello': 'world'}
        )
        time.sleep(1)
        p2 = DummyPackageTask(
            {'carrot': 'a', 'email': 'b', 'resource_id':'c'},
            {'hello': 'world'}
        )
        assert_not_equals(str(p1), str(p2))

    @smtpretty.activate(2525)
    def test_email_sent(self):
        """Test that the email is sent as expected"""
        t = DummyPackageTask({
            'resource_id': 'the-resource-id',
            'email': 'recipient@example.com',
            'carrot': 'cake'
        }, self._config)
        t.run()
        assert_equals(len(smtpretty.messages), 1)
        assert_equals(smtpretty.last_message.recipients, ['recipient@example.com'])

    @smtpretty.activate(2525)
    def test_email_placeholders(self):
        """Test that the email placeholders are inserted"""
        t = DummyPackageTask({
            'resource_id': 'the-resource-id',
            'email': 'recipient@example.com',
            'carrot': 'cake'
        }, self._config)
        t.run()
        assert_equals(len(smtpretty.messages), 1)
        assert_equals(
            smtpretty.last_message.mail_from,
            'the-resource-id-the-zip-file.zip-example.com-from'
        )
        assert_equals(
            smtpretty.last_message.headers['subject'],
            "the-resource-id;the-zip-file.zip;example.com subject",
        )
        assert_equals(
            "the-resource-id;the-zip-file.zip;example.com body",
            smtpretty.last_message.body,
        )

    @smtpretty.activate(2525)
    def test_request_is_logged(self):
        t = DummyPackageTask({
            'resource_id': 'the-resource-id',
            'email': 'recipient@example.com',
            'carrot': 'cake'
        }, self._config)
        t.run()
        stats = CkanPackagerStatistics(self._config['STATS_DB'])
        requests = stats.get_requests()
        assert_equals(1, len(requests))
        assert_equals('the-resource-id', requests[0]['resource_id'])
        assert_equals('recipient@example.com', requests[0]['email'])

    @smtpretty.activate(2525)
    def test_error_is_logged(self):
        t = DummyPackageTask({
            'resource_id': 'the-resource-id',
            'email': 'recipient@example.com',
            'carrot': 'break'
        }, self._config)
        with assert_raises(Exception) as context:
            t.run()
        stats = CkanPackagerStatistics(self._config['STATS_DB'])
        errors = stats.get_errors()
        assert_equals(1, len(errors))
        assert_equals('the-resource-id', errors[0]['resource_id'])
        assert_equals('recipient@example.com', errors[0]['email'])
        assert_in('this is broken', errors[0]['message'])
