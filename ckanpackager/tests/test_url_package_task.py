"""Test the UrlPackageClass class"""
import StringIO
import httpretty
from nose.tools import assert_equals, assert_true, assert_raises, assert_in
from ckanpackager.tasks.url_package_task import UrlPackageTask
from ckanpackager.lib.utils import BadRequestError


class DummyResource(object):
    """Fake Resourcefile object """
    def __init__(self):
        self.stream = StringIO.StringIO()
        self.create_invoked = False
        self.clean_invoked = False

    def get_writer(self):
        return self.stream

    def create_zip(self, command):
        self.create_invoked = True

    def clean_work_files(self):
        self.clean_invoked = True


class TestUrlPackageTask(object):
    """Test the UrlPackageTask task."""
    def setUp(self):
        """Setup up test config & task"""
        self._config = {
            'ZIP_COMMAND': "/usr/bin/zip -j {output} {input}",
        }
        self._task = UrlPackageTask({
            'resource_id': 'the-resource-id',
            'resource_url': 'http://example.com/the/resource/url.txt',
            'email': 'someone@0.0.0.0'
        }, self._config)

    def _register_uri(self):
        """Register httpretty URI"""
        httpretty.register_uri(
            httpretty.GET,
            'http://example.com/the/resource/url.txt',
            'the file content'
        )

    def test_required_parameters(self):
        """Ensure resource_url is required parameter"""
        with assert_raises(BadRequestError) as context:
            p = UrlPackageTask({'resource_id': 'a', 'email': 'a'}, {})

    def test_host(self):
        """Test the host is taken from the resource_url"""
        assert_equals(self._task.host(), 'example.com')

    @httpretty.activate
    def test_task_fetches_file(self):
        """Ensure running the test fetches the file at the given URL"""
        self._register_uri()
        self._task.create_zip(DummyResource())
        assert_equals(httpretty.last_request().path, '/the/resource/url.txt')

    @httpretty.activate
    def test_resource_gets_url_content(self):
        """Ensure that the content at the URL is saved to the resource file"""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(r.stream.getvalue(), 'the file content')

    @httpretty.activate
    def test_zip_created(self):
        """Ensure that the resource ZIP file is created"""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_true(r.create_invoked)

    @httpretty.activate
    def test_work_folder_cleaned(self):
        """Ensure that the work folder is cleaned"""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_true(r.clean_invoked)

    @httpretty.activate
    def test_request_authorization(self):
        """Ensure an authorization header is added"""
        apikey = 'some_api_key'
        task = UrlPackageTask({
            'resource_id': 'the-resource-id',
            'resource_url': 'http://example.com/the/resource/url.txt',
            'email': 'someone@0.0.0.0',
            'key': apikey
        }, self._config)
        r = DummyResource()
        task.create_zip(r)
        assert_true(r.clean_invoked)
        headers = dict(httpretty.last_request().headers)
        assert_equals(headers['authorization'], apikey)
