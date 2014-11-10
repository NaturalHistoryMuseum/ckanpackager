import os
import time
import tempfile
import shutil
import subprocess
from nose.tools import assert_true, assert_false, assert_equals, assert_in
from nose.tools import assert_not_in
from ckanpackager.lib.resource_file import ResourceFile, ArchiveError


class TestResourcefile(object):
    def setUp(self):
        """Create a temporary folders to work in"""
        self._zip = "/usr/bin/zip -j {output} {input}"
        self._tempdir = tempfile.mkdtemp()
        self._root = tempfile.mkdtemp()

    def tearDown(self):
        """Remove the temporary folders"""
        shutil.rmtree(self._tempdir)
        shutil.rmtree(self._root)

    def test_zip_file_created(self):
        """Create a simple resource with one file, and make sure the zip file
           is created as expected.

           This uses get_writer, but doesn't test if the written file is in the
           archive, let alone what it's content may be
        """
        # Create the resource
        req = {'resource_id': '123'}
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w = resource.get_writer()
        w.write('hello world')
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Test the zip archive exists as expected
        assert_true(resource.zip_file_exists())
        assert_true(os.path.exists(resource.get_zip_file_name()))
        base, ext = os.path.splitext(resource.get_zip_file_name())
        assert_equals('.zip', ext)
        res = subprocess.call(['unzip', '-qq', '-t', resource.get_zip_file_name()])
        assert_equals(0, res)

    def test_find_cached_zip(self):
        """Check that a cached zip is found within the given time"""
        # Create a resource
        req = {'resource_id': '123', 'hello': 'world'}
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w = resource.get_writer()
        w.write('hello world')
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Now look for it again
        resource2 = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        assert_true(resource2.zip_file_exists())

    def test_cached_zip_timeout(self):
        """Ensure that a cached zip file is not found if it has timed out"""
        # Create a resource
        req = {'resource_id': '123', 'hello': 'world'}
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w = resource.get_writer()
        w.write('hello world')
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Sleep, and look for it again
        time.sleep(2)
        resource2 = ResourceFile(req, self._root, self._tempdir, 1)
        assert_false(resource2.zip_file_exists())

    def test_not_cached_zip(self):
        """Ensure that a different request doesn't match a cached zip"""
        # Create a resource
        req = {'resource_id': '123', 'hello': 'world'}
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w = resource.get_writer()
        w.write('hello world')
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Now look for it again
        req2 = {'resource_id': '123', 'hello': 'world!'}
        resource2 = ResourceFile(req2, self._root, self._tempdir, 60*60*24)
        assert_false(resource2.zip_file_exists())

    def test_zip_file_contains_files(self):
        """Test that the zip files contains expected files

        Note that this uses the writer and csv writer, but only tests
        if the files exists - not their content.
        """
        # Create the resource
        req = {'resource_id': '123'}
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w1 = resource.get_writer('one.txt')
        w1.write('hello world')
        w2 = resource.get_writer('two.txt')
        w2.write('hello again')
        w3 = resource.get_csv_writer('one.csv')
        w3.writerow(['hello', 'world'])
        w4 = resource.get_csv_writer('two.csv')
        w4.writerow(['hello', 'world'])
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Test the zip archive contains the expected files
        p = subprocess.Popen(
            ['unzip', '-l', resource.get_zip_file_name()],
            stdout=subprocess.PIPE
            )
        out = " ".join(p.stdout.readlines())
        assert_in('one.txt', out)
        assert_in('two.txt', out)
        assert_in('one.csv', out)
        assert_in('two.csv', out)

    def test_request_id_default_file_name(self):
        """Test that default file names based on request_id work"""
        # Create a resource
        req = {'resource_id': 'abc123efgwow', 'hello': 'world'}
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w = resource.get_writer()
        w.write('hello world')
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Sleep, and look for it again
        p = subprocess.Popen(
            ['unzip', '-l', resource.get_zip_file_name()],
            stdout=subprocess.PIPE
            )
        out = " ".join(p.stdout.readlines())
        assert_in('abc123efgwow', out)

    def test_request_url_default_file_name(self):
        """Test that default file names based on request_id work"""
        # Create a resource
        req = {
            'resource_id': '1',
            'resource_url': 'http://somewhere.com/hellow/world.png?one=two'
        }
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w = resource.get_writer()
        w.write('hello world')
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Sleep, and look for it again
        p = subprocess.Popen(
            ['unzip', '-l', resource.get_zip_file_name()],
            stdout=subprocess.PIPE
            )
        out = " ".join(p.stdout.readlines())
        assert_in('world.png', out)
        assert_not_in('world.png?one=two', out)

    def test_writer(self):
        """Test that writen data is correct"""
        # Create a resource
        req = {'resource_id': '123', 'hello': 'world'}
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w = resource.get_writer('one.txt')
        w.write('hello world')
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Now check it's content
        p = subprocess.Popen(
            ['unzip', '-p', resource.get_zip_file_name()],
            stdout=subprocess.PIPE
        )
        assert_equals('hello world', "".join(p.stdout.readlines()))

    def test_csv_writer(self):
        """Test that csv data is correct"""
        # Create a resource
        req = {'resource_id': '123', 'hello': 'world'}
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w = resource.get_csv_writer('one.csv')
        w.writerows([['one', 'two'],['hello', 'world']])
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Now check it's content
        p = subprocess.Popen(
            ['unzip', '-p', resource.get_zip_file_name()],
            stdout=subprocess.PIPE
        )
        assert_equals(
            "one,two\nhello,world",
            ''.join(p.stdout.readlines()).strip()
        )

    def test_clean_up(self):
        """Ensure that work files are cleaned up"""
        # Create a resource
        req = {'resource_id': '123', 'hello': 'world'}
        resource = ResourceFile(req, self._root, self._tempdir, 60*60*24)
        w = resource.get_writer()
        w.write('hello world')
        resource.create_zip(self._zip)
        resource.clean_work_files()
        # Ensure writers are closed
        assert_true(w.closed)
        # Ensure work folder has been removed
        assert_equals([], os.listdir(self._tempdir))

