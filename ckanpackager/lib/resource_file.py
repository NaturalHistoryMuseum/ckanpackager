import os
import re
import time
import shlex
import hashlib
import tempfile
import unicodecsv
import subprocess
from urlparse import urlparse
from contextlib import contextmanager


class ArchiveError(Exception):
    """Exception raised when we fail to build the ZIP file"""
    pass


class ResourceFile():
    """Represents and builds a ZIP resource file from given request parameters"""
    def __init__(self, request_params, root, cache_time):
        """Create a new Resource File

        @param request_params: Dictionary of parameters defining the request
        @param root: Directory in which ZIPped resource files are store
        @param cache_time: Time (in second) for which a file is valid for the same request
        """
        self.request_params = request_params
        self.zip_file_name = None
        self.temp_file_name = None
        self.root = root
        self.cache_time = cache_time

    def zip_file_exists(self):
        """Check if the file already exists"""
        if self.zip_file_name:
            return True
        zip_file_name = self._get_cached_zip_file()
        if zip_file_name:
            self.zip_file_name = zip_file_name
            return True

    def get_zip_file_name(self):
        """Return the file name of ZIP the file, or None.

        If zip_file_exists returns True, then this will always return a file name.
        """
        return self.zip_file_name

    @contextmanager
    def get_writer(self, work_directory):
        """Yield a writer for the current request

        Create a new temporary file for this request, and yield a writer object for it.
        Once this has been called, get_file_name will return the name of the created file.

        The writer is closed on exit. If the inner block raised an exception, then the file will be
        deleted too. Otherwise it will be left in place.

        @param work_directory: folder in which to create the file
        """
        suffix = None
        prefix = None
        if 'resource_url' in self.request_params:
            url = urlparse(self.request_params['resource_url'])
            parts = [p for p in url.path.split('/') if p]
            if len(parts) > 0:
                filename = parts.pop()
                suffix = re.sub('^[^.]*', '', filename)
                prefix = re.sub('\..*$', '', filename) + '-'
        if suffix is None:
            suffix = ''
            prefix = self.request_params['resource_id'] + '-'

        temp_file = tempfile.NamedTemporaryFile(
            mode='wb',
            suffix=suffix,
            prefix=prefix,
            dir=work_directory,
            delete=False
        )
        self.temp_file_name = temp_file.name
        try:
            yield temp_file
        except:
            if not temp_file.closed:
                temp_file.close()
            raise

    @contextmanager
    def get_csv_writer(self, work_directory):
        """Yield a CSV writer for the current request

        Create a new CSV file for this request, and yield a CSV writer for it.
        Once this has been called, get_file_name will return the name of the
        created file.

        The CSV writer is closed on exit. If the inner block raised an exception, then the CSV file will be
        deleted too. Otherwise it will be left in place.

        @param work_directory: folder in which to create the file
        """
        csv_file = tempfile.NamedTemporaryFile(
            mode='wb',
            suffix='.csv',
            prefix=self.request_params['resource_id'] + '-',
            dir=work_directory,
            delete=False
        )
        self.temp_file_name = csv_file.name
        try:
            output_stream = unicodecsv.writer(
                csv_file,
                encoding='utf-8',
                delimiter=',',
                quotechar='"',
                lineterminator="\n"
            )
            yield output_stream
        except:
            if not csv_file.closed:
                csv_file.close()
            raise

    def create_zip(self, zip_command):
        """Create the ZIP file from the resource file created using get_csv_writer

        @param zip_command: Shell ZIP command. {input} and {output} are replaced with the relevant file names
        """
        #FIXME the task should be given a unique worker id rather than rely on this
        worker_id = os.getpid()
        zip_file_name = "{base}-{pid}-{time}.zip".format(
            base=os.path.join(self.root, self._base_name()),
            pid=worker_id,
            time=int(time.time())
        )
        cmd = shlex.split(zip_command)
        for i, v in enumerate(cmd):
            if v == '{input}':
                cmd[i] = self.temp_file_name
            if v == '{output}':
                cmd[i] = zip_file_name
        # FIXME: Should we implement a timeout?
        ret_code = subprocess.Popen(cmd).wait()
        if ret_code != 0:
            raise ArchiveError("Failed to create ZIP archive")
        self.zip_file_name = zip_file_name

    def clean_work_files(self):
        """Clean up temp files"""
        if self.temp_file_name and os.path.exists(self.temp_file_name):
            os.remove(self.temp_file_name)

    def _get_cached_zip_file(self):
        """Find cached file for this resource

        @return: The full file name, or None
        """
        zip_base = self._base_name()
        for base_name in os.listdir(self.root):
            if base_name.startswith(zip_base):
                file_name = os.path.join(self.root, base_name)
                mtime = os.path.getmtime(file_name)
                if (time.time() - mtime) < self.cache_time:
                    return file_name
        return None

    def _base_name(self):
        """Return the base name for the ZIP file"""
        md5 = hashlib.md5()
        cache_key_params = dict(self.request_params)
        if 'email' in cache_key_params:
            del cache_key_params['email']
        md5.update(str(cache_key_params))
        return md5.hexdigest()
