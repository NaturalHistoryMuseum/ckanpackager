import os
import time
import shlex
import shutil
import hashlib
import tempfile
import unicodecsv
import subprocess
from urlparse import urlparse


class ArchiveError(Exception):
    """Exception raised when we fail to build the ZIP file"""
    pass


class ResourceFile():
    """Represents and builds a ZIP resource file from given request parameters"""
    def __init__(self, request_params, root, temp_dir, cache_time):
        """Create a new Resource File

        @param request_params: Dictionary of parameters defining the request
        @param root: Directory in which ZIPped resource files are stored
        @param temp_dir: Directory in which we can create temporary folder
                         for generating the resource
        @param cache_time: Time (in second) for which a file is valid for the same request
        """
        self.request_params = request_params
        self.temp_dir = temp_dir
        self.root = root
        self.cache_time = cache_time
        self.working_folder = None
        self.zip_file_name = None
        self.writers = {}

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

    def set_zip_file_name(self, zip_file_name):
        """Force-set the zip file name.

        This is mostly usefull for testing
        """
        self.zip_file_name = zip_file_name

    def get_writer(self, name=None):
        """Get a writer for the given file name in the resource.

        If name is not defined, this will:
        - Use the base file name defined by the 'resource_url' request
          parameter if present;
        - Use the resource id if not.

        Note that writers are automatically closed when clean_work_files is
        called.

        @param name: Name of file to create, or None
        """
        self._create_working_folder()
        if name is None:
            if 'resource_url' in self.request_params:
                url = urlparse(self.request_params['resource_url'])
                parts = [p for p in url.path.split('/') if p]
                if len(parts) > 0:
                    name = parts.pop()
            if name is None:
                name = self.request_params['resource_id']
        if name not in self.writers:
            self.writers[name] = open(os.path.join(self.working_folder, name), 'wb')
        return self.writers[name]

    def get_csv_writer(self, name=None):
        """Get a CSV writer for the given file name in the resource.

        If name is not defined, this will:
        - Use the base file name defined by the 'resource_url' request
          parameter if present;
        - Use the resource id if not, with an added .csv extention

        Note that writers are automatically closed when clean_work_files is
        called.

        @param name: Name of file to create, or None
        """
        return unicodecsv.writer(
            self.get_writer(name),
            encoding='utf-8',
            delimiter=',',
            quotechar='"',
            lineterminator="\n"
        )

    def create_zip(self, zip_command):
        """Create the ZIP file from the files added to this resource

        @param zip_command: Shell ZIP command. {input} and {output} are replaced with the relevant file names
        """
        # Ensure we flush all the writers
        for w in self.writers:
            if not self.writers[w].closed:
                self.writers[w].flush()
        #FIXME the task should be given a unique worker id rather than rely on this
        worker_id = os.getpid()
        zip_file_name = "{base}-{pid}-{time}.zip".format(
            base=os.path.join(self.root, self._base_name()),
            pid=worker_id,
            time=int(time.time())
        )
        for resource_file in os.listdir(self.working_folder):
            cmd = shlex.split(zip_command)
            for i, v in enumerate(cmd):
                if v == '{input}':
                    cmd[i] = os.path.join(self.working_folder, resource_file)
                if v == '{output}':
                    cmd[i] = zip_file_name
            # FIXME: Should we implement a timeout?
            ret_code = subprocess.Popen(cmd).wait()
            if ret_code != 0:
                raise ArchiveError("Failed to create ZIP archive")
        self.zip_file_name = zip_file_name

    def clean_work_files(self):
        """Clean up temp files"""
        # Ensure all writers are closed
        for w in self.writers:
            if not self.writers[w].closed:
                self.writers[w].close()
        self.writers = []
        # Remove the temp working folder
        if self.working_folder and os.path.exists(self.working_folder):
            shutil.rmtree(self.working_folder, True)
        self.working_folder = None

    def _create_working_folder(self):
        """Creates a temporary working folder"""
        if self.working_folder is None:
            self.working_folder = tempfile.mkdtemp(
                dir=self.temp_dir
            )

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
