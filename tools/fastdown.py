import ftplib
import re
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, Future

import requests


class AtomTask:

    def __init__(self, url, filename, chunk_size=None, offset=None, callback_args=None):
        self.url = url
        self.filename = filename
        self.chunk_size = chunk_size
        self.offset = offset
        self.tries = 0
        self.callback_args = callback_args

    def __str__(self):
        return self.filename


class BaseDownloader:

    def __init__(self, chunk_parallel=1, chunk_size=8192, retry=5, timeout=3):
        self.chunk_parallel = chunk_parallel
        self.chunk_size = chunk_size
        self.retry = retry
        self.timeout = timeout
        self.executor = None
        self.basedir = ''
        self.success_callback = None
        self.failed_callback = None

    def set_executor(self, executor):
        self.executor = executor

    def set_basedir(self, basedir):
        self.basedir = basedir

    def __call__(self, task):
        pass

    def failed(self, task):
        task.tries += 1
        if self.failed_callback:
            self.failed_callback(task)
        if task.tries < self.retry:
            return self.executor.submit(self, task)
        else:
            raise IOError('Maximum tries exceed! Task: {}'.format(task))


class RequestsDownloader(BaseDownloader):

    def __call__(self, task):
        return self.single(task)

    def get_filename_from_header(self, header):
        disposition = header.get('content-disposition')
        if not disposition:
            raise ValueError('Cannot fetch filename!')
        filenames = re.findall('filename=(.+)', disposition)
        if len(filenames) == 0:
            raise ValueError('Cannot fetch filename!')
        return filenames[0]

    def single(self, task):
        if task.chunk_size is None:
            return self.single_no_chunk(task)
        else:
            return self.single_with_chunk(task)

    def single_no_chunk(self, task):
        try:
            res = requests.get(task.url, allow_redirects=True, timeout=self.timeout)
        except (requests.exceptions.Timeout, requests.exceptions.RequestException):
            return self.failed(task)
        if task.filename is None:
            task.filename = self.get_filename_from_header(res.headers)
        if self.success_callback:
            self.success_callback(task)
        open(os.path.join(self.basedir, task.filename), 'wb').write(res.content)

    def single_with_chunk(self, task):
        with requests.get(task.url, allow_redirects=True,
                timeout=self.timeout, stream=True) as res:
            res.raise_for_status()
            if task.filename is None:
                task.filename = self.get_filename_from_header(res.headers)
            with open(os.path.join(self.basedir, task.filename), 'wb') as f:
                for chunk in res.iter_content(chunk_size=task.chunk_size):
                    if chunk:
                        shutil.copyfileobj(res.raw, f)
        if self.success_callback:
            self.success_callback(task)


class FastDown:

    default_service = RequestsDownloader

    def __init__(self, file_parallel=1, chunk_parallel=1, chunk_size=None, retry=5, timeout=3):
        self.file_parallel = file_parallel
        self.chunk_parallel = chunk_parallel
        self.chunk_size = chunk_size
        self.retry = retry
        self.timeout = timeout
        self.service = self.default_service
        self.basedir = ''
        self.success_callback = None
        self.failed_callback = None

    def use_downloader(self, service):
        self.service = service

    def set_basedir(self, basedir):
        self.basedir = basedir

    def set_success_callback(self, callback):
        self.success_callback = callback

    def set_failed_callback(self, callback):
        self.failed_callback = callback

    def set_task(self, task):
        """Task should be arranged like list of (url, filename) tuples."""
        self.targets = []
        if isinstance(task, tuple):
            self.targets.extend(self.create_atom_task(*task))
        elif isinstance(task, str):
            self.targets.extend(self.create_atom_task(task, None))
        else:
            try:
                for t in task:
                    if isinstance(t, tuple):
                        if len(t) == 2:
                            url, filename = t
                            callback_args = None
                        elif len(t) == 3:
                            url, filename, callback_args = t
                    else:
                        url, filename, callback_args = t, None, None
                    self.targets.extend(self.create_atom_task(url, filename, callback_args))
            except TypeError:
                raise ValueError('task must be length-2 tuple or iterable of length-2 tuples.')

    def create_atom_task(self, url, filename, callback_args=None):
        if self.chunk_parallel == 1:
            return [AtomTask(url, filename, callback_args=callback_args)]
        return [AtomTask(url, filename, self.chunk_size, self.chunk_size * i,
            callback_args=callback_args) for i in range(self.chunk_parallel)]

    def download(self):
        downloader = self.service(self.chunk_parallel, self.chunk_size, self.retry, self.timeout)
        downloader.set_basedir(self.basedir)
        if self.success_callback:
            downloader.success_callback = self.success_callback
        if self.failed_callback:
            downloader.failed_callback = self.failed_callback
        with ThreadPoolExecutor(max_workers=self.file_parallel) as executor:
            downloader.set_executor(executor)
            futures = executor.map(downloader, self.targets)
            self.targets = []
            for future in futures:
                while isinstance(future, Future):
                    future = future.result()


class FTPDownloader(BaseDownloader):

    def __call__(self, task):
        if task.chunk_size is None:
            return self.single(task)
        else:
            raise NotImplementedError('Chunk download feature hasn\'t been implemented, please be patient!')

    def set_ftp_handler(self, ftp):
        self.ftp = ftp

    def single(self, task):
        try:
            filename = os.path.join(self.basedir, task.filename)
            # BUGFIX: If we failed once, an empty file will be created.
            # So we need to check the filesize too!
            if not os.path.exists(filename) or os.path.getsize(filename) == 0:
                self.ftp.retrbinary('RETR {}'.format(task.url), open(filename, 'wb').write)
        except Exception as err:
            return self.failed(task)
        if self.success_callback:
            self.success_callback(filename, task.callback_args)


class FTPFastDown(FastDown):

    default_service = FTPDownloader

    def set_ftp(self, ftp):
        self.ftp = ftp

    def download(self):
        downloader = self.service(self.chunk_parallel, self.chunk_size, self.retry, self.timeout)
        downloader.set_basedir(self.basedir)
        downloader.set_ftp_handler(self.ftp)
        if self.success_callback:
            downloader.success_callback = self.success_callback
        if self.failed_callback:
            downloader.failed_callback = self.failed_callback
        with ThreadPoolExecutor(max_workers=self.file_parallel) as executor:
            downloader.set_executor(executor)
            futures = executor.map(downloader, self.targets)
            self.targets = []
            for future in futures:
                while isinstance(future, Future):
                    future = future.result()


class SerialExecutor:

    def __init__(self, targets):
        self.targets = targets

    def submit(self, downloader, target):
        self.targets.append(target)


class SerialFTPFastDown(FTPFastDown):

    def download(self):
        downloader = self.service(self.chunk_parallel, self.chunk_size, self.retry, self.timeout)
        downloader.set_basedir(self.basedir)
        downloader.set_ftp_handler(self.ftp)
        if self.success_callback:
            downloader.success_callback = self.success_callback
        if self.failed_callback:
            downloader.failed_callback = self.failed_callback
        downloader.set_executor(SerialExecutor(self.targets))
        for target in self.targets:
            downloader(target)
        self.targets = []

