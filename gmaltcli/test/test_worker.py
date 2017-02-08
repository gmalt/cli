import os

import logging
import pytest
import time
import threading

try:
    # Python 3
    import queue
    from urllib.error import HTTPError, URLError
except ImportError:
    # Python 2
    from urllib2 import urlopen, HTTPError, URLError
    import Queue as queue

import gmaltcli.worker as worker


class TestSafeCounter(object):
    def test_increment_one(self):
        counter = worker.SafeCounter()
        counter.max = 100

        return_value = counter.increment()
        assert return_value[0] == 1
        assert return_value[1] == 100

        return_value = counter.increment()
        assert return_value[0] == 2
        assert return_value[1] == 100

    def test_increment_five_start_ten(self):
        counter = worker.SafeCounter(start=10, incr=5)
        counter.max = 100

        return_value = counter.increment()
        assert return_value[0] == 15
        assert return_value[1] == 100

        return_value = counter.increment()
        assert return_value[0] == 20
        assert return_value[1] == 100

    def test_get(self):
        counter = worker.SafeCounter()
        counter.max = 100

        counter.increment()
        counter.increment()
        counter.increment()

        assert counter.get() == 3


class FalseWorker(worker.Worker):
    def __init__(self, id_, queue_obj, counter, stop_event, processed, sleep=0):
        super(FalseWorker, self).__init__(id_, queue_obj, counter, stop_event)
        processed[id_] = []
        self.processed = processed
        self.sleep = sleep

    def process(self, queue_item, counter_info):
        self.processed[self.id].append(queue_item)
        if self.sleep:
            time.sleep(self.sleep)

    def _on_end(self):
        setattr(self, 'on_end_called', True)


class ErrorWorker(FalseWorker):
    def process(self, queue_item, counter_info):
        if queue_item == 80 or counter_info[0] == 80:
            raise Exception(
                'Exception on 80th item')
        else:
            super(ErrorWorker, self).process(queue_item, counter_info)


class TestWorkerPool(object):
    def setup_method(self, func_method):
        self.processed = {}
        self.pool = worker.WorkerPool(FalseWorker, 5, self.processed, sleep=0.1)
        self.pool.fill(['item' + str(item) for item in range(1, 100)])

    def test__init__(self):
        assert len(self.pool.workers) == 5
        assert all([isinstance(item, FalseWorker) for item in self.pool.workers])
        assert all([id(self.processed) == id(item.processed) for item in self.pool.workers])

    def test_fill(self):
        assert self.pool.queue.qsize() == 99
        assert self.pool.counter.max == 99

    def test_start(self):
        self.pool.start()

        assert len(self.processed[1] + self.processed[2] + self.processed[3] +
                   self.processed[4] + self.processed[5]) == 99
        assert all([len(self.processed[key]) > 10 for key in self.processed])

    def test_start_and_error(self):
        pool = worker.WorkerPool(ErrorWorker, 5, {}, sleep=0.1)
        pool.fill(['item' + str(item) for item in range(1, 100)])
        with pytest.raises(worker.WorkerPoolException):
            pool.start()


class TestWorker(object):
    def setup_method(self, func_method):
        test_stop_event = threading.Event()
        error_stop_event = threading.Event()

        test_counter = worker.SafeCounter()
        error_counter = worker.SafeCounter()

        test_worker_queue = queue.Queue()
        error_worker_queue = queue.Queue()
        for item in range(1, 100):
            test_worker_queue.put(item)
            error_worker_queue.put(item)

        self.processed = {}

        self.test_worker = FalseWorker(1, test_worker_queue,
                                       test_counter, test_stop_event,
                                       self.processed)
        self.error_worker = ErrorWorker(2, error_worker_queue,
                                        error_counter, error_stop_event,
                                        self.processed)

    def test_run(self):
        self.test_worker.run()

        assert len(self.processed[1]) == 99
        assert hasattr(self.test_worker, 'on_end_called')
        assert self.test_worker.queue.empty()
        assert not self.test_worker.stop_event.isSet()

        self.error_worker.run()

        assert len(self.processed[2]) == 79
        assert hasattr(self.error_worker, 'on_end_called')
        assert self.error_worker.queue.qsize() == 19
        assert self.error_worker.stop_event.isSet()

    def test__get_queue(self):
        self.test_worker._get_queue()

        assert len(self.processed[1]) == 1
        assert not hasattr(self.test_worker, 'on_end_called')
        assert self.test_worker.queue.qsize() == 98
        assert not self.test_worker.stop_event.isSet()

        for i in range(1, 80):
            self.error_worker._get_queue()

        assert len(self.processed[2]) == 79
        assert not hasattr(self.error_worker, 'on_end_called')
        assert self.error_worker.queue.qsize() == 20
        assert not self.error_worker.stop_event.isSet()

        # 80th event
        self.error_worker._get_queue()

        assert len(self.processed[2]) == 79
        assert not hasattr(self.error_worker, 'on_end_called')
        assert self.error_worker.queue.qsize() == 19
        assert self.error_worker.stop_event.isSet()


class TestDownloadWorker(object):
    def setup_method(self, func_method):
        stop_event = threading.Event()
        counter = worker.SafeCounter()
        worker_queue = queue.Queue()
        folder = None
        self.download_worker = worker.DownloadWorker(1, worker_queue, counter,
                                                     stop_event, folder)

    def test__secured_download_file_connection_error(self, monkeypatch):
        def raise_url_error(url, filename):
            raise URLError('message')
        monkeypatch.setattr(self.download_worker, '_download_file', raise_url_error)
        monkeypatch.setattr(logging, 'error', lambda x: x)
        with pytest.raises(URLError):
            self.download_worker._secured_download_file('url', 'filename')

    def test__secured_download_file_wrong_url(self, monkeypatch):
        def raise_url_error(url, filename):
            raise HTTPError('message', None, None, None, None)
        monkeypatch.setattr(self.download_worker, '_download_file', raise_url_error)
        monkeypatch.setattr(logging, 'error', lambda x: x)
        with pytest.raises(HTTPError):
            self.download_worker._secured_download_file('url', 'filename')

    def test__download_file(self, tmpdir):
        tmp_folder = str(tmpdir.mkdir('gmaltcli'))
        self.download_worker.folder = tmp_folder

        self.download_worker._download_file(
            'http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/Africa/N00E010.hgt.zip',
            'N00E010.hgt.zip')

        downloaded_path = os.path.join(self.download_worker.folder, 'N00E010.hgt.zip')
        assert os.path.exists(downloaded_path)
        assert os.path.getsize(downloaded_path) == 1743694


class TestExtractWorker(object):
    def setup_method(self, func_method):
        stop_event = threading.Event()
        counter = worker.SafeCounter()
        worker_queue = queue.Queue()
        folder = None
        self.extract_worker = worker.ExtractWorker(1, worker_queue, counter,
                                                   stop_event, folder)

    def test__extract_file(self, tmpdir):
        tmp_folder = str(tmpdir.mkdir('gmaltcli'))
        self.extract_worker.folder = tmp_folder
        zip_file = os.path.realpath(os.path.join(os.path.dirname(__file__), 'N00E010.hgt.zip'))

        self.extract_worker._extract_file(zip_file)

        extracted_file = os.path.join(tmp_folder, 'N00E010.hgt')
        assert os.path.exists(extracted_file)
        assert os.path.getsize(extracted_file) == 2884802
