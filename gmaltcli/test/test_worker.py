import pytest
import time

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
    def __init__(self, id_, queue_obj, counter, stop_event, processed):
        super(FalseWorker, self).__init__(id_, queue_obj, counter, stop_event)
        processed[id_] = []
        self.processed = processed

    def process(self, queue_item, counter_info):
        self.processed[self.id].append(queue_item)
        time.sleep(0.1)


class ErrorWorker(FalseWorker):
    def process(self, queue_item, counter_info):
        if counter_info[0] == 80:
            raise Exception('Exception on 80th item')


class TestWorkerPool(object):
    def setup_method(self):
        self.processed = {}
        self.pool = worker.WorkerPool(FalseWorker, 5, self.processed)
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
        pool = worker.WorkerPool(ErrorWorker, 5, {})
        pool.fill(['item' + str(item) for item in range(1, 100)])
        with pytest.raises(worker.WorkerPoolException):
            pool.start()
