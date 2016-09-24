import tempfile
import shutil
import logging
import threading
import os
import time
import importlib
import json
try:
    import queue
    from urllib.request import urlopen
    from urllib.error import HTTPError
except ImportError:
    import Queue as queue
    from urllib2 import urlopen, HTTPError


class SafeCounter(object):
    """ A counter thread-safe.

    .. note:: set max then call `increment` inside a thread
    """
    def __init__(self, start=0, max_=0, incr=1):
        self.counter = start
        self.max = max_
        self.incr = incr
        self.lock = threading.RLock()

    def increment(self):
        """ Increment the counter """
        with self.lock:
            self.counter += self.incr

    def get(self):
        """ Get the counter current value """
        return self.counter

    def __str__(self):
        return '%d/%d' % (self.counter, self.max)


class ThreadPoolException(Exception):
    """ Exception raised by :class:`hgt2sql.app.ThreadPool` when one of its
    thread has raised an exception
    """
    pass

class ThreadPool(object):
    """ Create a pool of Thread which subscribe to a queue and process its item

    .. note:: the constructor other args and kwargs are passed as additionnal
        params to the worker __init__ method

    :param class worker: The class of the Worker thread
    :type worker: TODO
    :param int size: number of worker to create in pool
    """
    def __init__(self, worker, size, *args, **kwargs):
        self.queue = queue.Queue()
        self.counter = SafeCounter()
        self.stop_event = threading.Event()
        self.workers = []
        for i in range(size):
            self.workers.append(worker(i + 1, self.queue, self.counter,
                                       self.stop_event, *args, **kwargs))

    def fill(self, iterable):
        """ Fill the queue with the items found in the `iterable`

        :param iterable: an iterable
        :type iterable: can be a dict, list, set
        """
        seq_iter = iterable if isinstance(iterable, dict) else xrange(len(iterable))
        for key in seq_iter:
            self.queue.put(iterable[key])
        self.counter.max = len(iterable)
        logging.debug('Queue filled with %d items' % len(iterable))

    def _wait(self):
        """ Wait for all the thread to end before exit

        .. note:: Used instead of the threading `join` method in order to allow
            the main thread to watch for event like `KeyboardInterrupt`
        """
        while threading.active_count() > 1:  # 1 = only the main thread remaining
            time.sleep(0.1)

    def start(self):
        """ Start the worker pool to process the queue

        .. note:: blocking call until the queue is empty or one of the thread
            raised an exception

        :raises: :class:`hgt2sql.app.ThreadPoolException` if one of the thread
            raised an exception
        """
        try:
            for worker in self.workers:
                worker.start()
            self._wait()
        except KeyboardInterrupt:
            self.stop_event.set()
            self._wait()  # Wait for threads to process the `stop_event`
            raise

        if self.stop_event.is_set():
            raise ThreadPoolException()


class Worker(threading.Thread):
    """ This worker is a thread. It subscribes to a queue. On each queue item,
    it executes the `process` method.

    .. note:: Implement the `process` method in child class. It gets an item
        from the queue as an argument

    .. warning:: in case of any error, it sets the `stop_event` to indicate to
        the program that it needs to exit

    :param int id_: id of the worker
    :param queue: the queue the worker subscribe to
    :type queue: :class:`queue.Queue`
    :param counter: a thread-safe counter with an `increment` method
    :type counter: TODO
    :param stop_event: a stop_event shared between all thread in the pool to
        indicate when an error occured
    :type stop_event: :class:`threading.Event`
    """

    def __init__(self, id_, queue, counter, stop_event):
        super(Worker, self).__init__()
        self.daemon = True
        self.id = id_
        self.queue = queue
        self.counter = counter
        self.stop_event = stop_event

    def run(self):
        """ Process items in the queue while it is not empty and while the
        `stop_event` is not set
        """
        self._log_debug('started')

        while True:
            if self.queue.empty() or self.stop_event.is_set():
                break

            self._get_queue()

        self._on_end()
        self._log_debug('stopped')

    def _get_queue(self):
        """ Get an item from the queue and call the `process` method on it.

        .. note:: in case of an exception, it sets the `stop_event`

        .. note:: child class needs to implement the `process` method
        """
        try:
            queue_item = self.queue.get()
            self.counter.increment()

            self.process(queue_item)

            self.queue.task_done()
        except Exception:
            self._log_debug('exception raised', None, exc_info=True)
            self.stop_event.set()

    def process(self, queue_item):
        """ Method called by `_get_queue` to process a queue_item.
        Implement it in child class

        :param queue_item: an item popped from the queue
        """
        raise Exception('process method not implemented in child worker')

    def _on_end(self):
        """ Executed when the worker ends """
        pass

    def _log_debug(self, message, params=None, exc_info=False):
        """ Helper method to log debug message or exception. It adds a prefix with the name
        of the class and the id of the thread

        :param str message: the message to print
        :param tuple params: the params to format the `message`
        :param bool exc_info: if True, calls :func:`logging.exception` in
            place of :func:`logging.debug`
        """
        message = message % params if params else message
        if exc_info:
            logging.exception('%s %d %s' % (self.__class__.__name__, self.id, message))
        else:
            logging.debug('%s %d %s' % (self.__class__.__name__, self.id, message))


class DownloadWorker(Worker):
    """ Worker in charge of downloading zip file into `folder` """

    def __init__(self, id_, queue, counter, stop_event, folder):
        super(DownloadWorker, self).__init__(id_, queue, counter, stop_event)
        self.folder = folder

    def process(self, queue_item):
        self._log_debug('downloading %s', (queue_item['url'],))
        logging.info('Downloading file %s' % self.counter)
        self._download_file(queue_item['url'], queue_item['zip'])

    def _download_file(self, url, filename):
        """ Download a file and stores it in `folder`

        :param str url: the url to download
        :param str filename: the name of the file created
        """
        hgt_zip_file = urlopen(url)
        with open(os.path.join(self.folder, filename), 'wb') as output:
            while True:
                data = hgt_zip_file.read(4096)
                if data and not self.stop_event.is_set():
                    output.write(data)
                else:
                    break


def download_hgt_zip_files(working_dir, data, concurrency, skip=False):
    """ Download the HGT zip files from remote server

    :param str working_dir: folder to put the downloaded files in
    :param dict data: dataset of SRTM data
    :param int concurrency: number of worker to start
    :param bool skip: if True skip this step
    """
    if skip:
        return

    logging.debug('Download start')
    download_task = ThreadPool(DownloadWorker, concurrency, working_dir)
    download_task.fill(data)
    download_task.start()
    logging.debug('Download end')


def load_dataset(dataset):
    if not os.path.isfile(dataset):
        dataset = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'datasets', '%s.json' % dataset)
    if not os.path.isfile(dataset):
        raise Exception('Invalid dataset %s' % dataset)

    with open(dataset) as dataset_file:
        data = json.load(dataset_file)
    return data


def run(*args, **kw):
    # These params will be parsed from args in the future
    skip_download = False
    skip_unzip = False
    clean_on_exit = True

    dataset = 'small'
    concurrency = 2
    working_dir = tempfile.mkdtemp('', 'hgt2sql_')

    verbose_level = logging.DEBUG


    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=verbose_level)

    # Do we really need this ?
    if not os.path.isdir(working_dir):
        logging.error('%s does not exist')
        sys.exit(1)
    if not os.access(working_dir, os.W_OK | os.X_OK):
        logging.error('%s is not writable')
        sys.exit(1)

    logging.info('config - dataset : %s' % dataset)
    logging.info('config - parallelism : %i' % concurrency)
    logging.info('config - working directory : %s' % working_dir)

    try:
        data = load_dataset(dataset)
        download_hgt_zip_files(working_dir, data['files'], concurrency,
                               skip=skip_download)
    except (KeyboardInterrupt, ThreadPoolException):
        # in case of ThreadPoolException, the worker which raised the error
        # logs it using logging.exception
        pass
    except Exception as exception:
        logging.exception(exception)

    # Clean on exit
    if clean_on_exit:
        logging.debug('Cleaning %s' % working_dir)
        shutil.rmtree(working_dir)
