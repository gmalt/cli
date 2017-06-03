# -*- coding: utf-8 -*-
import os
import threading
import logging
import time
import zipfile

try:
    # Python 3
    import queue
    from urllib.request import urlopen
    from urllib.error import HTTPError, URLError
except ImportError:
    # Python 2
    from urllib2 import urlopen, HTTPError, URLError
    import Queue as queue

try:
    # Python 2
    xrange
except NameError:
    # Python 3
    xrange = range

import gmalthgtparser as hgt


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
            next_counter = self.counter
        return next_counter, self.max

    def get(self):
        """ Get the counter current value """
        return self.counter

    def __str__(self):
        return '%d/%d' % (self.counter, self.max)


class WorkerPoolException(Exception):
    """ Exception raised by :class:`gmaltcli.worker.WorkerPool` when one of its
    thread has raised an exception
    """
    pass


class WorkerPool(object):
    """ Create a pool of worker Thread which subscribe to a queue and process its
    item

    .. note:: the constructor other args and kwargs are passed as additionnal
        params to the worker __init__ method

    :param worker: The class of the Worker thread
    :type worker: :class:`gmaltcli.worker.Worker`
    :param int size: number of worker to create in pool
    """
    def __init__(self, worker, size, *args, **kwargs):
        self.queue = queue.Queue()
        self.counter = SafeCounter()
        self.stop_event = threading.Event()
        self.workers = []
        for i in range(size):
            # noinspection PyCallingNonCallable
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
        while any([worker.isAlive() for worker in self.workers]):
            time.sleep(0.1)

    def start(self):
        """ Start the worker pool to process the queue

        .. note:: blocking call until the queue is empty or one of the thread
            raised an exception

        :raises: :class:`gmaltcli.worker.WorkerPoolException` if one of the thread
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
            raise WorkerPoolException()


class Worker(threading.Thread):
    """ This worker is a thread. It subscribes to a queue. On each queue item,
    it executes the `process` method.

    .. note:: Implement the `process` method in child class. It gets an item
        from the queue as an argument

    .. warning:: in case of any error, it sets the `stop_event` to indicate to
        the program that it needs to exit

    :param int id_: id of the worker
    :param queue_obj: the queue the worker subscribe to
    :type queue_obj: :class:`queue.Queue`
    :param counter: a thread-safe counter with an `increment` method
    :type counter: :class:`hgt2sql.worker.SafeCounter`
    :param stop_event: a stop_event shared between all thread in the pool to
        indicate when an error occured
    :type stop_event: :class:`threading.Event`
    """

    def __init__(self, id_, queue_obj, counter, stop_event):
        super(Worker, self).__init__()
        self.daemon = True
        self.id = id_
        self.queue = queue_obj
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
            counter_info = self.counter.increment()

            self.process(queue_item, counter_info)

            self.queue.task_done()
        except Exception as exception:
            logging.exception(exception)
            self._log_debug('exception raised')
            self.stop_event.set()

    def process(self, queue_item, counter_info):
        """ Method called by `_get_queue` to process a queue_item.
        Implement it in child class

        :param queue_item: an item popped from the queue
        :param str counter_info: information on the counter state
        """
        raise Exception('process method not implemented in child worker')

    def _on_end(self):
        """ Executed when the worker ends """
        pass

    def _log(self, level, message, params=None, prefix=None):
        prefix = prefix if prefix is not None else self.__class__.__name__
        message = message % params if params else message
        logging.log(level, '%s %d %s' % (prefix, self.id, message))

    def _log_debug(self, message, params=None, prefix=None):
        """ Helper method to log debug message or exception. It adds a
        prefix with the name of the class and the id of the thread

        :param str message: the message to print
        :param tuple params: the params to format the `message`
        """
        self._log(logging.DEBUG, message, params, prefix)

    def _log_info(self, message, params=None, prefix=None):
        """ Helper method to log info message. It adds a
        prefix with the name of the class and the id of the thread

        :param str message: the message to print
        :param tuple params: the params to format the `message`
        """
        self._log(logging.INFO, message, params, prefix)


class DownloadWorker(Worker):
    """ Worker in charge of downloading zip file into `folder` """

    def __init__(self, id_, queue_obj, counter, stop_event, folder):
        super(DownloadWorker, self).__init__(id_, queue_obj, counter, stop_event)
        self.folder = folder

    def process(self, queue_item, counter_info):
        self._log_debug('downloading %s', (queue_item['url'],))
        self._log_info('Downloading file %d/%d', counter_info, prefix='download')
        self._secured_download_file(queue_item['url'], queue_item['zip'])
        self._log_debug('downloaded %s', (queue_item['url'],))

    def _secured_download_file(self, url, filename):
        """ Download a file and stores it in `folder`

        .. note:: the download is delegated to method :meth:`worker.DownloadWorker._download_file`.
            This method is just a wrapper to catch exception from :mod:`urllib`

        :param str url: the url to download
        :param str filename: the name of the file created
        """
        try:
            self._download_file(url, filename)
        except URLError:
            logging.error('Unable to download file {}. Verify your internet connection'.format(url))
            raise
        except HTTPError:
            logging.error('Unable to download file {}. Verify the link.'.format(url))
            raise

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


class ExtractWorker(Worker):
    """ Worker in charge of extracting zip file found in `folder` """

    def __init__(self, id_, queue_obj, counter, stop_event, folder):
        super(ExtractWorker, self).__init__(id_, queue_obj, counter, stop_event)
        self.folder = folder

    def process(self, queue_item, counter_info):
        self._log_debug('extracting %s', (queue_item,))
        self._log_info('Extracting file %d/%d' % counter_info, prefix='extract')
        self._extract_file(queue_item)
        self._log_debug('extracted %s', (queue_item,))

    def _extract_file(self, filename):
        """ Extract a zip file in `folder`

        :param str filename: the name of the file to extract
        """
        with zipfile.ZipFile(filename) as zip_fd:
            for name in zip_fd.namelist():
                zip_fd.extract(name, self.folder)


class ImportWorker(Worker):
    """ Worker in charge of reading hgt file found in `folder` and importing it """

    def __init__(self, id_, queue_obj, counter, stop_event, folder, factory, use_raster, samples):
        super(ImportWorker, self).__init__(id_, queue_obj, counter, stop_event)
        self.folder = folder
        self.factory = factory
        self.use_raster = use_raster
        self.sample_with, self.sample_height = samples

    def process(self, queue_item, counter_info):
        """ Import one HGT file

        :param str queue_item: the HGT filepath to import
        :param counter_info: the counter for the current queue
        :type counter_info: :class:`gmaltcli.worker.SafeCounter`
        """
        self._log_debug('importing %s', (queue_item,))
        self._log_info('Importing file %d/%d' % counter_info, prefix='import')
        self._import_file(queue_item)

    def _import_file(self, filepath):
        """ Read a hgt file in `folder` and import it

        :param str filepath: the path of the file to import
        """
        with self.factory.get_manager(self.use_raster) as manager:
            with hgt.HgtParser(filepath) as parser:
                elev_iter = self._get_iterator(parser)
                self._execute_import(elev_iter, manager)

    def _get_iterator(self, parser):
        """ Get the right HTML iterator for the import task

        :param parser: the HGT parser for the file
        :type parser: :class:`gmalthgtparser.HgtParser`
        :return: a HGT iterator
        :rtype: iter
        """
        if self.use_raster:
            width = self.sample_with or parser.sample_lng
            height = self.sample_height or parser.sample_lat
            return parser.get_sample_iterator(width, height)
        else:
            return parser.get_value_iterator()

    def _execute_import(self, elev_iter, manager):
        """ Method called to import the data from a HGT iterator

        :param iter elev_iter: iterator of elevation HGT data
        :param manager: manager to import data into database
        :type manager: :class:`gmaltcli.database.BaseManager`
        """
        total = elev_iter.nb_values
        processed = 0
        last_percentage = 0

        for value in elev_iter:
            # Break import task if an error occured in another thread or if KeyboardInterrupt
            if self.stop_event.is_set():
                break

            manager.insert_data(value, elev_iter.parser)

            # Display progress as percentage
            processed += 1
            percents = float(processed) / total * 100
            if int(percents) != last_percentage:
                self._log_info("{0:.0f}% {1}/{2}".format(percents, processed, total), prefix='import')
                last_percentage = int(percents)
