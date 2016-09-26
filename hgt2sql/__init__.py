import tempfile
import shutil
import logging
import os
import json
import sys
import glob
import zipfile
try:
    from urllib.request import urlopen
    from urllib.error import HTTPError
except ImportError:
    from urllib2 import urlopen, HTTPError

from . import worker


class DownloadWorker(worker.Worker):
    """ Worker in charge of downloading zip file into `folder` """

    def __init__(self, id_, queue, counter, stop_event, folder):
        super(DownloadWorker, self).__init__(id_, queue, counter, stop_event)
        self.folder = folder

    def process(self, queue_item, counter_info):
        self._log_debug('downloading %s', (queue_item['url'],))
        logging.info('Downloading file %d/%d' % counter_info)
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


class ExtractWorker(worker.Worker):
    """ Worker in charge of extracting zip file found in `folder` """

    def __init__(self, id_, queue, counter, stop_event, folder):
        super(ExtractWorker, self).__init__(id_, queue, counter, stop_event)
        self.folder = folder

    def process(self, queue_item, counter_info):
        self._log_debug('extracting %s', (queue_item,))
        logging.info('Extracting file %d/%d' % counter_info)
        self._extract_file(queue_item)

    def _extract_file(self, filename):
        """ Extract a zip file in `folder`

        :param str filename: the name of the file to extract
        """
        with zipfile.ZipFile(filename) as zip_fd:
            for name in zip_fd.namelist():
                zip_fd.extract(name, self.folder)


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
    download_task = worker.WorkerPool(DownloadWorker, concurrency, working_dir)
    download_task.fill(data)
    download_task.start()
    logging.debug('Download end')


def extract_hgt_zip_files(working_dir, concurrency, skip=False):
    """ Extract the HGT zip files in working_dir

    :param str working_dir: folder where the zip files are
    :param int concurrency: number of worker to start
    :param bool skip: if True skip this step
    """
    if skip:
        return

    logging.debug('Extract start')
    extract_task = worker.WorkerPool(ExtractWorker, concurrency, working_dir)
    extract_task.fill([os.path.realpath(filename) for filename in glob.glob(os.path.join(working_dir, "*.zip"))])
    extract_task.start()
    logging.debug('Extract end')


def load_dataset(dataset):
    """ Load a dataset from a json file

    :param str dataset: can be the name of a json file in the `datasets` folder
        or the path to a custom json file
    :return: json load of the dataset file
    :rtype: dict
    """
    if not os.path.isfile(dataset):
        dataset = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'datasets', '%s.json' % dataset)
    if not os.path.isfile(dataset):
        raise Exception('Invalid dataset %s' % dataset)

    with open(dataset) as dataset_file:
        data = json.load(dataset_file)
    return data


def run(*args, **kw):
    # These params will be parsed from args in the future
    skip_download = True
    skip_unzip = False
    clean_on_exit = False

    dataset = 'small'
    concurrency = 2
    #working_dir = tempfile.mkdtemp('', 'hgt2sql_')
    working_dir = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'tmp'))

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
        extract_hgt_zip_files(working_dir, concurrency, skip=skip_unzip)
    except (KeyboardInterrupt, worker.WorkerPoolException):
        # in case of ThreadPoolException, the worker which raised the error
        # logs it using logging.exception
        pass
    except Exception as exception:
        logging.exception(exception)

    # Clean on exit
    if clean_on_exit:
        logging.debug('Cleaning %s' % working_dir)
        shutil.rmtree(working_dir)
