import tempfile
import shutil
import logging
import os
import json
import sys
import glob
import struct


from . import worker

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')


class ImportWorker(worker.Worker):
    """ Worker in charge of reading hgt file found in `folder` and importing it """

    def __init__(self, id_, queue, counter, stop_event, folder, sampling):
        super(ImportWorker, self).__init__(id_, queue, counter, stop_event)
        self.folder = folder
        self.sampling = sampling

    def process(self, queue_item, counter_info):
        self._log_debug('importing %s', (queue_item,))
        logging.info('Importing file %d/%d' % counter_info)
        self._import_file(queue_item)

    def _import_file(self, filepath):
        """ Read a hgt file in `folder` and import it

        :param str filepath: the path of the file to import
        """
        filename, file_ext = os.path.splitext(os.path.basename(filepath))
        start_lat, start_lng = self._extract_coordinates_from_filename(filename)
        print(start_lat, start_lng)
        i = 0
        empty = 0
        zero = 0
        nbvalue = 0
        values = set([])
        previous = None
        changes = 0
        with open(filepath) as hgt_data:
            while True:
                buf = hgt_data.read(2)
                if not buf:
                    break
                altitude = struct.unpack('>h', buf)
                if previous is None or altitude[0] != previous:
                    previous = altitude[0]
                    changes += 1
                if altitude[0] == 0:
                    zero += 1
                elif altitude[0] == -32768:
                    empty += 1
                else:
                    nbvalue += 1
                    values.add(altitude[0])
                i += 1
        logging.debug((filename, zero, empty, nbvalue, len(values), changes))

    def _extract_coordinates_from_filename(self, filename):
        lat_order = filename[:1]
        start_lat = float(filename[1:3])
        if lat_order == 'S':
            start_lat =  -1 * start_lat
        lng_order = filename[3:4]
        start_lng = float(filename[4:])
        if lat_order == 'W':
            start_lng =  -1 * start_lng
        return start_lat, start_lng


def import_hgt_zip_files(working_dir, concurrency, sampling, skip=False):
    """ Extract the HGT zip files in working_dir

    :param str working_dir: folder where the zip files are
    :param int concurrency: number of worker to start
    :param int sampling: sampling value of the hgt file
    :param bool skip: if True skip this step
    """
    if skip:
        return

    logging.debug('Import start')
    import_task = worker.WorkerPool(ImportWorker, concurrency, working_dir, sampling)
    import_task.fill([os.path.realpath(filename) for filename in glob.glob(os.path.join(working_dir, "*.hgt"))])
    import_task.start()
    logging.debug('Import end')


def run(*args, **kw):
    # These params will be parsed from args in the future
    skip_download = True
    skip_unzip = False
    skip_import = False
    clean_on_exit = False

    dataset = 'small'
    concurrency = 2
    # working_dir = tempfile.mkdtemp('', 'gmaltcli_')
    working_dir = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'tmp'))

    verbose_level = logging.DEBUG

    logging.getLogger('root').setLevel(verbose_level)

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
        import_hgt_zip_files(working_dir, concurrency, data['sampling'],
                             skip=skip_import)
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
