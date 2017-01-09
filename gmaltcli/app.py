import glob
import json
import logging
import os
import shutil
import sys
import argparse

import gmaltcli.hgt as hgt
import gmaltcli.worker as worker


def create_read_from_hgt_parser():
    """ CLI parser for gmalt-hgtread

    :return: cli parser
    :rtype: :class:`argparse.ArgumentParser`
    """
    parser = argparse.ArgumentParser(description='Pass along the latitude/longitude of the point you want to '
                                                 'know the latitude of and a HGT file. It will look for the '
                                                 'elevation of your point into the file and return it.')
    parser.add_argument('lat', type=float, help='The latitude of your point (example: 48.861295)')
    parser.add_argument('lng', type=float, help='The longitude of your point (example: 2.339703)')
    parser.add_argument('hgt_file', type=str, help='The file to load (example: N00E010.hgt)')
    return parser


def read_from_hgt():
    """ Function called by the console_script `gmalt-hgtread`

    Usage:

        gmalt-hgtread <lat> <lng> <path to hgt file>

    Print on stdout :

        Report:
            Location: (408P,166L)
            Band 1:
                Value: 644
    """
    parser = create_read_from_hgt_parser()
    args = parser.parse_args()

    try:
        with hgt.HgtParser(args.hgt_file) as hgt_parser:
            elev_data = hgt_parser.get_elevation((args.lat, args.lng))
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)

    sys.stdout.write('Report:\n')
    sys.stdout.write('    Location: ({}P,{}L)\n'.format(elev_data[1], elev_data[0]))
    sys.stdout.write('    Band 1:\n')
    sys.stdout.write('        Value: {}\n'.format(elev_data[2]))


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
    download_task = worker.WorkerPool(worker.DownloadWorker, concurrency, working_dir)
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
    extract_task = worker.WorkerPool(worker.ExtractWorker, concurrency, working_dir)
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


def create_get_hgt_parser():
    """ CLI parser for gmalt-hgtread

    :return: cli parser
    :rtype: :class:`argparse.ArgumentParser`
    """
    parser = argparse.ArgumentParser(description='Download and unzip HGT files from a remote source')
    parser.add_argument('dataset', type=str, help='A dataset file provided by this package or the path to your own '
                                                  'dataset file. Please read documentation to get dataset JSON format')
    parser.add_argument('folder', type=str, help='Path to the folder where the HGT zip will be downloaded or where the '
                                                 'HGT zip have already been downloaded.')
    parser.add_argument('--skip-download', dest='skip_download', action='store_true', help='Set this flag if you don\'t'
                                                                                           ' want to download the zip '
                                                                                           'files.')
    parser.add_argument('--skip-unzip', dest='skip_unzip', action='store_true', help='Set this flag if you don\'t want '
                                                                                     'to unzip the HGT zip files')
    parser.add_argument('-c', type=int, dest='concurrency', default=1, help='How many worker will attempt to download '
                                                                            'or unzip files in parallel')
    return parser


def get_hgt():
    """ Function called by the console_script `gmalt-hgtget`

    Usage:

        gmalt-hgtget [options] <dataset> <folder>
    """
    parser = create_get_hgt_parser()
    args = parser.parse_args()
    print(args)
    """
    # These params will be parsed from args in the future
    skip_download = True
    skip_unzip = False

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
    """
