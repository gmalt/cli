import glob
import json
import logging
import os
import shutil
import sys
import argparse

import gmaltcli.hgt as hgt
import gmaltcli.tools as tools
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


def create_get_hgt_parser():
    """ CLI parser for gmalt-hgtget

    :return: cli parser
    :rtype: :class:`argparse.ArgumentParser`
    """
    parser = argparse.ArgumentParser(description='Download and unzip HGT files from a remote source')
    parser.add_argument('dataset', type=tools.dataset_file, action=tools.LoadDatasetAction,
                        help='A dataset file provided by this package or the path to your own dataset file. Please '
                             'read documentation to get dataset JSON format')
    parser.add_argument('folder', type=tools.writable_folder,
                        help='Path to the folder where the HGT zip will be downloaded or where the HGT zip have '
                             'already been downloaded.')
    parser.add_argument('--skip-download', dest='skip_download', action='store_true',
                        help='Set this flag if you don\'t want to download the zip files.')
    parser.add_argument('--skip-unzip', dest='skip_unzip', action='store_true',
                        help='Set this flag if you don\'t want to unzip the HGT zip files')
    parser.add_argument('-c', type=int, dest='concurrency', default=1,
                        help='How many worker will attempt to download or unzip files in parallel')
    parser.add_argument('-v', dest='verbose', action='store_true', help='increase verbosity level')
    return parser


def get_hgt():
    """ Function called by the console_script `gmalt-hgtget`

    Usage:

        gmalt-hgtget [options] <dataset> <folder>
    """
    # Parse command line arguments
    parser = create_get_hgt_parser()
    args = parser.parse_args()

    verbose_level = logging.DEBUG if args.verbose else logging.INFO
    logging.getLogger().setLevel(verbose_level)

    logging.info('config - dataset file : %s' % args.dataset)
    logging.info('config - parallelism : %i' % args.concurrency)
    logging.info('config - folder : %s' % args.folder)

    try:
        # Download HGT zip file in a pool of thread
        tools.download_hgt_zip_files(args.folder, args.dataset_files, args.concurrency,
                                     skip=args.skip_download)
        # Unzip in folder all HGT zip files found in folder
        tools.extract_hgt_zip_files(args.folder, args.concurrency, skip=args.skip_unzip)
    except (KeyboardInterrupt, worker.WorkerPoolException):
        # in case of ThreadPoolException, the worker which raised the error
        # logs it using logging.exception
        pass
    except Exception as exception:
        logging.exception(exception)
