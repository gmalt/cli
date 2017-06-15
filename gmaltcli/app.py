# -*- coding: utf-8 -*-
import logging
import sys
import argparse
import sqlalchemy.exc

import gmalthgtparser as hgt

import gmaltcli.tools as tools
import gmaltcli.worker as worker
import gmaltcli.database as database

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')


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

    tools.configure_logging(args.verbose)

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


def create_load_hgt_parser():
    """ CLI parser for gmalt-hgtload

    :return: cli parser
    :rtype: :class:`argparse.ArgumentParser`
    """
    parser = argparse.ArgumentParser(description='Read HGT files and import elevation values into a database')
    parser.add_argument('folder', type=tools.existing_folder,
                        help='Path to the folder where the HGT files are stored.')
    parser.add_argument('-c', type=int, dest='concurrency', default=1,
                        help='How many worker will attempt to load files in parallel')
    parser.add_argument('-v', dest='verbose', action='store_true', help='increase verbosity level')
    parser.add_argument('-tb', '--traceback', dest='traceback', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('-e', '--echo', dest='echo', action='store_true', help=argparse.SUPPRESS)

    # Database connection args
    db_group = parser.add_argument_group('database', 'database connection configuration')
    db_group.add_argument('--type', type=str, dest='type', default="postgres",
                          help='The type of your database (default : postgres)')
    db_group.add_argument('-H', '--host', type=str, dest='host', default="localhost",
                          help='The hostname of the database')
    db_group.add_argument('-P', '--port', type=int, dest='port', help='The port of the database')
    db_group.add_argument('-d', '--db', type=str, dest='database', default="gmalt", help='The name of the database')
    db_group.add_argument('-u', '--user', type=str, dest='username', required=True,
                          help='The user to connect to the database')
    db_group.add_argument('-p', '--pass', type=str, dest='password', help='The password to connect to the database')
    db_group.add_argument('-t', '--table', type=str, dest='table', default="elevation",
                          help='The table name to import data')

    # Raster configuration
    gis_group = parser.add_argument_group('gis', 'GIS configuration')
    gis_group.add_argument('-r', '--raster', dest='use_raster', action='store_true',
                           help='Use raster to import data. Your database must have GIS capabilities '
                                'like PostGIS for PostgreSQL.')
    gis_group.add_argument('-s', '--sample', nargs=2, type=int, dest='sample', metavar=('LNG_SAMPLE', 'LAT_SAMPLE'),
                           default=(None, None), help="Separate a HGT file in multiple rasters. Sample on lng axis "
                                                      "and lat axis.")
    gis_group.add_argument('--skip-raster2pgsql-check', dest='check_raster2pgsql', default=True, action='store_false',
                           help='Skip raster2pgsql presence check')

    return parser


def load_hgt():
    """ Function called by the console_script `gmalt-hgtload`

    Usage:

        gmalt-hgtload [options] -u <user> <folder>
    """
    # Parse command line arguments
    parser = create_load_hgt_parser()
    args = vars(parser.parse_args())

    # logging
    traceback = args.pop('traceback')
    tools.configure_logging(args.pop('verbose'), echo=args.pop('echo'))

    # Pop everything not related to database uri string
    concurrency = args.pop('concurrency')
    folder = args.pop('folder')
    use_raster = args.pop('use_raster')
    samples = args.pop('sample')
    db_driver = args.pop('type')
    table_name = args.pop('table')
    check_raster2pgsql = args.pop('check_raster2pgsql')

    # sqlalchemy.engine.url.URL args
    db_info = args

    # If postgres driver and raster2pgsql is available, propose to use this solution instead.
    if db_driver == 'postgres' and use_raster and check_raster2pgsql and tools.check_for_raster2pgsql():
        sys.exit(0)

    logging.info('config - parallelism : %i' % concurrency)
    logging.info('config - folder : %s' % folder)
    logging.info('config - db driver : %s' % db_driver)
    logging.info('config - db host : %s' % db_info.get('host'))
    logging.info('config - db user : %s' % db_info.get('username'))
    logging.info('config - db name : %s' % db_info.get('database'))
    logging.info('config - db table : %s' % table_name)
    if use_raster:
        logging.debug('config - use raster : %s' % use_raster)
        logging.debug('config - raster sampling : {}'.format('{}x{}'.format(*samples) if samples[0] else 'none'))

    # create sqlalchemy engine
    factory = database.ManagerFactory(db_driver, table_name, pool_size=concurrency, **db_info)

    try:
        # First validate that the database is ready
        with factory.get_manager(use_raster) as manager:
            manager.prepare_environment()

        # Then process HGT files
        tools.import_hgt_zip_files(folder, concurrency, factory, use_raster, samples)
    except sqlalchemy.exc.OperationalError:
        logging.error('Unable to connect to database with these settings : {}'.format(factory.engine.url),
                      exc_info=traceback)
    except database.NotSupportedException:
        logging.error('Database does not support raster settings. Have you enabled GIS extension ?', exc_info=traceback)
    except (KeyboardInterrupt, worker.WorkerPoolException):
        # in case of ThreadPoolException, the worker which raised the error
        # logs it using logging.exception
        pass
    except Exception as e:
        logging.error('Unknown error : {}'.format(str(e)), exc_info=traceback)
