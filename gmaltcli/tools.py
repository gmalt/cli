import argparse
import os
import json
import glob
import logging

import gmaltcli.worker as worker


def dataset_file(dataset):
    if not os.path.isfile(dataset):
        dataset = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'datasets', '{}.json'.format(dataset))

    if not os.path.isfile(dataset):
        raise argparse.ArgumentTypeError('Invalid dataset {}'.format(dataset))

    return dataset


def configure_logging(verbosity_level):
    verbose_level = logging.DEBUG if verbosity_level else logging.INFO
    logging.getLogger().setLevel(verbose_level)


def existing_folder(folder_path):
    fullpath = os.path.realpath(folder_path)

    if not os.path.isdir(fullpath):
        raise argparse.ArgumentTypeError('{} does not exist'.format(fullpath))

    return fullpath


def writable_folder(folder_path):
    fullpath = existing_folder(folder_path)

    if not os.access(fullpath, os.W_OK | os.X_OK):
        raise argparse.ArgumentTypeError('{} is not writable'.format(fullpath))

    return fullpath


class LoadDatasetAction(argparse.Action):
    """ Load a dataset from a json file

    .. note:: this action adds 2 keywords to the :class:`argparse.Namespace` : `dataset_sampling` to indicate if
        we are loading srtm1 or srtm3 data and `dataset_files` which is a dict of all HGT files in this dataset
    """

    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed in argument {}".format(dest))
        super(LoadDatasetAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)
        with open(values) as dataset_file:
            data = json.load(dataset_file)
        setattr(namespace, 'dataset_sampling', data['sampling'])
        setattr(namespace, 'dataset_files', data['files'])


def download_hgt_zip_files(working_dir, data, concurrency, skip=False):
    """ Download the HGT zip files from remote server

    :param str working_dir: folder to put the downloaded files in
    :param dict data: dataset of SRTM data
    :param int concurrency: number of worker to start
    :param bool skip: if True skip this step
    """
    if skip:
        logging.debug('Download skipped')
        return

    logging.info('Nb of files to download : {}'.format(len(data)))
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
        logging.debug('Extract skipped')
        return

    zip_files = [os.path.realpath(filename) for filename in glob.glob(os.path.join(working_dir, "*.zip"))]
    logging.info('Nb of files to extract : {}'.format(len(zip_files)))
    logging.debug('Extract start')
    extract_task = worker.WorkerPool(worker.ExtractWorker, concurrency, working_dir)
    extract_task.fill(zip_files)
    extract_task.start()
    logging.debug('Extract end')
