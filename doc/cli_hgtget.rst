gmalt CLI - gmalt-hgtget
========================


Introduction
------------

This command dowloads and extracts SRTM zip files.

.. note:: For now, only SRTM3 dataset is available.


Usage
-----

This command takes 4 options :

- ``-v`` : increase verbosity level
- ``-c <concurrency>`` : set the number of threads that are going to download and unzip files in parallel
- ``--skip-download`` : skip the download step
- ``--skip-unzip`` : skip the unzip step

And takes 2 positional arguments :

- ``dataset`` : the name of a prepared dataset or the path to a file describing your dataset. The available datasets
are :
    - ``srtm3`` : to download all the HGT files covering the whole world
    - ``small`` : a subset of SRTM3 for testing purposes
- ``folder`` : the folder where the HGT zip files will be downloaded and unarchived (or if you have already
downloaded it, the folder where the HGT zip files are stored to unarchive)


Examples
--------

.. code-block:: console

    $ mkdir tmp
    $ gmalt-hgtget small tmp/
    2017-06-05 20:40:36,605 - INFO - config - dataset file : pathto/gmaltcli/datasets/small.json
    2017-06-05 20:40:36,606 - INFO - config - parallelism : 1
    2017-06-05 20:40:36,606 - INFO - config - folder : pathto/tmp
    2017-06-05 20:40:36,606 - INFO - Nb of files to download : 3
    2017-06-05 20:40:36,606 - INFO - download 1 Downloading file 1/3
    2017-06-05 20:40:39,414 - INFO - download 1 Downloading file 2/3
    2017-06-05 20:40:43,240 - INFO - download 1 Downloading file 3/3
    2017-06-05 20:40:44,518 - INFO - Nb of files to extract : 3
    2017-06-05 20:40:44,519 - INFO - extract 1 Extracting file 1/3
    2017-06-05 20:40:44,541 - INFO - extract 1 Extracting file 2/3
    2017-06-05 20:40:44,566 - INFO - extract 1 Extracting file 3/3
    $ ls tmp/
    N00E006.hgt  N00E006.hgt.zip  N00E009.hgt  N00E009.hgt.zip  N00E010.hgt  N00E010.hgt.zip
