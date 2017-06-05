gmalt CLI - gmalt-hgtread
=========================

Introduction
------------

This command allows to read values from HGT files. It works the same way as the standard command from ``GDAL`` : ``gdallocationinfo``

.. note:: the command has only been tested with SRTM3 dataset.

Usage
-----

The command takes 3 positional arguments :

- ``lat`` : the latitude of the elevation you are looking for
- ``lng`` : the longitude of the elevation you are looking for
- ``hgt_file`` : the HGT file you are searching the elevation inside

It returns :

- the zero indexed column number of the elevation value in the file
- the zero indexed line number of the elevation value in the file
- the elevation value

Examples
--------

.. code-block:: console

    $ gmalt-hgtread 1.0001 10.0001 gmaltcli/tests/srtm3/N00E010.hgt
    Report:
        Location: (0P,0L)
        Band 1:
            Value: 57

    $ gmalt-hgtread 2.0001 18.1251 gmaltcli/tests/srtm3/N00E010.hgt
    2017-06-05 20:19:27,460 - ERROR - point (2.0001, 18.1251) is not inside HGT file N00E010.hgt
