gmalt CLI
=========

.. image:: https://travis-ci.org/gmalt/cli.svg?branch=master
    :target: https://travis-ci.org/gmalt/cli

Introduction
------------

This project provides a few CLI commands to download, extract and import HGT data into a SQL database.

It has been tested on python 2.7, 3.4, 3.5 and 3.6 and works with PostgreSQL with and without PostGIS extension.

Documentation
-------------

- `Installation <https://github.com/gmalt/cli/blob/master/doc/install.rst>`_
- The CLI commands :
    - ``gmalt-hgtget`` : `download and extract HGT zip files <https://github.com/gmalt/cli/blob/master/doc/cli_hgtget.rst>`_
    - ``gmalt-hgtread`` : `read an elevation value in a HGT file <https://github.com/gmalt/cli/blob/master/doc/cli_hgtread.rst>`_
    - ``gmalt-hgtload`` : `load the HGT data in a SQL database <https://github.com/gmalt/cli/blob/master/doc/cli_hgtload.rst>`_

Roadmap
-------

Feel free to make a pull request for any of the items in this list :

* Provide a bash script in case the executable ``raster2pgsql`` is present
* Improve interface with parser using ``namedtuple``
* Support MySQL without GIS extension
* Support MySQL with GIS extension
* ``gmalt-hgtload`` should support importing a single file instead of the content of a folder
