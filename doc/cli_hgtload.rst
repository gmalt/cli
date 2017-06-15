gmalt CLI - gmalt-hgtload
=========================

Introduction
------------

This command allows to parse HGT files and load the elevation values in a database.

Elevation data can be loaded in 2 formats :

- ``standard`` : the default way
- ``raster`` : for database supporting raster GIS field (for example ``PostGIS`` extension for ``postgres``)

For now, it supports only ``postgresql`` but adding other database support should be straightforward.

.. note:: the command has only been tested with SRTM3 dataset.

Usage
-----

The command takes 12 options :

- Generic options :
    - ``-v`` : increase verbosity level
    - ``-c <concurrency>`` : set the number of threads that are going to load files in parallel

- Database connection options :
    - ``--type TYPE`` : the type of database (default : postgres. and it is the only supported value for now)
    - ``--host HOST`` : the hostname of the database
    - ``--port PORT`` : the port of the database
    - ``--db DATABASE`` : the name of the database
    - ``--user USERNAME`` : the user to connect to the database
    - ``--pass PASSWORD`` : the password to connect to the database
    - ``--table TABLE`` : the name of the table where the data will be imported

- GIS options :
    - ``--raster`` : set this option if you want to import the data in a raster format
    - ``--sample LNG_SAMPLE LAT_SAMPLE`` : if the previous flag is set, you can configure the size of each raster. If not provided, one raster per file.

And takes one positional argument :

- ``folder`` : the folder where the HGT unziped raw files are stored

Standard format and example
---------------------------

In the standard format (when you don't enable the GIS options in the cli), each elevation value is stored in a row.

This is the output of such a table ``elevation`` in ``postgres`` (if it does not exist, the script will create it automatically) :

.. code-block::

    postgres=# \d elevation
              Table "public.elevation"
     Column  |       Type       | Modifiers
    ---------+------------------+-----------
     lat_min | double precision | not null
     lng_min | double precision | not null
     lat_max | double precision | not null
     lng_max | double precision | not null
     value   | smallint         |
    Indexes:
        "elevation_pkey" PRIMARY KEY, btree (lat_min, lng_min, lat_max, lng_max)

After importing the elevation date, you can find the elevation value in meter at the latitude 48.8566 and the longitude 2.3522 by using this kind of query :

.. code-block::

    SELECT value FROM elevation WHERE lat_min < 48.8566
                                      AND lat_max > 48.8566
                                      AND lng_min < 2.3522
                                      AND lng_max > 2.3522;

As an example, I am going to suppose you have run `gmalt-hgtget <https://github.com/gmalt/cli/blob/master/doc/cli_hgtget.rst>`_ in a ``path/to/downloaded/hgt/files``.
You have a PostgreSQL server available at the IP ``172.16.0.5`` and a user ``gmalt`` with the password ``gmalt`` with read/write permissions on an existing ``gmalt`` database.

You can execute :

.. code-block:: console

    $ gmalt-hgtload -c 3 -u gmalt -p gmalt -d gmalt -H '172.16.0.5' -t elevation path/to/downloaded/hgt/files/
    2017-06-15 22:07:47,899 - INFO - config - parallelism : 3
    2017-06-15 22:07:47,899 - INFO - config - folder : path/to/downloaded/hgt/files/
    2017-06-15 22:07:47,899 - INFO - config - db driver : postgres
    2017-06-15 22:07:47,899 - INFO - config - db host : 172.16.0.5
    2017-06-15 22:07:47,899 - INFO - config - db user : gmalt
    2017-06-15 22:07:47,899 - INFO - config - db name : gmalt
    2017-06-15 22:07:47,899 - INFO - config - db table : elevation
    2017-06-15 22:07:47,923 - DEBUG - Database compatible with provided settings.
    2017-06-15 22:07:47,925 - DEBUG - Table elevation not found. Creation in progress.
    2017-06-15 22:07:47,934 - INFO - Table elevation created.
    2017-06-15 22:07:47,935 - INFO - Nb of files to import : 3
    2017-06-15 22:07:47,935 - DEBUG - Import start
    2017-06-15 22:07:47,935 - DEBUG - Queue filled with 3 items
    2017-06-15 22:07:47,935 - DEBUG - ImportWorker 1 started
    2017-06-15 22:07:47,935 - DEBUG - ImportWorker 1 importing path/to/downloaded/hgt/files/N00E009.hgt
    2017-06-15 22:07:47,935 - INFO - import 1 Importing file 1/3
    2017-06-15 22:07:47,935 - DEBUG - ImportWorker 2 started
    2017-06-15 22:07:47,935 - DEBUG - ImportWorker 3 started
    2017-06-15 22:07:47,936 - DEBUG - ImportWorker 2 importing path/to/downloaded/hgt/files/N00E010.hgt
    2017-06-15 22:07:47,936 - DEBUG - ImportWorker 3 importing path/to/downloaded/hgt/files/N00E006.hgt
    2017-06-15 22:07:47,936 - INFO - import 2 Importing file 2/3
    2017-06-15 22:07:47,936 - INFO - import 3 Importing file 3/3
    2017-06-15 22:08:45,382 - INFO - import 1 1% 14425/1442401
    2017-06-15 22:08:45,506 - INFO - import 2 1% 14425/1442401
    2017-06-15 22:08:45,543 - INFO - import 3 1% 14425/1442401
    ...
    2017-06-15 22:09:43,499 - INFO - import 2 100% 1442401/1442401
    2017-06-15 22:09:43,519 - DEBUG - ImportWorker 2 stopped
    2017-06-15 22:10:40,816 - INFO - import 1 100% 1442401/1442401
    2017-06-15 22:10:41,019 - DEBUG - ImportWorker 1 stopped
    2017-06-15 22:10:41,048 - INFO - import 3 100% 1442401/1442401
    2017-06-15 22:10:41,962 - INFO - ImportWorker 3 stopped
    2017-06-15 22:10:42,250 - DEBUG - Import end

Raster format and example
-------------------------

If you use PostgreSQL, you will have to install the ``PostGIS`` extension and to enable this extension in the database where you want to import the elevation values.

In the raster format (when you enable the GIS options in the cli), elevation values are stored in raster format (a special kind of field provided by the GIS extension of your database).

This is the output of such a table ``elevation`` with the ``PostGIS`` extension enabled in ``postgres`` (if it does not exist, the script will create it automatically) :

.. code-block:: console

    postgres=# \d elevation
                              Table "public.elevation"
     Column |  Type   |                              Modifiers
    --------+---------+--------------------------------------------------------
     rid    | integer | not null default nextval('elevation_rid_seq'::regclass)
     rast   | raster  |
    Indexes:
        "elevation_pkey" PRIMARY KEY, btree (rid)
        "elevation_rast_gist_idx" gist (st_convexhull(rast))

As an example, I am going to suppose you have run `gmalt-hgtget <https://github.com/gmalt/cli/blob/master/doc/cli_hgtget.rst>`_ in a ``path/to/downloaded/hgt/files``.
You have a PostgreSQL server available at the IP ``172.16.0.5`` and a user ``gmalt`` with the password ``gmalt`` with read/write permissions on an existing ``gmalt`` database where the PostGIS extension is enabled.
I am going to choose to split each HGT file in different raster of size 50x50. If you don't provide a sample configuration, each HGT file is stored completely in a raster.

You can execute :

.. code-block:: console

    $ gmalt-hgtload -c 3 -u gmalt -p gmalt -d gmalt -H '172.16.0.5' -t elevation -r -s 50 50 path/to/downloaded/hgt/files/
    2017-06-15 22:43:51,042 - INFO - config - parallelism : 3
    2017-06-15 22:43:51,042 - INFO - config - folder : path/to/downloaded/hgt/files/
    2017-06-15 22:43:51,042 - INFO - config - db driver : postgres
    2017-06-15 22:43:51,042 - INFO - config - db host : 172.16.0.5
    2017-06-15 22:43:51,042 - INFO - config - db user : gmalt
    2017-06-15 22:43:51,042 - INFO - config - db name : gmalt
    2017-06-15 22:43:51,042 - INFO - config - db table : elevation
    2017-06-15 22:43:51,042 - DEBUG - config - use raster : True
    2017-06-15 22:43:51,042 - DEBUG - config - raster sampling : 50x50
    2017-06-15 22:43:51,066 - DEBUG - Database compatible with provided settings.
    2017-06-15 22:43:51,068 - DEBUG - Table elevation exists. Nothing to create.
    2017-06-15 22:43:51,068 - INFO - Nb of files to import : 3
    2017-06-15 22:43:51,068 - DEBUG - Import start
    2017-06-15 22:43:51,068 - DEBUG - Queue filled with 3 items
    2017-06-15 22:43:51,069 - DEBUG - ImportWorker 1 started
    2017-06-15 22:43:51,069 - DEBUG - ImportWorker 2 started
    2017-06-15 22:43:51,069 - DEBUG - ImportWorker 1 importing path/to/downloaded/hgt/files/N00E009.hgt
    2017-06-15 22:43:51,069 - DEBUG - ImportWorker 3 started
    2017-06-15 22:43:51,069 - DEBUG - ImportWorker 2 importing path/to/downloaded/hgt/files/N00E010.hgt
    2017-06-15 22:43:51,069 - INFO - import 1 Importing file 1/3
    2017-06-15 22:43:51,069 - DEBUG - ImportWorker 3 importing path/to/downloaded/hgt/files/N00E006.hgt
    2017-06-15 22:43:51,069 - INFO - import 2 Importing file 2/3
    2017-06-15 22:43:51,069 - INFO - import 3 Importing file 3/3
    ...
    2017-06-15 22:44:06,722 - DEBUG - ImportWorker 2 stopped
    ...
    2017-06-15 22:44:08,241 - DEBUG - ImportWorker 1 stopped
    ...
    2017-06-15 22:44:10,536 - DEBUG - ImportWorker 3 stopped
    2017-06-15 22:44:10,620 - DEBUG - Import end

Troubleshooting
---------------

- An elevation value or a raster already imported won't be duplicated in the database if you run the load command a second time.
- In case you need to connect to postgres through an Unix socket, use ``-H ''`` as the command line ``host`` argument
