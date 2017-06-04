# -*- coding: utf-8 -*-
import logging

from future.utils import with_metaclass
from sqlalchemy import create_engine as sqlalchemy_create_engine
import sqlalchemy.engine.url as sql_url
import sqlalchemy.exc


class NotSupportedException(sqlalchemy.exc.SQLAlchemyError):
    """ Exception raised if database does not support the provided settings. Most probably because
    no GIS extension has been enabled """
    pass


class ManagerRegistry(type):
    """ Python Registry pattern to store all manager.

    .. note:: A manager extends :class:`gmaltcli.database.BaseManager` to insert elevation data
        into the database. A database driver may have multiple manager if gmalt supports multiple
        schema for this one.
    """
    REGISTRY = {}

    def __new__(cls, *args, **kwargs):
        new_cls = type.__new__(cls, *args, **kwargs)
        cls.REGISTRY[(new_cls.TYPE, new_cls.USE_RASTER)] = new_cls
        return new_cls

    @staticmethod
    def get_manager_class(db_driver, use_raster):
        """ Get a manager class matching the database driver and the model (raster support or not)

        :param str db_driver: the database drive
        :param bool use_raster: True if the manager must be of raster type (GIS extension in database)
        :return: :class:`gmaltcli.database.BaseManager`
        """
        if (db_driver, use_raster) not in ManagerRegistry.REGISTRY:
            raise Exception('Unknown database driver {}'.format(db_driver))
        return ManagerRegistry.REGISTRY[(db_driver, use_raster)]


class Manager(object):
    def __new__(cls, db_driver, use_raster, *args, **kwargs):
        """ Not really useful but I wanted to try a class which returns an object of another
        type on instantiation.

        Here it uses the :class:`gmaltcli.database.ManagerRegistry` to return the right manager when
        developer instantiates the Manager.

        :return: a manager object
        :rtype: :class:`gmaltcli.database.BaseManager`
        """
        return ManagerRegistry.get_manager_class(db_driver, use_raster)(*args, **kwargs)


class ManagerFactory(object):
    """ This class provides a factory of :class:`gmaltcli.database.BaseManager`

    .. seealso: :func:`gmaltcli.database.ManagerBuilder.__create_engine` for details on constructor args
    """
    def __init__(self, type_, table_name, pool_size=1, **db_info):
        self.db_driver = type_
        self.table_name = table_name
        self.engine = self.__create_engine(type_, pool_size=pool_size, **db_info)

    @staticmethod
    def __create_engine(type_, pool_size=1, debug=False, **db_info):
        """ Create a sqlalchemy engine

        .. seealso:: supports all keywords arguments of constructor :class:`sqlalchemy.engine.url.URL`

        :param str type_: the type of engine (postgres, mysql)
        :param int pool_size: pool size of the engine (at least as much as the number of threads
            in :class:`gmaltcli.worker.WorkerPool`)
        :param bool debug: Enable echo parameters of sqlalchemy engine
        :return: a sqlalchemy engine
        :rtype: :class:`sqlalchemy.engine.base.Engine`
        """
        uri = sql_url.URL(type_, **db_info)
        return sqlalchemy_create_engine(uri, pool_size=pool_size, echo=debug)

    def get_manager(self, use_raster=False):
        return Manager(self.db_driver, use_raster, self.engine, self.table_name)


class BaseManager(object):
    """ Base class to manage insertion of elevation value

    .. note:: child class needs to define the different queries and the `prepare_params`
        method (which provides the parameters for the queries)

    .. note:: manager object needs to be accessed using a context manager

    :param engine: a sqlalchemy engine
    :type engine: :class:`sqlalchemy.engine.base.Engine`
    :param str table_name: the name of the table to store the elevation value
    """
    TYPE = None
    USE_RASTER = None

    TABLE_EXISTS_QUERY = None
    TABLE_CREATE_QUERY = None
    VALUE_EXIST_QUERY = None
    VALUE_CREATE_QUERY = None

    def __init__(self, engine, table_name):
        self.engine = engine
        self.table_name = table_name
        self.connection = None

    def __enter__(self):
        if not self.connection:
            self.connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

    def _execute(self, connection, query, params=None, method='fetchall'):
        """ Execute the SQL `query` with the binded `params` and call the `method` on the result cursor

        .. warning:: query not executed in a context manager so you have to close the connection outside

        :param connection: a sqlalchemy connection object
        :type connection: :class:`sqlalchemy.engine.base.Connection`
        :param str query: the SQL query to execute
        :param dict params: dict of values to bind to the query
        :param str method: the method to call on the
            sqlalchemy result cursor (see method from :class:`sqlalchemy.engine.ResultProxy`)
        :return: the result of the query or None
        """
        params = params or {}
        params.update({'table_name': self.table_name})
        # print(conn.connection.cursor().mogrify(query.format(**params), params))
        result = connection.execute(query.format(**params), params)
        if result.returns_rows:
            return getattr(result, method)()
        else:
            return None

    def execute(self, query, params=None, method='fetchall'):
        """ Execute the SQL `query` with the binded `params` inside a transaction

        .. note:: the class should be used as a context manager to have a connection opened

        ..seealso:: :func:`gmaltcli.database.BaseManager._execute` for params description
        """
        with self.connection.begin():
            return self._execute(self.connection, query, params=params, method=method)

    def table_exists(self):
        """ Execute the `TABLE_EXISTS_QUERY` query

        :return: 1 if table exists else None
        :rtype: int
        """
        return self.execute(self.TABLE_EXISTS_QUERY, method='scalar')

    def create_table(self):
        """ Execute the `TABLE_CREATE_QUERY` query

        :return: None
        """
        return self.execute(self.TABLE_CREATE_QUERY)

    def prepare_environment(self):
        """ Check if the database is compatible with the chosen format of the elevation data and create the table to
        store these data if it does not exist
        """
        if not self.is_compatible():
            raise NotSupportedException('Database is not compatible with the provided settings')

        logging.debug('Database compatible with provided settings.')

        if not self.table_exists():
            logging.debug('Table {} not found. Creation in progress.'.format(self.table_name))
            self.create_table()
            logging.info('Table {} created.'.format(self.table_name))
        else:
            logging.debug('Table {} exists. Nothing to create.'.format(self.table_name))

    def is_compatible(self):
        """ Override in child class to check if the database if compatible with the chosen format of elevation
        data (for example, you can check if the PostGIS extension is installed for raster data on PostgreSQL

        :return: True if compatible else False
        :rtype: bool
        """
        return True

    def prepare_params(self, data, parser):
        """ Prepare params for SELECT (to check if these elevations data have already been import) or INSERT queries

        .. note:: see implementation in child class

        :param data: elevation data (polygon, elevation)
        :type data: tuple
        :param parser: the HGT parser
        :type parser: :class:`gmalthgtparser.HgtParser`
        :return: dict with the params for both queries
        :rtype: dict
        """
        raise Exception('to be implemented in child class')

    def insert_data(self, data, parser):
        """ Insert elevation data if they don't exist in the table yet

        :param data: data coming from a HGT iterator (:class:`gmalthgtparser.HgtSampleIterator`
            or :class:`gmalthgtparser.HgtValueIterator`)
        :type data: tuple
        :param parser: the HGT parser used to get the data. Passed in this method because some database manager needs
            generic information about the parsed file that are stored in the parser
        :rtype: :class:`gmalthgtparser.HgtParser`
        """
        # Don't import void elevation values
        elevation_value = data[4]
        if elevation_value == parser.VOID_VALUE:
            return

        params = self.prepare_params(data, parser)
        value_exists = self.execute(self.VALUE_EXIST_QUERY, params, method='scalar')
        if not value_exists:
            self.execute(self.VALUE_CREATE_QUERY, params, method='scalar')


class PostgresValueManager(with_metaclass(ManagerRegistry, BaseManager)):
    """ Provides SQL queries to import elevation value in a PostgreSQL table WITHOUT PostGIS """
    TYPE = 'postgres'
    USE_RASTER = False

    TABLE_EXISTS_QUERY = ("SELECT EXISTS("
                          "    SELECT  1"
                          "    FROM    information_schema.tables"
                          "    WHERE   table_name=%(table_name)s"
                          ")")

    TABLE_CREATE_QUERY = ("CREATE TABLE \"{table_name}\" ("
                          "    lat_min DOUBLE PRECISION,"
                          "    lng_min DOUBLE PRECISION,"
                          "    lat_max DOUBLE PRECISION,"
                          "    lng_max DOUBLE PRECISION,"
                          "    \"value\" SMALLINT,"
                          "    PRIMARY KEY (lat_min, lng_min, lat_max, lng_max)"
                          ");")

    VALUE_EXIST_QUERY = ("SELECT 1 "
                         "FROM   \"{table_name}\" "
                         "WHERE  lat_min=%(lat_min)s"
                         "       AND lng_min=%(lng_min)s"
                         "       AND lat_max=%(lat_max)s"
                         "       AND lng_max=%(lng_max)s;")

    VALUE_CREATE_QUERY = ("INSERT INTO \"{table_name}\" (lat_min, lng_min, lat_max, lng_max, \"value\") "
                          "VALUES (%(lat_min)s, %(lng_min)s, %(lat_max)s, %(lng_max)s, %(value)s)")

    def prepare_params(self, data, parser):
        """
        .. seealso:: :func:`gmaltcli.database.BaseManager.prepare_params`
        """
        area_corners = data[3]
        elevation_value = data[4]

        return {
            'lat_min': min([corner[0] for corner in area_corners]),
            'lat_max': max([corner[0] for corner in area_corners]),
            'lng_min': min([corner[1] for corner in area_corners]),
            'lng_max': max([corner[1] for corner in area_corners]),
            'value': elevation_value
        }


class PostgresRasterManager(with_metaclass(ManagerRegistry, BaseManager)):
    """ Provides SQL queries to import elevation value in a PostgreSQL table WITH PostGIS """
    TYPE = 'postgres'
    USE_RASTER = True

    TABLE_EXISTS_QUERY = ("SELECT EXISTS("
                          "    SELECT  1"
                          "    FROM    information_schema.tables"
                          "    WHERE   table_name=%(table_name)s"
                          ")")

    TABLE_CREATE_QUERY = ("CREATE TABLE \"{table_name}\" (\"rid\" serial PRIMARY KEY,\"rast\" raster);"
                          "CREATE INDEX \"{table_name}_rast_gist_idx\" "
                          "ON           \"{table_name}\" "
                          "USING gist   (st_convexhull(\"rast\"));")

    POSTGIS_AVAILABLE_QUERY = ("SELECT EXISTS("
                               "    SELECT  1"
                               "    FROM    pg_extension"
                               "    WHERE   extname='postgis'"
                               ")")

    VALUE_EXIST_QUERY = ("SELECT 1 "
                         "FROM   \"{table_name}\" "
                         "WHERE  ST_Envelope(rast) = ST_GeomFromText('POLYGON((%(minx)s %(miny)s, %(minx)s %(maxy)s, %(maxx)s %(maxy)s, %(maxx)s %(miny)s, %(minx)s %(miny)s))', 4326);")  # noqa

    VALUE_CREATE_QUERY = ("INSERT INTO \"{table_name}\" (\"rast\") "
                          "VALUES (ST_SetValues("
                          "    ST_AddBand("
                          "        ST_MakeEmptyRaster(%(width)s, %(height)s, %(topleftx)s, %(toplefty)s, %(scalex)s, %(scaley)s, 0, 0, 4326),"  # noqa
                          "        '16BSI'::text, %(default_value)s, %(nodata_value)s"
                          "    ),"
                          "    1, 1, 1, %(elevation_values)s::double precision[][]"
                          "));")

    def is_compatible(self):
        """ Execute query to check if the postgis extension is enabled

        :return: 1 if the extension is activated else None
        :rtype: int or None
        """
        return self.execute(self.POSTGIS_AVAILABLE_QUERY, method='scalar')

    def prepare_params(self, data, parser):
        """
        .. seealso:: :func:`gmaltcli.database.BaseManager.prepare_params`
        """
        elevation_values = data[4]

        area_corners = data[3]
        top_left_corner = area_corners[1]
        bottom_right_corner = area_corners[3]

        return {
            # Used in INSERT query
            'width': len(elevation_values[0]),
            'height': len(elevation_values),
            'topleftx': top_left_corner[1],
            'toplefty': top_left_corner[0],
            'scalex': float(parser.square_width),
            'scaley': -1 * float(parser.square_height),  # raster descending on latitude (line per line)
            'default_value': 0,
            'nodata_value': parser.VOID_VALUE,
            'elevation_values': elevation_values,

            # Used in SELECT query
            'minx': top_left_corner[1],
            'miny': bottom_right_corner[0],
            'maxx': bottom_right_corner[1],
            'maxy': top_left_corner[0]
        }
