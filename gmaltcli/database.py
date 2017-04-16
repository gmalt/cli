import logging

from future.utils import with_metaclass
from sqlalchemy import create_engine as sqlalchemy_create_engine
import sqlalchemy.engine.url as sql_url
import sqlalchemy.exc


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
    def __create_engine(type_, pool_size=1, **db_info):
        """ Create a sqlalchemy engine

        .. seealso:: supports all keywords arguments of constructor :class:`sqlalchemy.engine.url.URL`

        :param str type_: the type of engine (postgres, mysql)
        :param int pool_size: pool size of the engine (at least as much as the number of threads
            in :class:`gmaltcli.worker.WorkerPool`)
        :return: a sqlalchemy engine
        :rtype: :class:`sqlalchemy.engine.base.Engine`
        """
        uri = sql_url.URL(type_, **db_info)
        return sqlalchemy_create_engine(uri, pool_size=pool_size)

    def get_manager(self, use_raster=False):
        return Manager(self.db_driver, use_raster, self.engine.connect(), self.table_name)


class BaseManager(object):
    TYPE = None
    USE_RASTER = None

    TABLE_EXISTS_QUERY = None
    TABLE_CREATE_QUERY = None
    VALUE_EXIST_QUERY = None
    VALUE_CREATE_QUERY = None

    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def _execute(self, query, params=None, method='fetchall'):
        params = params if params is not None else {}
        params.update({'table_name': self.table_name})
        with self.connection.begin():
            result = self.connection.execute(query.format(**params), params)
            if result.returns_rows:
                return getattr(result, method)()
            else:
                return None

    def table_exists(self):
        return self._execute(self.TABLE_EXISTS_QUERY, method='scalar')

    def create_table(self):
        return self._execute(self.TABLE_CREATE_QUERY)

    def prepare_environment(self):
        if not self.is_compatible():
            raise sqlalchemy.exc.NotSupportedError('Database is not compatible with the provided settings')

        logging.debug('Database compatible with provided settings.')

        if not self.table_exists():
            logging.debug('Table {} not found. Creation in progress.'.format(self.table_name))
            self.create_table()
            logging.info('Table {} created.'.format(self.table_name))
        else:
            logging.debug('Table {} exists. Nothing to create.'.format(self.table_name))

    def is_compatible(self):
        return True

    def prepare_params(self, data, parser):
        raise Exception('to be implemented in child class')

    def insert_or_update(self, data, parser):
        # Don't import void elevation values
        elevation_value = data[4]
        if elevation_value is None:
            return

        params = self.prepare_params(data, parser)
        value_exists = self._execute(self.VALUE_EXIST_QUERY, params, method='scalar')
        if not value_exists:
            self._execute(self.VALUE_CREATE_QUERY, params, method='scalar')


class PostgresValueManager(with_metaclass(ManagerRegistry, BaseManager)):
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

    VALUE_EXIST_QUERY = ("SELECT 1"
                         "FROM   \"{table_name}\""
                         "WHERE  lat_min=%(lat_min)s"
                         "       AND lng_min=%(lng_min)s"
                         "       AND lat_max=%(lat_max)s"
                         "       AND lng_max=%(lng_max)s;")

    VALUE_CREATE_QUERY = ("INSERT INTO \"{table_name}\" (lat_min, lng_min, lat_max, lng_max, \"value\") "
                          "VALUES (%(lat_min)s, %(lng_min)s, %(lat_max)s, %(lng_max)s, %(value)s)")

    def prepare_params(self, data, parser):
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

    VALUE_EXIST_QUERY = ("SELECT 1"
                         "FROM   \"{table_name}\""
                         "WHERE  ST_Envelope(rast) = ST_GeomFromText('POLYGON((%(minx)s %(miny)s, %(minx)s %(maxy)s, %(maxx)s %(maxy)s, %(maxx)s %(miny)s, %(minx)s %(miny)s))', 4326);")  # noqa

    VALUE_CREATE_QUERY = ("INSERT INTO \"{table_name}\" (\"rast\") "
                          "VALUES (ST_SetValues("
                          "    ST_AddBand("
                          "        ST_MakeEmptyRaster(%(width)s, %(height)s, %(topleftx)s, %(toplefty)s, %(scalex)s, %(scaley)s, 0, 0, 4326),"  # noqa
                          "        '16BSI'::text, %(default_value)s, %(nodata_value)s"
                          "    ),"
                          "    1, 0, 0, %(elevation_values)s::double precision[][]"
                          "));")

    def is_compatible(self):
        return self._execute(self.POSTGIS_AVAILABLE_QUERY, method='scalar')

    def prepare_params(self, data, parser):
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
