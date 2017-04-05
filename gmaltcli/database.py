import logging

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

    def insert_or_update(self, data):
        raise Exception('to be implemented in child class')


class BaseValueManager(BaseManager):
    USE_RASTER = False

    def insert_or_update(self, data):
        params = {
            'lat_min': min([corner[0] for corner in data[3]]),
            'lat_max': max([corner[0] for corner in data[3]]),
            'lng_min': min([corner[1] for corner in data[3]]),
            'lng_max': max([corner[1] for corner in data[3]]),
        }
        value_exists = self._execute(self.VALUE_EXIST_QUERY, params, method='scalar')
        if not value_exists:
            params.update({'value': data[4]})
            self._execute(self.VALUE_CREATE_QUERY, params, method='scalar')


class PostgresValueManager(BaseValueManager):
    __metaclass__ = ManagerRegistry
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


class PostgresRasterManager(BaseManager):
    __metaclass__ = ManagerRegistry
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

    def is_compatible(self):
        return self._execute(self.POSTGIS_AVAILABLE_QUERY, method='scalar')