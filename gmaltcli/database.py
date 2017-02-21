import logging

from sqlalchemy import create_engine as sqlalchemy_create_engine
import sqlalchemy.engine.url as sql_url
import sqlalchemy.exc


def create_engine(type_, pool_size=1, **db_info):
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


class ManagerRegistry(type):
    """ Python Registry pattern to store all manager.
    
    .. note:: A manager extends :class:`gmaltcli.database.BaseManager` to insert elevation data 
        into the database. A database driver may have multiple manager if gmalt supports multiple
        schema for this one.
    """
    REGISTRY = {}

    def __new__(cls, *args, **kwargs):
        # new_cls = type.__new__(cls, *args, **kwargs)
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
        type on instanciation.
        
        Here it uses the :class:`gmaltcli.database.ManagerRegistry` to return the right manager when
        developer instanciates the Manager.
        
        :return: a manager object
        :rtype: :class:`gmaltcli.database.BaseManager`
        """
        return ManagerRegistry.get_manager_class(db_driver, use_raster)(*args, **kwargs)


class BaseManager(object):
    TYPE = None

    def __init__(self, engine, table_name):
        self.connection = engine.connect()
        self.table_name = table_name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def _execute(self, query, params=None, method='fetchall'):
        params = params if params is not None else {}
        with self.connection.begin():
            result = self.connection.execute(query.format(**params), params)
            if result.returns_rows:
                return getattr(result, method)()
            else:
                return None

    def table_exists(self):
        return self._execute(self.TABLE_EXISTS_QUERY, {'table_name': self.table_name}, method='scalar')

    def create_table(self):
        return self._execute(self.TABLE_CREATE_QUERY, {'table_name': self.table_name})

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

    def insert_or_update(self):
        raise Exception('to be implemented in child class')


class BasePostgresManager(BaseManager):
    TYPE = 'postgres'

    TABLE_EXISTS_QUERY = """ 
          SELECT EXISTS(
            SELECT  1 
            FROM    information_schema.tables 
            WHERE   table_name=%(table_name)s
          ) """


class PostgresManager(BasePostgresManager):
    __metaclass__ = ManagerRegistry
    USE_RASTER = False

    TABLE_CREATE_QUERY = """
            CREATE TABLE "{table_name}" (
                lat_min DOUBLE PRECISION,
                lng_min DOUBLE PRECISION, 
                lat_max DOUBLE PRECISION,
                lng_max DOUBLE PRECISION,
                "value" SMALLINT,
                PRIMARY KEY (lat_min, lng_min, lat_max, lng_max)
            ); """


class PostgresRasterManager(BasePostgresManager):
    __metaclass__ = ManagerRegistry
    USE_RASTER = True

    TABLE_CREATE_QUERY = """
            CREATE TABLE "{table_name}" ("rid" serial PRIMARY KEY,"rast" raster);
            CREATE INDEX "{table_name}_rast_gist_idx" ON "{table_name}" USING gist (st_convexhull("rast"));
        """

    POSTGIS_AVAILABLE_QUERY = """
            SELECT EXISTS(
                SELECT  1
                FROM    pg_extension 
                WHERE   extname='postgis'
              ) """

    def is_compatible(self):
        return self._execute(self.POSTGIS_AVAILABLE_QUERY, method='scalar')