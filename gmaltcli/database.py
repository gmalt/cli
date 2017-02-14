from sqlalchemy import create_engine as sqlalchemy_create_engine
import sqlalchemy.engine.url as sql_url


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
        print('here')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def prepare_environment(self):
        raise Exception('to be implemented in child class')

    def insert_or_update(self):
        raise Exception('to be implemented in child class')


class PostgresManager(BaseManager):
    __metaclass__ = ManagerRegistry
    TYPE = 'postgres'
    USE_RASTER = False

    def prepare_environment(self):
        pass

    def insert_or_update(self, values):
        pass


class PostgresRasterManager(BaseManager):
    __metaclass__ = ManagerRegistry
    TYPE = 'postgres'
    USE_RASTER = True

    def prepare_environment(self):
        pass

    def insert_or_update(self, values):
        pass
