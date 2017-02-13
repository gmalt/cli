from sqlalchemy import create_engine
import sqlalchemy.engine.url as sql_url


class DatabaseRegistry(type):
    REGISTRY = {}

    def __new__(cls, *args, **kwargs):
        # new_cls = type.__new__(cls, *args, **kwargs)
        new_cls = type.__new__(cls, *args, **kwargs)
        cls.REGISTRY[(new_cls.TYPE, new_cls.USE_RASTER)] = new_cls
        return new_cls

    @staticmethod
    def get_db_class(db_type, use_raster):
        if (db_type, use_raster) not in DatabaseRegistry.REGISTRY:
            raise Exception('Unknown database type {}'.format(db_type))
        return DatabaseRegistry.REGISTRY[(db_type, use_raster)]


class Database(object):
    def __new__(cls, db_type, use_raster, *args, **kwargs):
        """ 
        :return: a database object
        :rtype: :class:`gmaltcli.database.BaseDatabase`
        """
        return DatabaseRegistry.get_db_class(db_type, use_raster)(*args, **kwargs)


class BaseDatabase(object):
    TYPE = None

    def __init__(self, table, pool_size=1, **db_info):
        self.uri = sql_url.URL(self.TYPE, **db_info)
        self.engine = create_engine(self.uri, pool_size=pool_size)
        self.table = table

    def is_valid(self):
        raise Exception('to be implemented in child class')

    def insert_or_update(self):
        raise Exception('to be implemented in child class')


class PostgresDatabase(BaseDatabase):
    __metaclass__ = DatabaseRegistry
    TYPE = 'postgres'
    USE_RASTER = False

    def is_valid(self):
        pass

    def insert_or_update(self, values):
        pass


class PostgresRasterDatabase(BaseDatabase):
    __metaclass__ = DatabaseRegistry
    TYPE = 'postgres'
    USE_RASTER = True

    def is_valid(self):
        pass

    def insert_or_update(self, values):
        pass
