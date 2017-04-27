import pytest
import sqlalchemy

import gmaltcli.database as database
import gmaltcli.tests.tools as tools


def test_manager_registry_get_manager_class():
    postgres_standard = database.ManagerRegistry.get_manager_class('postgres', False)
    assert postgres_standard is database.PostgresValueManager

    postgres_raster = database.ManagerRegistry.get_manager_class('postgres', True)
    assert postgres_raster is database.PostgresRasterManager

    with pytest.raises(Exception) as e:
        database.ManagerRegistry.get_manager_class('couchdb', True)
    assert str(e.value) == "Unknown database driver couchdb"


def test_manager_constructor():
    postgres_standard = database.Manager('postgres', False, 'connection', 'table_name')
    assert isinstance(postgres_standard, database.PostgresValueManager)
    assert postgres_standard.connection == 'connection'
    assert postgres_standard.table_name == 'table_name'

    postgres_raster = database.Manager('postgres', True, 'connection', 'table_name')
    assert isinstance(postgres_raster, database.PostgresRasterManager)
    assert postgres_raster.connection == 'connection'
    assert postgres_raster.table_name == 'table_name'


def test_manager_factory_constructor_call_private_create_engine_method(monkeypatch):
    mock_callable = tools.MockCallable()

    def mockreturn(*args, **kwargs):
        return mock_callable(*args, **kwargs)
    monkeypatch.setattr(database.ManagerFactory, '_ManagerFactory__create_engine', mockreturn)

    database.ManagerFactory('postgres', 'table_name', pool_size=5, host='localhost', port=3306)
    assert mock_callable.args[1:] == ('postgres',)
    assert mock_callable.kwargs == {'host': 'localhost', 'port': 3306, 'pool_size': 5}


def test_manager_factory_constructor():
    factory = database.ManagerFactory('postgres', 'table_name')
    assert isinstance(factory.engine, sqlalchemy.engine.Engine)
    assert factory.db_driver == 'postgres'
    assert factory.table_name == 'table_name'


def test_manager_factory_get_manager(monkeypatch):
    def mockreturn(*args, **kwargs):
        return 'connection'
    monkeypatch.setattr(sqlalchemy.engine.Engine, 'connect', mockreturn)

    factory = database.ManagerFactory('postgres', 'table_name')
    postgres_standard = factory.get_manager(use_raster=False)
    assert isinstance(postgres_standard, database.PostgresValueManager)
    assert postgres_standard.connection == 'connection'
    assert postgres_standard.table_name == 'table_name'

    postgres_raster = factory.get_manager(use_raster=True)
    assert isinstance(postgres_raster, database.PostgresRasterManager)
    assert postgres_raster.connection == 'connection'
    assert postgres_raster.table_name == 'table_name'

    with pytest.raises(Exception) as e:
        factory = database.ManagerFactory('couchdb', 'table_name')
        factory.get_manager()
    assert str(e.value) == "Can't load plugin: sqlalchemy.dialects:couchdb"


def test_base_manager_prepare_environment_not_compatible(monkeypatch):
    is_compatible_mock = tools.MockCallable()

    def mockreturn(*args, **kwargs):
        return is_compatible_mock(*args, **kwargs)
    monkeypatch.setattr(database.BaseManager, 'is_compatible', mockreturn)

    with pytest.raises(database.NotSupportedException) as e:
        manager = database.BaseManager('connection', 'table_name')
        manager.prepare_environment()
    assert str(e.value) == 'Database is not compatible with the provided settings'


def test_base_manager_prepare_environment_table_exists(monkeypatch):
    def mockreturn(*args, **kwargs):
        return True
    monkeypatch.setattr(database.BaseManager, 'is_compatible', mockreturn)
    monkeypatch.setattr(database.BaseManager, 'table_exists', mockreturn)

    def mockreturn_not_called(*args, **kwargs):
        raise Exception('not called')
    monkeypatch.setattr(database.BaseManager, 'create_table', mockreturn_not_called)

    manager = database.BaseManager('connection', 'table_name')
    assert manager.prepare_environment() is None


def test_base_manager_prepare_environment_create_table(monkeypatch):
    def mockreturn_is_compatible(*args, **kwargs):
        return True
    monkeypatch.setattr(database.BaseManager, 'is_compatible', mockreturn_is_compatible)

    def mockreturn_table_exists(*args, **kwargs):
        return False
    monkeypatch.setattr(database.BaseManager, 'table_exists', mockreturn_table_exists)

    create_table_mock = tools.MockCallable()

    def mockreturn_create_table(*args, **kwargs):
        return create_table_mock(*args, **kwargs)
    monkeypatch.setattr(database.BaseManager, 'create_table', mockreturn_create_table)

    manager = database.BaseManager('connection', 'table_name')
    manager.prepare_environment()
    assert create_table_mock.called is True
    assert create_table_mock.args[1:] == tuple()
    assert create_table_mock.kwargs == dict()


def test_postgres_value_manager_prepare_params():
    manager = database.PostgresValueManager('connection', 'table_name')
    return_value = manager.prepare_params(
        ('notused1', 'notused2', 'notused2', [(2, 9), (5, 12), (8, 6), (15, 11)], 456),
        'notused3'
    )
    assert return_value == {'lat_max': 15, 'lat_min': 2, 'lng_max': 12, 'lng_min': 6, 'value': 456}


def test_postgres_raster_manager_prepare_params():
    mock_parser = type('test', (object,), {})()
    mock_parser.square_width = 5.6
    mock_parser.square_height = 8.7
    mock_parser.VOID_VALUE = -36543

    manager = database.PostgresRasterManager('connection', 'table_name')
    return_value = manager.prepare_params(
        ('notused1', 'notused2', 'notused2', [(2, 9), (5, 12), (8, 6), (15, 11)], [[456, 87, 65], [12, 54]]),
        mock_parser
    )
    assert return_value == {'default_value': 0, 'elevation_values': [[456, 87, 65], [12, 54]], 'height': 2,
                            'maxx': 11, 'maxy': 5, 'minx': 12, 'miny': 15, 'nodata_value': -36543, 'scalex': 5.6,
                            'scaley': -8.7, 'topleftx': 12, 'toplefty': 5, 'width': 3}
