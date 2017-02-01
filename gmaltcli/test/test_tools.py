import os
import pytest

try:
    import mock
except ImportError:
    from unittest import mock

import gmaltcli.tools as tools
import gmaltcli.worker as worker


@pytest.fixture
def custom_small_path():
    return os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../datasets', 'small.json'))


@pytest.fixture
def custom_zip_path():
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'zip')


def test_dataset_file(custom_small_path):
    with pytest.raises(Exception) as e:
        tools.dataset_file('notfound')
    assert "Invalid dataset" in str(e.value)

    return_value = tools.dataset_file('small')
    assert return_value.endswith('small.json')

    return_value = tools.dataset_file(custom_small_path)
    assert return_value.endswith('small.json')


class TestLoadDatasetAction(object):
    def test_init_with_nargs(self):
        with pytest.raises(Exception) as e:
            tools.LoadDatasetAction(None, 'dataset', nargs='nargs')
        assert str(e.value) == "nargs not allowed in argument dataset"

    def test_call(self, custom_small_path):
        load_action = tools.LoadDatasetAction(None, 'dataset')

        namespace = type('', (), {})()
        load_action(None, namespace, custom_small_path)
        assert hasattr(namespace, 'dataset')
        assert namespace.dataset.endswith('small.json')

        assert hasattr(namespace, 'dataset_sampling')
        assert namespace.dataset_sampling == 1201

        assert hasattr(namespace, 'dataset_files')
        assert len(namespace.dataset_files) == 3


def test_download_hgt_zip_files(monkeypatch):
    mock_worker = mock.Mock()
    monkeypatch.setattr(worker, 'WorkerPool', mock_worker)

    # If skip, function exists immediately (mock not used)
    tools.download_hgt_zip_files('cwd', {'data': 'dict'}, 3, skip=True)
    assert mock_worker.call_count == 0

    # validate calls done on worker.WorkerPool
    tools.download_hgt_zip_files('cwd', {'data': 'dict'}, 3, skip=False)
    mock_worker.assert_has_calls([
        mock.call(worker.DownloadWorker, 3, 'cwd'),
        mock.call().fill({'data': 'dict'}),
        mock.call().start()
    ])


def test_extract_hgt_zip_files(monkeypatch, custom_zip_path):
    mock_worker = mock.Mock()
    monkeypatch.setattr(worker, 'WorkerPool', mock_worker)

    # If skip, function exists immediately (mock not used)
    tools.extract_hgt_zip_files(custom_zip_path, 3, skip=True)
    assert mock_worker.call_count == 0

    # validate calls done on worker.WorkerPool
    tools.extract_hgt_zip_files(custom_zip_path, 3, skip=False)
    mock_worker.assert_has_calls([
        mock.call(worker.ExtractWorker, 3, custom_zip_path),
        mock.call().fill([os.path.join(custom_zip_path, 'file1.zip')]),
        mock.call().start()
    ])
