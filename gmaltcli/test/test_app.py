import json

import pytest

import gmaltcli.app as app


def test_create_read_from_hgt_parser_too_few_args(capsys):
    parser = app.create_read_from_hgt_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err \
           or 'the following arguments are required: lat, lng, hgt_file' in err  # python 3


def test_create_read_from_hgt_parser_too_much_args(capsys):
    parser = app.create_read_from_hgt_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['43.9076', '2.9876', 'N00E010.hgt', 'too much'])
    out, err = capsys.readouterr()
    assert 'unrecognized arguments: too much' in err


def test_create_read_from_hgt_parser_all_args():
    parser = app.create_read_from_hgt_parser()
    parsed = parser.parse_args(['43.9076', '2.9876', 'N00E010.hgt'])
    assert parsed.hgt_file == 'N00E010.hgt'
    assert parsed.lat == 43.9076
    assert parsed.lng == 2.9876


def test_create_get_hgt_parser_too_few_args(capsys):
    parser = app.create_get_hgt_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err \
           or 'the following arguments are required: dataset, folder' in err  # python 3


def test_create_get_hgt_too_much_args(capsys, tmpdir):
    tmp_working_dir = tmpdir.mkdir("working_dir")
    parser = app.create_get_hgt_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['small', str(tmp_working_dir), 'too much'])
    out, err = capsys.readouterr()
    assert 'unrecognized arguments: too much' in err


def test_create_get_hgt_parser_minimal_args(tmpdir):
    tmp_working_dir = tmpdir.mkdir("working_dir")
    parser = app.create_get_hgt_parser()
    parsed = parser.parse_args(['small', str(tmp_working_dir)])
    assert parsed.concurrency == 1
    assert parsed.dataset.endswith('gmaltcli/datasets/small.json')
    assert len(parsed.dataset_files) == 3
    assert parsed.dataset_sampling == 1201
    assert parsed.folder.endswith('working_dir')
    assert not parsed.skip_download
    assert not parsed.skip_unzip
    assert not parsed.verbose


def test_create_get_hgt_parser_all_args(tmpdir):
    tmp_working_dir = tmpdir.mkdir("working_dir")
    parser = app.create_get_hgt_parser()
    parsed = parser.parse_args(['small', str(tmp_working_dir), '--skip-download', '--skip-unzip', '-v', '-c 2'])
    assert parsed.concurrency == 2
    assert parsed.dataset.endswith('gmaltcli/datasets/small.json')
    assert len(parsed.dataset_files) == 3
    assert parsed.dataset_sampling == 1201
    assert parsed.folder.endswith('working_dir')
    assert parsed.skip_download
    assert parsed.skip_unzip
    assert parsed.verbose


def test_create_get_hgt_parser_dataset_as_file(tmpdir):
    false_dataset = {'sampling': 321,
                     'files': {
                        'file1.hgt': {
                            'url': 'http://my.url.fr/file1.hgt.zip',
                            'zip': 'file1.hgt.zip'
                        }}}
    tmp_dataset = tmpdir.mkdir("dataset").join("customset.json")
    tmp_dataset.write(json.dumps(false_dataset))
    tmp_working_dir = tmpdir.mkdir("working_dir")

    parser = app.create_get_hgt_parser()
    parsed = parser.parse_args([str(tmp_dataset), str(tmp_working_dir)])
    assert parsed.dataset.endswith('dataset/customset.json')
    assert len(parsed.dataset_files) == 1
    assert parsed.dataset_sampling == 321
    assert parsed.folder.endswith('working_dir')


def test_create_load_hgt_parser_too_few_args(capsys):
    parser = app.create_load_hgt_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err \
           or 'the following arguments are required: folder, -u/--user' in err  # python 3


def test_create_load_hgt_parser_too_much_args(capsys, tmpdir):
    tmp_working_dir = tmpdir.mkdir("working_dir")
    parser = app.create_load_hgt_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['-u', 'user', str(tmp_working_dir), 'too much'])
    out, err = capsys.readouterr()
    assert 'unrecognized arguments: too much' in err


def test_create_load_hgt_parser_min_args(tmpdir):
    tmp_working_dir = tmpdir.mkdir("working_dir")
    parser = app.create_load_hgt_parser()
    parsed = parser.parse_args(['-u', 'gmalt', str(tmp_working_dir)])
    assert parsed.concurrency == 1
    assert parsed.database == 'gmalt'
    assert parsed.folder == str(tmp_working_dir)
    assert parsed.host == 'localhost'
    assert parsed.password is None
    assert parsed.port is None
    assert parsed.sample is None
    assert parsed.table == 'elevation'
    assert parsed.type == 'postgres'
    assert parsed.use_raster is False
    assert parsed.username == 'gmalt'
    assert parsed.verbose is False
    assert parsed.traceback is False


def test_create_load_hgt_parser_all_args(tmpdir):
    tmp_working_dir = tmpdir.mkdir("working_dir")
    parser = app.create_load_hgt_parser()
    parsed = parser.parse_args(['-c', '2', '-v', '-tb', '--type', 'mysql', '-H', 'db.local', '-P', '3306',
                                '-d', 'elev_db', '-u', 'gmalt', '-p', 'password', '-t', 'elev_tb', '-r', '-s',
                                '3601', '3601', str(tmp_working_dir)])
    assert parsed.concurrency == 2
    assert parsed.database == 'elev_db'
    assert parsed.folder == str(tmp_working_dir)
    assert parsed.host == 'db.local'
    assert parsed.password == 'password'
    assert parsed.port == 3306
    assert parsed.sample == [3601, 3601]
    assert parsed.table == 'elev_tb'
    assert parsed.type == 'mysql'
    assert parsed.use_raster is True
    assert parsed.username == 'gmalt'
    assert parsed.verbose is True
    assert parsed.traceback is True
