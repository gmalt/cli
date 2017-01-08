import pytest

import gmaltcli.app as app


def test_create_read_from_hgt_parser(capsys):
    parser = app.create_read_from_hgt_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([])
    out, err = capsys.readouterr()
    assert 'too few arguments' in err \
           or 'the following arguments are required: lat, lng, hgt_file' in err  # python 3

    parsed = parser.parse_args(['43.9076', '2.9876', 'N00E010.hgt'])
    assert parsed.hgt_file == 'N00E010.hgt'
    assert parsed.lat == 43.9076
    assert parsed.lng == 2.9876

    with pytest.raises(SystemExit):
        parser.parse_args(['43.9076', '2.9876', 'N00E010.hgt', 'too much'])
    out, err = capsys.readouterr()
    assert 'unrecognized arguments: too much' in err
