import pytest

import gmaltcli.hgt as hgt


@pytest.fixture
def empty_hgt():
    return hgt.HgtParser('N00E010.hgt')


def test_get_bottom_left_center(empty_hgt):
    assert empty_hgt._get_bottom_left_center('N00E010.hgt') == (0.0, 10.0)
    assert empty_hgt._get_bottom_left_center('S20W03.hgt') == (-20.0, -3.0)
    assert empty_hgt._get_bottom_left_center('N01W001.hgt') == (1.0, -1.0)
    with pytest.raises(Exception) as e:
        empty_hgt._get_bottom_left_center('SF01AB001.hgt')
