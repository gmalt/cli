import collections
import struct

import pytest

from . import tools as test_tools
import gmaltcli.hgt as hgt


@pytest.fixture
def empty_hgt():
    return hgt.HgtParser('N00E010.hgt')


@pytest.fixture
def srtm1_hgt():
    return hgt.HgtParser('N00E010.hgt', 3601, 3601)


def test_get_iterators(empty_hgt):
    assert isinstance(empty_hgt.get_value_iterator(), collections.Iterable)
    assert isinstance(empty_hgt.get_sample_iterator(50, 50), collections.Iterable)


def test_get_top_left_square(empty_hgt):
    corners = (
        (0.9995833333333332, 9.999583333333334),
        (1.0004166666666665, 9.999583333333334),
        (1.0004166666666665, 10.000416666666666),
        (0.9995833333333332, 10.000416666666666)
    )
    assert empty_hgt._get_top_left_square() == corners


def test_shift_first_square(empty_hgt):
    corners = (
        (0.9895833333333331, 9.999583333333334),
        (0.9904166666666665, 9.999583333333334),
        (0.9904166666666665, 10.000416666666666),
        (0.9895833333333331, 10.000416666666666)
    )
    assert empty_hgt.shift_first_square(12, 0) == corners

    corners = (
        (0.9995833333333332, 10.009583333333333),
        (1.0004166666666665, 10.009583333333333),
        (1.0004166666666665, 10.010416666666666),
        (0.9995833333333332, 10.010416666666666)
    )
    assert empty_hgt.shift_first_square(0, 12) == corners

    corners = (
        (0.8604166666666665, 10.89625),
        (0.8612499999999998, 10.89625),
        (0.8612499999999998, 10.897083333333333),
        (0.8604166666666665, 10.897083333333333)
    )
    assert empty_hgt.shift_first_square(167, 1076) == corners

    with pytest.raises(Exception) as e:
        empty_hgt.shift_first_square(-5, 0)
    assert str(e.value) == "Out of bound line or col"
    with pytest.raises(Exception) as e:
        empty_hgt.shift_first_square(56, 1201)
    assert str(e.value) == "Out of bound line or col"


def test_square_width_height(empty_hgt, srtm1_hgt):
    assert empty_hgt.square_width == 0.0008333333333333334
    assert empty_hgt.square_height == 0.0008333333333333334

    assert srtm1_hgt.square_width == 0.0002777777777777778
    assert srtm1_hgt.square_height == 0.0002777777777777778

    random_hgt = hgt.HgtParser('N00E010.hgt', 1201, 3601)
    assert random_hgt.square_width == 0.0002777777777777778
    assert random_hgt.square_height == 0.0008333333333333334


def test_area_width_height(empty_hgt, srtm1_hgt):
    assert empty_hgt.area_width == 1.0008333333333332
    assert empty_hgt.area_height == 1.0008333333333332

    assert srtm1_hgt.area_width == 1.0002777777777778
    assert srtm1_hgt.area_height == 1.0002777777777778

    random_hgt = hgt.HgtParser('N00E010.hgt', 1201, 3601)
    assert random_hgt.area_width == 1.0002777777777778
    assert random_hgt.area_height == 1.0008333333333332


def test_get_bottom_left_center(empty_hgt):
    assert empty_hgt._get_bottom_left_center('N00E010.hgt') == (0.0, 10.0)
    assert empty_hgt._get_bottom_left_center('S20W03.hgt') == (-20.0, -3.0)
    assert empty_hgt._get_bottom_left_center('N01W001.hgt') == (1.0, -1.0)
    with pytest.raises(Exception) as e:
        empty_hgt._get_bottom_left_center('SF01AB001.hgt')


def test_get_corners_from_filename(empty_hgt, srtm1_hgt):
    corners = (
        (-0.0004166666666666667, 9.999583333333334),
        (1.0004166666666665, 9.999583333333334),
        (1.0004166666666665, 11.000416666666666),
        (-0.0004166666666666667, 11.000416666666666)
    )
    assert empty_hgt._get_corners_from_filename((0.0, 10.0)) == corners

    corners = (
        (-0.0001388888888888889, 9.99986111111111),
        (1.000138888888889, 9.99986111111111),
        (1.000138888888889, 11.000138888888888),
        (-0.0001388888888888889, 11.000138888888888)
    )
    assert srtm1_hgt._get_corners_from_filename((0.0, 10.0)) == corners


def test_is_inside(empty_hgt):
    assert empty_hgt.is_inside((0.5, 10.5))
    assert empty_hgt.is_inside((0.1, 10.9))

    assert not empty_hgt.is_inside((1.5, 10.9))
    assert not empty_hgt.is_inside((0.5, 9.8))


def test_get_idx(empty_hgt, srtm1_hgt):
    assert empty_hgt.get_idx(5, 1200) == 1441205
    assert empty_hgt.get_idx(1200, 5) == 7205

    with pytest.raises(Exception) as e:
        assert empty_hgt.get_idx(-5, 1200)
    assert str(e.value) == "Out of bound line or col"
    with pytest.raises(Exception) as e:
        assert empty_hgt.get_idx(5, 1201)
    assert str(e.value) == "Out of bound line or col"

    assert srtm1_hgt.get_idx(5, 1200) == 4321205
    assert srtm1_hgt.get_idx(1200, 5) == 19205


def test_get_value(monkeypatch, empty_hgt):
    opened_file = test_tools.MockOpenedFile(struct.pack('>h', 156))
    monkeypatch.setattr(empty_hgt, 'file', opened_file)
    alt_value = empty_hgt.get_value(7205)

    assert opened_file.seek_values == [0, 14410]
    assert opened_file.buf_values == [2]
    assert alt_value == 156

    opened_file.clean()
    opened_file.value = struct.pack('>h', -32768)
    alt_value = empty_hgt.get_value(7205)
    assert alt_value is None


def test_get_idx_in_file(empty_hgt):
    with pytest.raises(Exception) as e:
        empty_hgt.get_idx_in_file((3.0, 12))
    assert str(e.value) == "point (3.0, 12) is not inside HGT file N00E010.hgt"

    assert empty_hgt.get_idx_in_file((0.56, 10.86)) == (528, 1032, 635160)
