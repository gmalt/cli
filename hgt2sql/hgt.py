import os
import re
import struct


class HgtParser(object):
    """ A tool to parse a HGT file

    It is intended to be used in a context manager::

        with HgtParser('myhgtfile.hgt') as parser:
            parser.get_elevation((lat, lng))

    :param str filepath: the path to the HGT file to parse
    :param int sample_lat: the number of values on the latitude axis
    :param int sample_lng: the number of values on the latitude axis
    """
    def __init__(self, filepath, sample_lat=1201, sample_lng=1201):
        self.file = None
        self.filepath = filepath

        self.sample_lat = sample_lat
        self.sample_lng = sample_lng

        self.filename = os.path.basename(filepath)
        self.bottom_left_center = self._get_bottom_left_center(self.filename)
        self.corners = self._get_corners_from_filename(self.bottom_left_center)

    def __enter__(self):
        if not os.path.exists(self.filepath):
            raise Exception('file {} not found'.format(self.filepath))
        self.file = open(self.filepath, 'rb')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()
            self.file = None

    @property
    def square_width(self):
        """ Provide the width (length on the longitude axis) of a square providing one elevation value

        :return: the width of a square with one elevation value
        :rtype: float
        """
        return 1.0 / (self.sample_lng - 1)

    @property
    def square_height(self):
        """ Provide the height (length on the latitude axis) of a square providing one elevation value

        :return: the height of a square with one elevation value
        :rtype: float
        """
        return 1.0 / (self.sample_lat - 1)

    @property
    def area_width(self):
        """ Provide the total width of the HGT file

        :return: the total width of the HGT file
        :rtype: float
        """
        return 1.0 + self.square_width

    @property
    def area_height(self):
        """ Provide the total height of the HGT file

        :return: the total height of the HGT file
        :rtype: float
        """
        return 1.0 + self.square_height

    @staticmethod
    def _get_bottom_left_center(filename):
        """ Extract the latitude and longitude of the center of the bottom left elevation
        square based on the filename

        :param str filename: name of the HGT file
        :return: tuple (latitude of the center of the bottom left square, longitude of the bottom left square)
        :rtype: tuple of float
        """
        filename_regex = re.compile('^([NS])([0-9]+)([WE])([0-9]+).*')
        result = filename_regex.match(filename)

        lat_order, lat_left_bottom_center, lng_order, lng_left_bottom_center = result.groups()

        lat_left_bottom_center = float(lat_left_bottom_center)
        lng_left_bottom_center = float(lng_left_bottom_center)
        if lat_order == 'S':
            lat_left_bottom_center *= -1
        if lng_order == 'W':
            lng_left_bottom_center *= -1

        return lat_left_bottom_center, lng_left_bottom_center

    def _get_corners_from_filename(self, bottom_left_corner):
        """ Based on the bottom left center latitude and longitude get the latitude and longitude of all the corner
         covered by the parsed HGT file

        :param tuple bottom_left_corner: position of the bottom left corner (lat, lng)
        :return: tuple of 4 position tuples (bottom left, top left, top right, bottom right) with (lat, lng) for each
        position as float
        :rtype: ((float, float), (float, float), (float, float), (float, float))
        """
        bottom_left = (bottom_left_corner[0] - self.square_height / 2, bottom_left_corner[1] - self.square_width / 2)
        top_left = (bottom_left[0] + self.area_height, bottom_left[1])
        top_right = (top_left[0], top_left[1] + self.area_width)
        bottom_right = (bottom_left[0], bottom_left[1] + self.area_width)

        return bottom_left, top_left, top_right, bottom_right

    def is_inside(self, point):
        """ Check if the point is inside the parsed HGT file

        :param tuple point: (lat, lng) of the point
        :return: True if the point is inside else False
        :rtype: bool
        """
        return \
            self.corners[0][0] < point[0] \
            and self.corners[0][1] < point[1] \
            and point[0] < self.corners[2][0] \
            and point[1] < self.corners[2][1]

    def _get_value(self, idx):
        """ Get the elevation value at the provided index

        :param int idx: index of the value
        :return: the elevation value
        :rtype: int
        """
        self.file.seek(0)
        self.file.seek(idx * 2)
        buf = self.file.read(2)
        val = struct.unpack('>h', buf)
        return val[0]

    def _get_idx_in_file(self, pos):
        """ From a position (lat, lng) as float. Get the index of the elevation value inside the HGT file

        :param tuple pos: (lat, lng) of the position
        :return: tuple (index on the latitude from the top, index on the longitude from the left, index in the file)
        :rtype: (int, int, int)
        """
        lat_idx = 1200 - int(round((pos[0] - self.bottom_left_center[0]) / self.square_height))
        lng_idx = int(round((pos[1] - self.bottom_left_center[1]) / self.square_width))
        idx = lat_idx * 1201 + lng_idx
        return lat_idx, lng_idx, idx

    def get_elevation(self, pos):
        """ Get the elevation for a position

        :param tuple pos: (lat, lng) of the position
        :return: tuple (index on the latitude from the top, index on the longitude from the left, elevation in meters)
        :rtype: (int, int, int)
        """
        if not self.is_inside(pos):
            raise Exception('point {} is not inside HGT file {}'.format(pos, self.filename))

        lat_idx, lng_idx, idx = self._get_idx_in_file(pos)

        return lat_idx, lng_idx, self._get_value(idx)
