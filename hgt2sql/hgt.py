import os
import re


class HgtParser(object):
    def __init__(self, fd):
        if not hasattr(fd, 'read') or not hasattr(fd, 'name'):
            raise Exception('HgtParser needs an opened file as input')
        self.file = fd

        self.sample_lat = 1201
        self.sample_lng = 1201

        self.filename = os.path.basename(fd.name)
        self.bottom_left_center = self._get_bottom_left_center(self.filename)
        self.corners = self._get_corners_from_filename(self.bottom_left_center)
        print(self.corners)

    @property
    def square_width(self):
        return 1.0 / self.sample_lng

    @property
    def square_heigth(self):
        return 1.0 / self.sample_lat

    @property
    def area_width(self):
        return 1.0 + (2 * self.square_width)

    @property
    def area_heigth(self):
        return 1.0 + (2 * self.square_heigth)

    @staticmethod
    def init_from_filepath(filepath):
        if not os.path.exists(filepath):
            raise Exception('file {} does not exists'.format(filepath))
        return HgtParser(open(filepath, 'rb'))

    def _get_bottom_left_center(self, filename):
        filename_regex = re.compile('^([NS]{1})([0-9]+)([WE]{1})([0-9]+).*')
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
        bottom_left = (bottom_left_corner[0] - self.square_heigth / 2, bottom_left_corner[1] - self.square_width / 2)
        top_left = (bottom_left[0] + self.area_heigth, bottom_left[1])
        top_right = (top_left[0], top_left[1] + self.area_width)
        bottom_right = (bottom_left[0], bottom_left[1] + self.area_width)

        return (
            bottom_left,
            top_left,
            top_right,
            bottom_right
        )

    def is_inside(self, point):
        return \
            self.corners[0][0] < point[0] \
            and self.corners[0][1] < point[1] \
            and point[0] < self.corners[2][0] \
            and point[1] < self.corners[2][1]

    def get_elevation(self, pos):
        if not self.is_inside(pos):
            raise Exception('point {} is not inside HGT file {}'.format(pos, self.filename))

        lat_idx = pos[0] - self.bottom_left_center[0]
        lng_idx = pos[1] - self.bottom_left_center[1]
        print ()
