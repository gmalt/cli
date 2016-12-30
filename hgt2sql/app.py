import logging
import sys
import argparse

import hgt2sql.hgt as hgt


def read_from_hgt():
    parser = argparse.ArgumentParser(description='Pass along the latitude/longitude of the point you want to '
                                                 'know the latitude of and a HGT file. It will look for the '
                                                 'elevation of your point into the file and return it.')
    parser.add_argument('lat', type=float, help='The latitude of your point (example: 48.861295)')
    parser.add_argument('lng', type=float, help='The longitude of your point (example: 2.339703)')
    parser.add_argument('hgt_file', type=argparse.FileType('rb'), help='The file to load (example: N00E010.hgt)')
    args = parser.parse_args()

    pos = (args.lat, args.lng)
    hgt_parser = hgt.HgtParser(args.hgt_file)

    try:
        elev = hgt_parser.get_elevation(pos)
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)



