import logging
import sys
import argparse

import gmaltcli.hgt as hgt


def read_from_hgt():
    parser = argparse.ArgumentParser(description='Pass along the latitude/longitude of the point you want to '
                                                 'know the latitude of and a HGT file. It will look for the '
                                                 'elevation of your point into the file and return it.')
    parser.add_argument('lat', type=float, help='The latitude of your point (example: 48.861295)')
    parser.add_argument('lng', type=float, help='The longitude of your point (example: 2.339703)')
    parser.add_argument('hgt_file', type=str, help='The file to load (example: N00E010.hgt)')
    args = parser.parse_args()

    try:
        with hgt.HgtParser(args.hgt_file) as hgt_parser:
            elev_data = hgt_parser.get_elevation((args.lat, args.lng))
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)

    sys.stdout.write('Report:\n')
    sys.stdout.write('    Location: ({}P,{}L)\n'.format(elev_data[1], elev_data[0]))
    sys.stdout.write('    Band 1:\n')
    sys.stdout.write('        Value: {}\n'.format(elev_data[2]))


