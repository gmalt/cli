#! /usr/bin/env python

""" Generate some HGT random file for testing purposes """

import random
import argparse

import struct

parser = argparse.ArgumentParser(description='Generate some HGT random file for testing purposes')
parser.add_argument('width', type=int, help='Number of columns and lines in the file')
parser.add_argument('output', help='File to generate')

args = parser.parse_args()

values = []

with open(args.output, 'wb') as file:
    for i in range(0, args.width ** 2):
        value = random.randint(0, 500)
        values.append(value)
        file.write(struct.pack('>h', value))

print(values)