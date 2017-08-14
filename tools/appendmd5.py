#! /usr/bin/env python

""" Add md5sum to the json dataset files generated with the srtm3.py command """

import json
import os
import re

allsums = {}

md5sum_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'srtm3_md5sum.txt')
with open(md5sum_filepath, 'r') as md5sum_file:
    for line in md5sum_file:
        m = re.match('([A-Za-z0-9]+)\s+(.*)', line)
        md5sum = m.group(1)
        filename = m.group(2)
        allsums[filename] = md5sum


srtm3_dataset_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                  '..', 'gmaltcli', 'datasets', 'srtm3.json')
with open(srtm3_dataset_filepath, 'r') as dataset_file:
    dataset = json.loads(dataset_file.read())
    for filename in dataset:
        if dataset[filename]['zip'] not in allsums:
            raise Exception('no md5 sum for {}'.format(dataset[filename]['zip']))
        dataset[filename]['md5'] = allsums[dataset[filename]['zip']]


with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'srtm3_md5.json'), 'w') as outfile:
    json.dump(dataset, outfile, sort_keys=True, indent=4, separators=(',', ': '))
