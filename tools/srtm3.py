#! /usr/bin/env python

""" Get all the SRTM3 files and urls in a dict and dump it into a JSON file """

import os
import json

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

import lxml.html

SRTM3_FOLDERS=(
    'http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/Africa/',
    'http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/Australia/',
    'http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/Eurasia/',
    'http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/Islands/',
    'http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/North_America/',
    'http://dds.cr.usgs.gov/srtm/version2_1/SRTM3/South_America/'
)

hgt_urls = {}


def process_links(links, url):
    for link in links:
        zip_filename = link.attrib['href']
        full_url = "%s%s" % (url, link.attrib['href'])
        hgt_filename, zip_extension = os.path.splitext(zip_filename)
        if zip_extension == '.zip':
            hgt_urls[hgt_filename] = {'zip': zip_filename, 'url': full_url}


for url in SRTM3_FOLDERS:
    html = urlopen(url).read()
    doc = lxml.html.fromstring(html)
    links = doc.xpath('//a[@href]')
    process_links(links, url)


with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'srtm3.json'), 'w') as outfile:
    json.dump(hgt_urls, outfile, sort_keys=True, indent=4, separators=(',', ': '))
