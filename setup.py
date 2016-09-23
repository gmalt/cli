import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name = "hgt2sql",
    version = read('VERSION'),
    author = "Jonathan Bouzekri",
    author_email = "jonathan.bouzekri@gmail.com",
    description = ("Download, extract and import HGT data into a SQL database"),
    license = "MIT",
    keywords = "example documentation tutorial",
    url = "http://github.com/gmalt/hgt2sql",
    packages=find_packages(),
    long_description=read('README.md'),
    extras_require={
        'tools': ['lxml'],
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Scientific/Engineering :: GIS",
        "Topic :: Utilities",
    ],
)
