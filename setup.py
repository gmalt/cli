import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read().strip()


setup(
    name="gmaltcli",
    version=read('VERSION'),
    author="Jonathan Bouzekri",
    author_email="jonathan.bouzekri@gmail.com",
    description="Download, extract and import HGT data into a SQL database",
    license="MIT",
    keywords=["hgt", "gis", "cli", "download", "import"],
    url="https://github.com/gmalt/cli",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    include_package_data=True,
    long_description=read('README.rst'),
    install_requires=['SQLAlchemy', 'psycopg2', 'future', 'gmalthgtparser'],
    extras_require={
        'tools': ['lxml'],
        'test': ['pytest', 'flake8', 'mock'],
        'build': ['wheel']
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Scientific/Engineering :: GIS",
        "Topic :: Utilities",
    ],
    entry_points='''
        [console_scripts]
        gmalt-hgtread = gmaltcli.app:read_from_hgt
        gmalt-hgtget = gmaltcli.app:get_hgt
        gmalt-hgtload = gmaltcli.app:load_hgt
    '''
)
