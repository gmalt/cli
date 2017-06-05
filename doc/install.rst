gmalt CLI - Installation
========================

Global from PyPI
----------------

If you want to install this package globally at the OS level to benefit from the command lines anywhere, you can install it from PyPI.

Just run this command in a terminal:

.. code-block:: console

    $ sudo pip install gmaltcli

After installation, the commands should be available:

.. code-block:: console

    $ which gmalt-hgtread
    /usr/bin/gmalt-hgtread
    $ which gmalt-hgtget
    /usr/bin/gmalt-hgtget
    $ which gmalt-hgtload
    /usr/bin/gmalt-hgtload

Local in a virtualenv
---------------------

This is the recommanded way to install because the CLI is a one time usage. You download and load the elevation data and that's it.

Clone the repository, create and activate a virtualenv then install dependencies :

.. code-block:: console

    $ git clone git@github.com:gmalt/cli.git gmalt-cli
    $ cd gmalt-cli
    $ virtualenv venv
    $ . venv/bin/activate
    $ python setup.py develop

the commands should be available:

.. code-block:: console

    $ which gmalt-hgtread
    .../venv/bin/gmalt-hgtread
    $ which gmalt-hgtget
    .../venv/bin/gmalt-hgtget
    $ which gmalt-hgtload
    .../venv/bin/gmalt-hgtload
