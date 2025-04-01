============
Installation
============

This version of the library is compatible with Python versions 3.8, 3.9,
3.10, 3.11, 3.12, and 3.13.

This package depends only on modules from the Python Standard Library,
except for ``typing_extensions`` and the extra install options described below.


Pip install
===========

You can use pip to install the library from the
`Python Package Index <https://pypi.org/project/eventsourcing/>`_.

::

    $ pip install eventsourcing

It is recommended to install the library into a Python virtual environment.

::

    $ python3 -m venv my_venv
    $ source my_venv/bin/activate
    (my_venv) $ pip install eventsourcing


When including the library in a list of project dependencies, in order to
avoid installing future incompatible releases, it is recommended to specify
the major and minor version numbers.

As an example, the expression ``eventsourcing<=9.3.99999`` would install the
latest version of the 9.3 series, allowing future bug fixes released with
point version number increments. You can use this expression in a ``pip install``
command, in a ``requirements.txt`` file, or in a ``setup.py`` file.

::

    $ pip install "eventsourcing<=9.4.99999"

If you are specifying the dependencies of your project in a ``pyproject.toml``
file, and for example using the Poetry build tool, you can specify the
dependency on this library in the following way.

::

    [tool.poetry.dependencies]
    python = "^3.8"
    eventsourcing = { version = "~9.4" }


Specifying the major and minor version number in this way will avoid any
potentially destabilising additional features introduced with minor version
number increments, and also avoid all backward incompatible changes introduced
with major version number increments.

Upgrading to new minor versions is encouraged, but it is recommended to
do this manually so that you are sure your project isn't inadvertently
broken by changes in the library. Migrating to new major versions is
also encouraged, but by definition this may involve your making changes
to your project to adjust for the backward incompatibilities introduced
by the new major version. Of course it's your project so, if you wish,
feel free to pin the major and minor and point version, or indeed only
the major version.

Install options
===============

Running the install command with different options will install
the extra dependencies associated with that option. If you installed
without any options, you can easily install optional dependencies
later by running the install command again with the options you want.

If you want to :ref:`store events with PostgreSQL <postgres-environment>`, then install with
the ``postgres`` option. This installs `Psycopg v3 <https://pypi.org/project/psycopg/>`_
and its connection pool package.

The C optimization is recommended by the `Psycopg <https://www.psycopg.org>`_  developers for production usage.
The pre-built binary option ``psycopg[binary]`` is a convenient alternative for development and testing, and
for those unable to meet the prerequisites needed for building ``psycopg[c]``.

This package now follows the recommendation that library's should depend only on the pure Python package, giving
users the choice of either compiling the C optimization or using the pre-built binary or using the pure
Python package. If you don't install either ``psycopg[c]`` or ``psycopg[binary]`` then you need to make sure
the libpq is installed. The libpq is the client library used by psql, the PostgreSQL command line client. See
the `Psycopg docs <https://www.psycopg.org/psycopg3/docs/basic/install.html#pure-python-installation>`_ for more
information.

See the :ref:`PostgreSQL persistence module documentation <postgres-environment>` for more information about storing
events in PostgreSQL.

::

    $ pip install "eventsourcing[postgres]"


If you want to store cryptographically encrypted events,
then install with the ``cryptography`` option. This simply installs
the Python `cryptography <https://pypi.org/project/cryptography/>`_.
Please note, you will need to :ref:`configure your application <Application configuration>`
environment to enable encryption.

::

    $ pip install "eventsourcing[cryptography]"


Alternatively, if you want to store cryptographically encrypted events,
then you can install with the ``crypto`` option. This simply installs
`PyCryptodome <https://pypi.org/project/pycryptodome/>`_.
Please note, you will need to :ref:`configure your application <Application configuration>`
environment to enable encryption.

::

    $ pip install "eventsourcing[crypto]"


Options can be combined, so that if you want to store encrypted events in PostgreSQL,
then install with both the ``postgres`` and the ``cryptography`` options.

::

    $ pip install "eventsourcing[postgres,cryptography]"


.. _Template:

Project template
================

To start a new project with modern tooling, you can use the
`template for Python eventsourcing projects <https://github.com/pyeventsourcing/cookiecutter-eventsourcing#readme>`_.

The project template uses Cookiecutter to generate project files.
It uses the build tool Poetry to create a Python virtual environments
for your project, to manage project dependencies, and to create distributions.
It uses popular development dependencies such as pytest, coverage, Black,
isort, and mypy to help you develop and maintain your code. It has a GitHub
Actions workflow, and has an initial README and LICENCE files that you
can adjust.

The project template also includes the "dog school" example. The tests
should pass. You can adjust the tests, rename the classes, and change the
methods. Or just delete the included example code for a fresh start.


Developers
==========

If you want to install the code for the purpose of developing the library, then
fork and clone the GitHub repository.

Once you have cloned the project's GitHub repository, change into the root folder,
or open the project in an IDE. You should see a Makefile.

If you don't already have Poetry installed, run `make install-poetry`.

::

    $ make install-poetry


Run `make install-packages` to create a new virtual environment and
install packages that needed for development, such as Sphinx, Coverage.py, Black,
mypy, ruff, and isort.

::

    $ make install-packages


Once installed, check the project's test suite passes by running `make test`.

::

    $ make test


Before the tests will pass, you will need setup PostgreSQL, with a database
called 'eventsourcing' that can be accessed by a user called 'eventsourcing'
that has password 'eventsourcing'.

The following commands will install PostgreSQL on MacOS and setup the database and
database user. If you already have PostgreSQL installed, just create the database
and user. You may prefer to run PostgreSQL in a Docker container.

::

    $ brew install postgresql
    $ brew services start postgresql
    $ psql postgres
    postgres=# CREATE DATABASE eventsourcing;
    postgres=# CREATE USER eventsourcing WITH PASSWORD 'eventsourcing';
    postgres=# ALTER DATABASE eventsourcing OWNER TO eventsourcing;
    $ psql eventsourcing
    postgres=# CREATE SCHEMA myschema AUTHORIZATION eventsourcing;


Check the syntax and static types are correct by running `make lint`.

::

    $ make lint


The code can be automatically reformatted using the following command
(which uses isort and Black). Ruff and mypy errors will often need
to be fixed by hand.

::

    $ make fmt


You can build the docs (and check they build) with `make docs`.

::

    $ make docs

Before submitting Pull Requests on GitHub, please make sure everything is working
by running `make docs lint test`.
