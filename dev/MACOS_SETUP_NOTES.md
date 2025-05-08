This section describes how to setup PostgreSQL on MacOS so developers can run the test suite:

- install postgresl with homebrew:

$ brew install postgresql

- edit pg_hba.conf so that passwords are required when connecting with TCP/IP:

$ vim /opt/homebrew/var/postgresql@14/pg_hba.conf

# TYPE  DATABASE        USER            ADDRESS                 METHOD
# "local" is for Unix domain socket connections only
local   all             all                                     trust
# IPv4 local connections:
host    all             all             127.0.0.1/32            md5
# IPv6 local connections:
host    all             all             ::1/128                 md5

- start PostgreSQL

$ brew services start postgresql

- use psql with the postgres database and your user to create roles for postgres and eventsourcing and database for eventsourcing
$ psql postgres
postgres=> CREATE ROLE postgres LOGIN SUPERUSER PASSWORD 'postgres';
postgres=> CREATE DATABASE eventsourcing;
postgres=> CREATE USER eventsourcing WITH PASSWORD 'eventsourcing';
postgres=> ALTER DATABASE eventsourcing OWNER TO eventsourcing;

- use psql with the eventsourcing user to create schema in eventsourcing database
$ psql -U eventsourcing
eventsourcing=> CREATE SCHEMA myschema AUTHORIZATION eventsourcing;


To build PDF docs (make docs-pdf), download and install MacTeX from https://www.tug.org/mactex/mactex-download.html
and then make sure latexmk is on your PATH (export PATH="$PATH:/Library/TeX/texbin").

To use psycopg without psycopg-c or psycopg-binary (e.g. when testing beta versions of new Python releases
before psycopg-binary has been released), install libpq with homebrew:

$ brew install libpq
