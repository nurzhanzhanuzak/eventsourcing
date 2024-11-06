This document describes how to setup MacOS with databases needed to run the test suite:

- MySQL
- PostgreSQL
- Redis
- Cassandra
- Axon Server


To setup MySQL:

$ brew install mysql
$ brew services start mysql
$ mysql -u root
mysql> CREATE DATABASE eventsourcing;
mysql> CREATE USER 'eventsourcing'@'localhost' IDENTIFIED BY 'eventsourcing';
mysql> GRANT ALL PRIVILEGES ON eventsourcing.* TO 'eventsourcing'@'localhost';

To setup PostgreSQL:

$ brew install postgresql
$ brew services start postgresql
$ psql postgres
postgres=> CREATE DATABASE eventsourcing;
postgres=> CREATE USER eventsourcing WITH PASSWORD 'eventsourcing';
postgres=> ALTER DATABASE eventsourcing OWNER TO eventsourcing;
$ psql -U eventsourcing
eventsourcing=> CREATE SCHEMA myschema AUTHORIZATION eventsourcing;

Maybe also: (?)
postgres=> CREATE ROLE postgres LOGIN SUPERUSER PASSWORD 'postgres';

Also edit pg_hba.conf so that passwords are required when connecting with TCP/IP.


To setup Redis:

$ brew install redis
$ brew services start redis


To setup Cassandra:

$ brew install cassandra
$ brew services start cassandra

If that doesn't actually start Cassandra, then try this in a terminal:
$ cassandra -f


To setup Axon:
$ ./dev/download_axon_server.sh
$ ./axonserver/axonserver.jar


After this, the databases can be stopped with:

$ make brew-services-stop


The database can be started with:

$ make brew-services-start
