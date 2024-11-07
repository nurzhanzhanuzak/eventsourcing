.. _Searchable timestamps example:

Application 4 - Searchable timestamps
=====================================

This example demonstrates how to extend the library's application recorder classes
to support retrieving aggregates at a particular point in time with both PostgreSQL
and SQLite.

Application
-----------

The application class ``SearchableTimestampsApplication`` extends the ``BookingApplication``
presented in the :doc:`cargo shipping example </topics/examples/cargo-shipping>`. It extends
the application ``_record()`` method by setting in the processing event a list of ``Cargo``
events that will be used by the recorder to insert event timestamps into an index. It also
introduces a ``get_cargo_at_timestamp()`` method that expects a ``tracking_id`` and a
``timestamp`` argument, then returns a ``Cargo`` aggregate as it was at the specified time.

.. literalinclude:: ../../../examples/searchabletimestamps/application.py


Persistence
-----------

The recorder classes ``SearchableTimestampsApplicationRecorder`` extend the PostgreSQL
and SQLite ``ApplicationRecorder`` classes by creating a table that contains rows
with the originator ID, timestamp, and originator version of aggregate events. The
define SQL statements that insert and select from the rows of the table.
They define a ``get_version_at_timestamp()`` method which returns the version of
an aggregate at a particular time.

.. literalinclude:: ../../../examples/searchabletimestamps/persistence.py

The application recorder classes extend the ``_insert_events()`` method by inserting rows,
according to the event timestamp data passed down from the application.

The infrastructure factory classes ``SearchableTimestampsInfrastructureFactory`` extend the
PostgreSQL and SQLite ``Factory`` classes by overriding the ``application_recorder()`` method
so that a ``SearchableTimestampsApplicationRecorder`` is constructed as the application recorder.


PostgreSQL
----------

.. literalinclude:: ../../../examples/searchabletimestamps/postgres.py


SQLite
------

.. literalinclude:: ../../../examples/searchabletimestamps/sqlite.py


Test case
---------

The test case ``SearchableTimestampsTestCase`` uses the application to evolve the
state of a ``Cargo`` aggregate. The aggregate is then reconstructed as it was at
particular times in its evolution. The test is executed twice, with the application
configured for both PostgreSQL and SQLite.

.. literalinclude:: ../../../examples/searchabletimestamps/test_searchabletimestamps.py
