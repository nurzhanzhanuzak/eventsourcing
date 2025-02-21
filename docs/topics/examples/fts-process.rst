System 1 - Content management system
====================================

In this example, event notifications from the ``ContentManagement`` application described in
:doc:`/topics/examples/content-management` are processed and projected into an
eventually-consistent full text search index, a searchable "materialized view" of
the pages' body text just like :doc:`/topics/examples/fts-content-management`.

This is an example of CQRS. By separating the search engine "read model" from the content management
"write model", the commands that update pages will perform faster. But, more importantly, the search
engine can be redesigned and rebuilt by reprocessing those events. The projected searchable content
can be deleted and rebuilt, perhaps also to include page titles, or timestamps, or other information
contained in the domain events such as the authors, because it is updated by processing events.
This is the main advantage of "CQRS" over the "inline" technique used in :doc:`/topics/examples/fts-content-management`
where the search index is simply updated whenever new events are recorded. Please note, it is possible
to migrate from the "inline" technique to CQRS, by adding the downstream processing and then removing
the inline updating, since the domain model is already event sourced. Similarly, other projections
can be added to work alongside and concurrently with the updating of the search engine.

Application
-----------

The ``PagesIndexApplication`` defined below is a :class:`~eventsourcing.system.ProcessApplication`.
Its ``policy()`` function is coded to process the ``Page.Created`` and ``Page.BodyUpdated`` domain
events of the ``ContentManagement`` application. It also has a ``search()`` method that returns
a list of page IDs.

The ``PagesIndexApplication`` class in this example works in a similar way to the ``SearchableContentApplication``
class in :doc:`/topics/examples/fts-content-management`, by setting variable keyword arguments
``insert_pages`` and ``update_pages`` on a :class:`~eventsourcing.application.ProcessingEvent` object.
However, rather than populating the variable keyword arguments in the ``save()`` method, it populates ``insert_pages``
and ``update_pages`` within its ``policy()`` function. The ``insert_pages`` and ``update_pages`` arguments are set
on the :class:`~eventsourcing.application.ProcessingEvent` object passed into the ``policy()``
function, which carries an event notification ID that indicates the position in the application sequence of
the domain event that is being processed.

The application will be configured to run with a custom :class:`~eventsourcing.persistence.ProcessRecorder`
so that search index records will be updated atomically with the inserting of a tracking record which
indicates which upstream event notification has been processed.

Because the ``Page.BodyUpdated`` event carries only the ``diff`` of the page body, the
``policy()`` function must first select the current page body from its own records
and then apply the diff as a patch. The "exactly once" semantics provided by the library's
system module guarantees that the diffs will always be applied in the correct order. Without
this guarantee, the projection could become inconsistent, with the consequence that the diffs
will fail to be applied.

.. literalinclude:: ../../../examples/ftsprocess/application.py

System
------

A :class:`~eventsourcing.system.System` of applications is defined, in which the
``PagesIndexApplication`` follows the ``ContentManagement`` application. This system
can then be used in any :class:`~eventsourcing.system.Runner`.

.. literalinclude:: ../../../examples/ftsprocess/system.py

PostgreSQL
----------

The ``PostgresSearchableContentRecorder`` from :doc:`/topics/examples/fts-content-management`
is used to define a custom :class:`~eventsourcing.persistence.ProcessRecorder` for PostgreSQL.
The PostgreSQL :class:`~eventsourcing.postgres.Factory` class is extended to involve this custom recorder
in a custom persistence module so that it can be used by the ``PagesIndexApplication``.


.. literalinclude:: ../../../examples/ftsprocess/postgres.py

SQLite
------

The ``SqliteSearchableContentRecorder`` from :doc:`/topics/examples/fts-content-management`
is used to define a custom :class:`~eventsourcing.persistence.ProcessRecorder` for SQLite.
The SQLite :class:`~eventsourcing.sqlite.Factory` class is extended to involve this custom recorder
in a custom persistence module so that it can be used by the ``PagesIndexApplication``.

.. literalinclude:: ../../../examples/ftsprocess/sqlite.py


Test case
---------

The test case ``ContentManagementSystemTestCase`` creates three pages, for 'animals', 'plants'
and 'minerals'. Content is added to the pages. The content is searched with various queries and
the search results are checked. The test is executed twice, once with the application configured
for both PostgreSQL, and once for SQLite.

.. literalinclude:: ../../../examples/ftsprocess/test_system.py


Code reference
--------------

.. automodule:: examples.ftsprocess.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.ftsprocess.test_system
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

