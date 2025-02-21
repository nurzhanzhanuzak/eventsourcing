Projections 1 - Full text search
================================

In this example, event notifications from example :doc:`/topics/examples/content-management`
are processed and projected into an eventually-consistent full text search index, a searchable "materialized view" of
the pages' body.

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

Persistence
-----------

The :class:`~examples.ftsprojection.projection.FtsTrackingRecorder` defines an abstract interface for
inserting and updating pages in a full text search index with tracking information. This is so that
a projection can be defined independently of a particular implementation.

It extends both the abstract :class:`~examples.ftscontentmanagement.persistence.FtsRecorder` class
from example :doc:`/topics/examples/fts-content-management` and the library's abstract
:class:`~eventsourcing.persistence.TrackingRecorder` class, so that subclasses will implement
both the interface for full text search and for recording tracking information.

It defines method signatures :func:`~examples.ftsprojection.projection.FtsTrackingRecorder.insert_pages_with_tracking`
and :func:`~examples.ftsprojection.projection.FtsTrackingRecorder.update_pages_with_tracking` so that
pages may be inserted and updated in a full text search index atomically with tracking information.

.. literalinclude:: ../../../examples/ftsprojection/projection.py
    :pyobject: FtsTrackingRecorder


PostgreSQL
----------

The :class:`~examples.ftsprojection.projection.PostgresFtsTrackingRecorder` implements the
abstract :class:`~examples.ftsprojection.projection.FtsTrackingRecorder` by inheriting
the :class:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder` class from example
:doc:`/topics/examples/fts-content-management` and the library's
:class:`~eventsourcing.postgres.PostgresTrackingRecorder` class.

It implements the method :func:`~examples.ftsprojection.projection.PostgresFtsTrackingRecorder.insert_pages_with_tracking`
required by :class:`~examples.ftsprojection.projection.FtsTrackingRecorder` by calling within a database transaction both
:func:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder._insert_pages` of :class:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder`
and :func:`~eventsourcing.postgres.PostgresTrackingRecorder._insert_tracking` of :class:`~eventsourcing.postgres.PostgresTrackingRecorder`,
so that new pages will be inserted in the full text search index atomically with tracking information.

It implements the method :func:`~examples.ftsprojection.projection.PostgresFtsTrackingRecorder.update_pages_with_tracking`
required by :class:`~examples.ftsprojection.projection.FtsTrackingRecorder` by calling within a database transaction both
:func:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder._update_pages` of :class:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder`
and :func:`~eventsourcing.postgres.PostgresTrackingRecorder._insert_tracking` of :class:`~eventsourcing.postgres.PostgresTrackingRecorder`,
so that existing pages will be updated in the full text search index atomically with tracking information.

.. literalinclude:: ../../../examples/ftsprojection/projection.py
    :pyobject: PostgresFtsTrackingRecorder


Projection
----------

The :class:`~examples.ftsprojection.projection.FtsProjection` class defined below
implements the library's :class:`~eventsourcing.projection.Projection` class, the abstract base class
for projections of event-sourced applications.

The generic class :class:`~eventsourcing.projection.Projection` has one type variable which is expected
to be a :class:`~eventsourcing.persistence.TrackingRecorder`, and which in this case is specified to be
:class:`~examples.ftsprojection.projection.FtsTrackingRecorder`, which means the projection should
be constructed with a subclass of :class:`~examples.ftsprojection.projection.FtsTrackingRecorder`,
for example :class:`~examples.ftsprojection.projection.PostgresFtsTrackingRecorder`.

Its :func:`~examples.ftsprojection.projection.FtsProjection.process_event` function is coded to
process the :class:`Page.Created <examples.contentmanagement.domainmodel.Page.Created>` and
:class:`Page.BodyUpdated <examples.contentmanagement.domainmodel.Page.BodyUpdated>`
events of the domain model in :doc:`/topics/examples/content-management`.

When a :class:`Page.Created <examples.contentmanagement.domainmodel.Page.Created>` event is received,
the method :func:`~examples.ftsprojection.projection.FtsTrackingRecorder.insert_pages_with_tracking`
of an :class:`~examples.ftsprojection.projection.FtsTrackingRecorder` object is called.

When a :class:`Page.BodyUpdated <examples.contentmanagement.domainmodel.Page.BodyUpdated>` event is received,
the method :func:`~examples.ftsprojection.projection.FtsTrackingRecorder.update_pages_with_tracking`
of an :class:`~examples.ftsprojection.projection.FtsTrackingRecorder` object is called.

.. literalinclude:: ../../../examples/ftsprojection/projection.py
    :pyobject: FtsProjection


Test case
---------

The test case :class:`~examples.ftsprojection.test_projection.TestFtsProjection` shows how the
library's :class:`~eventsourcing.projection.ProjectionRunner` class can be used to run a projection.

First, a :class:`~eventsourcing.projection.ProjectionRunner` object is constructed with the
application class :class:`~examples.contentmanagement.application.ContentManagement`
and the projection class :class:`~examples.ftsprojection.projection.FtsProjection`.

An environment is specified that defines persistence infrastructure for the application and the projection,
in particular :class:`~examples.ftsprojection.projection.PostgresFtsTrackingRecorder` is specifed as
the projection's tracking recorder.

Because the projection uses a subscription, the projection will follow events from every instance
of the :class:`~examples.contentmanagement.application.ContentManagement` application "write model"
that is configured to use the same database. And because the projection is recorded in the database,
it can be queried from any instance of the :class:`~examples.ftsprojection.projection.PostgresFtsTrackingRecorder`
recorder "read model" that is configured to use the same database. The application and the projection can use
different databases, but in this example they use different tables in the same PostgreSQL database.

The test creates three pages, for 'animals', 'plants' and 'minerals'. Content is added to the pages.
After waiting for the projection to have been updated, the content is searched with various queries
and the search results are checked. The tracking recorder method :func:`~eventsourcing.persistence.TrackingRecorder.wait`
is called so that the search index will have been be updated with the results of processing new events before the
projection is queried.

The test demonstrates that the projection firstly catches up with existing content, and then continues
automatically to process new content.

.. literalinclude:: ../../../examples/ftsprojection/test_projection.py
    :pyobject: TestFtsProjection


Code reference
--------------

.. automodule:: examples.ftsprojection.projection
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.ftsprojection.test_projection
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

