Projections 1 - Full text search
================================

In this example, a :ref:`projection <Projection>` is defined, using the :doc:`projection module </topics/projection>`,
that processes events from example :doc:`/topics/examples/content-management`. The events
are processed into an eventually-consistent full text search index, a searchable
"materialized view" of the content of the application.

This is an example of CQRS. In this example, only the :data:`~examples.contentmanagement.domainmodel.Page.body`
values of the :class:`~examples.contentmanagement.domainmodel.Page` aggregates are indexed in the search engine.
By separating the search engine "read model" from the content management "write model", the search engine can
be redesigned and rebuilt by reprocessing those events. The projected searchable content can be deleted and
rebuilt, perhaps also to include page titles, or timestamps, or other information contained in the domain events
such as the authors.

This is the main advantage of "CQRS" over the "inline" technique used in :doc:`/topics/examples/fts-content-management`
where the search index is simply updated whenever new events are recorded.

.. Please note, it is possible
  to migrate from the "inline" technique to CQRS, by adding the downstream processing and then removing
  the inline updating, since the domain model is already event sourced. Similarly, other projections
  can be added to work alongside and concurrently with the updating of the search engine.

Persistence
-----------

Firstly, let's consider the "read model".

The :class:`~examples.ftsprojection.projection.FtsViewInterface` defines an abstract interface for
searching, inserting, and updating pages in a full text search index with tracking information. Abstract
methods are defined so that a projection can be defined independently of a particular database.

It defines abstract method signatures :func:`~examples.ftsprojection.projection.FtsViewInterface.insert_pages_with_tracking`
and :func:`~examples.ftsprojection.projection.FtsViewInterface.update_pages_with_tracking` so that
pages may be inserted and updated atomically in a full text search index along with tracking information.

.. literalinclude:: ../../../examples/ftsprojection/projection.py
    :pyobject: FtsViewInterface


It extends both the abstract :class:`~examples.ftscontentmanagement.persistence.FtsRecorder` class
from example :doc:`/topics/examples/fts-content-management` and the library's abstract
:class:`~eventsourcing.persistence.TrackingRecorder` class, so that subclasses will implement
both the interface for full text search and for recording tracking information.

Abstract search method signatures are inherited from the :class:`~examples.ftscontentmanagement.persistence.FtsRecorder` class.


PostgreSQL
----------

Now let's consider how we might implement this "read model" interface to work with a PostgreSQL database.

The :class:`~examples.ftsprojection.projection.PostgresFtsView` implements the
abstract :class:`~examples.ftsprojection.projection.FtsViewInterface` by inheriting
the :class:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder` class from example
:doc:`/topics/examples/fts-content-management` and the library's
:class:`~eventsourcing.postgres.PostgresTrackingRecorder` class.

It implements the method :func:`~examples.ftsprojection.projection.PostgresFtsView.insert_pages_with_tracking`
required by :class:`~examples.ftsprojection.projection.FtsViewInterface` by calling within a database transaction both
:func:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder._insert_pages` of :class:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder`
and :func:`~eventsourcing.postgres.PostgresTrackingRecorder._insert_tracking` of :class:`~eventsourcing.postgres.PostgresTrackingRecorder`,
so that new pages will be inserted in the full text search index atomically with tracking information.

It implements the method :func:`~examples.ftsprojection.projection.PostgresFtsView.update_pages_with_tracking`
required by :class:`~examples.ftsprojection.projection.FtsViewInterface` by calling within a database transaction both
:func:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder._update_pages` of :class:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder`
and :func:`~eventsourcing.postgres.PostgresTrackingRecorder._insert_tracking` of :class:`~eventsourcing.postgres.PostgresTrackingRecorder`,
so that existing pages will be updated in the full text search index atomically with tracking information.

.. literalinclude:: ../../../examples/ftsprojection/projection.py
    :pyobject: PostgresFtsView

Search method implementations are inherited from the :class:`~examples.ftscontentmanagement.postgres.PostgresFtsRecorder`  class.


Projection
----------

Having defined a read model, let's consider how the domain events of the content management application
can be processed. This section uses the :ref:`projection class <Projection>` from the :doc:`projection module </topics/projection>`.

The :class:`~examples.ftsprojection.projection.FtsProjection` class implements the library's
:class:`~eventsourcing.projection.Projection` class, the abstract base class for projections
of event-sourced applications. It can be run as an event-processing component using
the library's :ref:`projection runner <Projection runner>`.

Its :func:`~examples.ftsprojection.projection.FtsProjection.process_event` function is coded to
process the :class:`Page.Created <examples.contentmanagement.domainmodel.Page.Created>` and
:class:`Page.BodyUpdated <examples.contentmanagement.domainmodel.Page.BodyUpdated>`
events of the domain model in :doc:`/topics/examples/content-management`.

When a :class:`Page.Created <examples.contentmanagement.domainmodel.Page.Created>` event is received,
the method :func:`~examples.ftsprojection.projection.FtsViewInterface.insert_pages_with_tracking`
of an :class:`~examples.ftsprojection.projection.FtsViewInterface` object is called.

When a :class:`Page.BodyUpdated <examples.contentmanagement.domainmodel.Page.BodyUpdated>` event is received,
the method :func:`~examples.ftsprojection.projection.FtsViewInterface.update_pages_with_tracking`
of an :class:`~examples.ftsprojection.projection.FtsViewInterface` object is called.

.. literalinclude:: ../../../examples/ftsprojection/projection.py
    :pyobject: FtsProjection

The :class:`~eventsourcing.projection.Projection` class is a generic class that requires one type variable, which
is expected to be a subclass of :class:`~eventsourcing.persistence.TrackingRecorder`. In this case, the type variable
is specified to be :class:`~examples.ftsprojection.projection.FtsViewInterface`, which means the projection
should be constructed with a subclass of :class:`~examples.ftsprojection.projection.FtsViewInterface`,
for example :class:`~examples.ftsprojection.projection.PostgresFtsView`.

Test case
---------

The test case :class:`~examples.ftsprojection.test_projection.TestFtsProjection` shows how the
library's :ref:`projection runner <Projection runner>` class can be used to run the full text search
projection of the content application.

The test demonstrates that the projection firstly catches up with existing content, and then continues
automatically to process new content.

The test creates two pages, for 'animals' and for 'plants'. Content is added to the pages.
The projection is then started. The tracking recorder method :func:`~eventsourcing.persistence.TrackingRecorder.wait`
is called so that the search index will have been be updated with the results of processing new events before the
projection is queried. After waiting for the projection to process the application's events,
the search index is queried, and the search results are checked. A third
page for 'minerals' is then created.

A :class:`~eventsourcing.projection.ProjectionRunner` object is constructed with the
application class :class:`~examples.contentmanagement.application.ContentManagement`,
the projection class :class:`~examples.ftsprojection.projection.FtsProjection`, and the
tracking recorder class :class:`~examples.ftsprojection.projection.PostgresFtsView`.

An environment is specified that defines persistence infrastructure for the application and the tracking
recorder.

Because the projection uses a subscription, the projection will follow events from every instance
of the :class:`~examples.contentmanagement.application.ContentManagement` application "write model"
that is configured to use the same database. And because the projection is recorded in the database,
it can be queried from any instance of the :class:`~examples.ftsprojection.projection.PostgresFtsView`
recorder "read model" that is configured to use the same database. To demonstrate this, separate instances
of the application and the recorder are used as the "write model" and "read model" interfaces. The projection
runs independently.

The application and the recorder could use different databases, but in this example they use different
tables in the same PostgreSQL database.

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

