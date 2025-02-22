================================================
:mod:`~eventsourcing.projection` --- Projections
================================================

This module shows how :doc:`event-sourced applications
</topics/application>` can be projected into materialised
views that support arbitrary queries.

The central idea here follows the notion from `CQRS <https://en.wikipedia.org/wiki/Command_Query_Responsibility_Segregation>`_
of having separate command and query interfaces. This idea is often implemented in event-sourced systems
with distinct and separate "write" and "read" models. The "write model" is an event-sourced application,
and the "read model" is one or many "materialised views" of the event-sourced application. The event-sourced
application is projected into a materialised view, by processing the application's events,
usually with an asynchronous event-processing component, so that the materialised view is
`eventually-consistent <https://en.wikipedia.org/wiki/Eventual_consistency>`_.

By processing each domain event in an application sequence in order, and by recording
updates to the materialised view atomically with tracking objects that indicate the
position in the application sequence of the event that was processed, and by constraining
the tracking records to be unique, and by resuming to process the application from the
position indicated by the last tracking record, the materialised
view will be a "reliable" deterministic function of the state of the application.


Tracking recorders
==================

The library's :ref:`tracking recorder <Tracking recorder>` classes
(:class:`~eventsourcing.popo.POPOTrackingRecorder`, :class:`~eventsourcing.sqlite.SQLiteTrackingRecorder`,
and :class:`~eventsourcing.postgres.PostgresTrackingRecorder`) can be extended arbitrarily to define command
and query methods that update and present a materialised view of the state of an event-sourced application
atomically with tracking information. The tracking information indicates the position in an application
sequence of an event that was processed when the materialised view was updated.

For example, the :class:`CountRecorder` class, included below, extends the library's abstract base class
:class:`~eventsourcing.persistence.TrackingRecorder` by defining abstract methods
:func:`incr_created_events_counter`, :func:`incr_subsequent_events_counter`, :func:`get_created_events_counter`,
:func:`get_subsequent_events_counter`, and :func:`get_all_events_counter`. These methods
will be implemented by concrete tracking recorder classes to update a materialised view
that counts aggregate events from an event-sourced application.

.. literalinclude:: ../../tests/projection_tests/test_projection.py
    :pyobject: CountRecorder

The :class:`POPOCountRecorder` class, included below, implements this interface using plain old Python objects.

.. literalinclude:: ../../tests/projection_tests/test_projection.py
    :pyobject: POPOCountRecorder

.. _Projection:

Projection
==========

The library's abstract base class :class:`~eventsourcing.projection.Projection` can be used to define how
domain events will be processed. Subclasses are expected to use a particular kind of tracking recorder. It
defines an abstract method :func:`~eventsourcing.projection.Projection.process_event` that must be implemented by subclasses.

For example, the :class:`CountProjection` class, included below, inherits :class:`~eventsourcing.projection.Projection`,
specifies that instances will use a :class:`CountRecorder`, and implements :func:`~eventsourcing.projection.Projection.process_event`
by calling :func:`incr_created_event_count` for each :class:`Aggregate.Created <eventsourcing.domain.Aggregate.Created>` event,
and by calling :func:`incr_subsequent_event_count` for each subsequent :class:`Aggregate.Event <eventsourcing.domain.Aggregate.Event>`.

.. literalinclude:: ../../tests/projection_tests/test_projection.py
    :pyobject: CountProjection

.. _Projection runner:

Projection Runner
=================

The library's :class:`~eventsourcing.projection.ProjectionRunner` class is provided for the purpose
or running projections.

A projection runner object can be constructed with an application class, a projection class, a tracking
recorder class, and an environment that specifies the persistence modules to be used by the application
and the tracking recorder.

The projection runner will construct an instance of the given application class, and an instance of
the given projection class, and an instance of the given tracking recorder class. It will
:ref:`subscribe to its application <Subscriptions>`, from the position indicated by its tracking recorder's
:func:`~eventsourcing.persistence.TrackingRecorder.max_tracking_id` method. And then it will call
the :func:`~eventsourcing.projection.Projection.process_event` method of the projection for each event
in the application sequence.

Because it starts a :ref:`subscription <Subscriptions>` to the application, it will first catch up by
processing already recorded events that have not yet been processed. And then it will continue indefinitely
to process events that are recorded after the runner has been started.

The :class:`~eventsourcing.projection.ProjectionRunner` class has a :func:`~eventsourcing.projection.ProjectionRunner.run_forever`
method, which blocks indefinitely, or until an optional timeout, or until an exception is raised by the projection or
by the subscription. This allows an event processing component to be started and run independently as a
separate operating system process, and then to terminate when there is an error. Operators of the system can
examine the error and resume processing by reconstructing the runner. Some errors may be transient operational
issues, such as database connectivity, in which case the processing could be resumed automatically. Some errors
may be programming errors, and will require manual intervention before the event processing can continue.

The :class:`TestCountProjection` class shown below constructs a :class:`~eventsourcing.projection.ProjectionRunner`
with the library's :class:`~eventsourcing.application.Application` class, the :class:`CountProjection` class,
and the :class:`POPOCountRecorder`.

Two aggregates are saved in the "write model". They have two subsequent events each.
The total counts for the application events are obtained from the "read model".

.. literalinclude:: ../../tests/projection_tests/test_projection.py
    :pyobject: TestCountProjection

If the application "write model" and the tracking recorder "read model" use a durable database, such as
PostgreSQL, any instance of the application can be used to write events, and any instance of the tracking
recorder can be used to query the materialised view. However, in this case, using the :ref:`POPO module <popo-module>`
means that we need to use the same instance of the application and of the recorder.


With PostgreSQL
===============

We can also implement the tracking recorder to work with PostgreSQL. The :func:`_incr_counter` method
of :class:`PostgresCountRecorder`, included below, updates the materialised view and records
a tracking object atomically in the same database transaction.

.. literalinclude:: ../../tests/projection_tests/test_projection.py
    :pyobject: PostgresCountRecorder

Because this example uses a durable database, separate instances of the application and the recorder
can be used as interfaces to the "write model" and the "read model".

The application and the projection could use separate databases, but in this example they simply
use different tables in the same database.


.. literalinclude:: ../../tests/projection_tests/test_projection.py
    :pyobject: TestCountProjectionWithPostgres

See example :doc:`/topics/examples/fts-projection` for a more substantial example.

Code reference
==============

.. automodule:: eventsourcing.projection
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__, __enter__, __exit__, __iter__, __next__
