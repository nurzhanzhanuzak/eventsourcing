===============================
Tutorial - Part 4 - Projections
===============================

This part of the tutorial shows how event-sourced applications can be projected into
materialised views that support arbitrary queries.

As we saw in :doc:`Part 3 </topics/tutorial/part1>`, we can use the library's
:class:`~eventsourcing.application.Application` class to define event-sourced
applications. Now, let's explore how the state of an event-sourced application
can be "projected" into a materialised view that can be used to query the state of
the application in ways that are not supported by the event-sourced application
itself.

Tracking recorders
==================

Firstly, let's consider the materialised view itself. It will need both command and query methods. The query
methods will be designed to support the queries needed by users that cannot be supported by the event-sourced
application itself. The command methods will be used by an event-processing component to update the materialised
view.

The library's :ref:`tracking recorder <Tracking recorder>` classes,
:class:`~eventsourcing.popo.POPOTrackingRecorder`, :class:`~eventsourcing.sqlite.SQLiteTrackingRecorder`,
and :class:`~eventsourcing.postgres.PostgresTrackingRecorder`, can be extended arbitrarily to define command
and query methods that update and present a materialised view of the state of an event-sourced application.

The library's abstract base class :class:`~eventsourcing.persistence.TrackingRecorder` can be extended to
define an interface for a materialised view that will be implemented to work concretely with one or many
actual database systems. Separating the interface from the implementations in this way will allow us to
define a projection independently of a particular persistence mechanism.

By extending the library's tracking recorder class, command methods can be more easily implemented to record
:ref:`tracking objects <Tracking objects>` atomically with updates to the materialised view. This is an essential
aspect of ensuring the projection of an event-sourced application will be a reliable deterministic function of
the state of the event-sourced application.

Counting events
===============

To show how this can work, let's build a materialised view that simply counts the events of aggregates in
an event-sourced application. We will count "created" events and also "subsequent" events.

The example :class:`CountRecorderInterface` class, shown below, extends the library's abstract base class
:class:`~eventsourcing.persistence.TrackingRecorder` by defining abstract methods
:func:`incr_created_events_counter`, :func:`incr_subsequent_events_counter`, :func:`get_created_events_counter`,
:func:`get_subsequent_events_counter`, and :func:`get_all_events_counter`.

The command methods expect an argument, :data:`tracking`, which is expected to be an instance of the object class
:class:`~eventsourcing.persistence.Tracking`. The intention of including this argument in the command method
signatures is so that the materialised view can be updated atomically with the recording of tracking information.
This will allow us to make the materialised view be a reliable deterministic function of the state of an event-sourced
application.

.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: CountRecorderInterface

These abstract methods can be implemented by concrete tracking recorder classes. For example, the
:class:`POPOCountRecorder` class, shown below, implements this interface using plain old Python objects.
It inherits and extends the :class:`~eventsourcing.popo.POPOTrackingRecorder` class, using its database lock
to serialise commands, and using its "private" :func:`_insert_tracking` method to insert tracking records.
It defines attributes whose values are incremented by the command methods and returned by the query methods.

.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: POPOCountRecorder

Later in this part of the tutorial, we will also implement the interface to work with PostgreSQL. But now
let's consider how to define how the events of an event-sourced application will be processed.

Event counting projection
=========================

The library's generic abstract base class :class:`~eventsourcing.projection.Projection` can be used to define how
the domain events of an event-sourced application will be processed.

The :class:`~eventsourcing.projection.Projection` class is a generic class because it has one type argument, which is
expected to be a type of tracking recorder.

The :class:`~eventsourcing.projection.Projection` class has one required constructor argument, :data:`tracking_recorder`,
which is expected to be a tracking recorder object of the type specified by the type argument. This constructor argument
will be assigned to the projection object's :data:`tracking_recorder` attribute.

The :class:`~eventsourcing.projection.Projection` class is an abstract class because it defines an abstract method
:func:`~eventsourcing.projection.Projection.process_event` that must be implemented by subclasses. Events will typically
be processed by calling command methods on the projection's tracking recorder.

For example, see the :class:`CountProjection` class below. It inherits the :class:`~eventsourcing.projection.Projection`
class. By stating the type argument of :class:`~eventsourcing.projection.Projection` is :class:`CountRecorder`, we
are specifying that instances of :class:`CountProjection` will use an instance of :class:`CountRecorder` as their
tracking recorder.

The :class:`CountProjection` class implements the abstract method :func:`~eventsourcing.projection.Projection.process_event`
by calling :func:`incr_created_event_count` on an instance of :class:`CountRecorder` for each
:class:`Aggregate.Created <eventsourcing.domain.Aggregate.Created>` event,
and by calling :func:`incr_subsequent_event_count` for each subsequent :class:`Aggregate.Event <eventsourcing.domain.Aggregate.Event>`.

.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: CountProjection


After defining a materialised view that can be updated, and after defining how the events of an event-sourced
projection will be processed, we can now run the projection with an event-sourced application.

Running the projection
======================

The library's :class:`~eventsourcing.projection.ProjectionRunner` class is provided for the purpose
or running projections.

A projection runner object can be constructed with an application class, a projection class, a tracking
recorder class, and an environment that specifies the persistence modules to be used by the application
and the tracking recorder.

The projection runner will construct an instance of the given application class, and an instance of
the given projection class with an instance of the given tracking recorder class. It will
:ref:`subscribe to the application <Subscriptions>`, from the position indicated by its tracking recorder's
:func:`~eventsourcing.persistence.TrackingRecorder.max_tracking_id` method. And then it will call
the :func:`~eventsourcing.projection.Projection.process_event` method of the projection for each domain event
yielded by the application subscription.

Because the projection runner starts a subscription to the application, it will first catch up by
processing already recorded events that have not yet been processed, and then it will continue
to process events that are subsequently recorded in the application's database.

The :class:`~eventsourcing.projection.ProjectionRunner` class has a :func:`~eventsourcing.projection.ProjectionRunner.run_forever`
method, which blocks until an optional timeout, or until an exception is raised by the projection or
by the subscription (exceptions will be re-raised by the :func:`~eventsourcing.projection.ProjectionRunner.run_forever` method).
This allows an event processing component to be started and run independently as a
separate operating system process for a controllable period of time, and then to terminate in a controlled
way when there is an error. Operators of the system can examine the error and resume processing by reconstructing
the runner. Some types of errors may be transient operational errors, such as database connectivity, in which case
the processing could be resumed automatically. Some errors may be programming errors, and will require manual
intervention before the event processing can continue.

The :class:`TestCountProjection` class shown below constructs a :class:`~eventsourcing.projection.ProjectionRunner`
with the library's :class:`~eventsourcing.application.Application` class, the :class:`CountProjection` class,
and the :class:`POPOCountRecorder`.

Aggregates are created and updated in the "write model". The events are counted by the "read model".

.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: TestCountProjection

If the application "write model" and the tracking recorder "read model" use a durable database, such as
PostgreSQL, any instance of the application can be used to write events, and any instance of the tracking
recorder can be used to query the materialised view. However, in this case, using the :ref:`POPO module <popo-module>`
means that we need to use the same instance of the application and of the recorder.


With PostgreSQL
===============

We can also implement the tracking recorder to work with PostgreSQL. As shown below, the :func:`_incr_counter` method
of :class:`PostgresCountRecorder` is used to record a tracking object atomically in the same database transaction as
the event counters are incremented.

.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: PostgresCountRecorder

Because this example uses a durable database, separate instances of the application and the recorder
can be used as interfaces to the "write model" and the "read model".

The application and the projection could use separate databases, but in this example they simply
use different tables in the same database.


.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: TestCountProjectionWithPostgres

See example :doc:`/topics/examples/fts-projection` for a more substantial example.


Exercises
=========

1. Replicate the code in this tutorial in your development environment.

2. Develop a projection that counts dogs and tricks from a `DogSchool` application.


Next steps
==========

* To continue this tutorial, please read :doc:`Part 5 </topics/tutorial/part5>`.
* For more information about event-driven projections, please read
  :doc:`the projection module documentation </topics/projection>`.
* See also the :ref:`Example projections`.
