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

Materialised views
==================

Firstly, let's consider the materialised view itself. It will need both command and query methods. The query
methods will support queries needed by users that cannot be supported directly by the event-sourced application
itself. The command methods will be used to update the materialised view when events are processed.

Firstly, the library's abstract base class :class:`~eventsourcing.persistence.TrackingRecorder` can be extended
arbitrarily to define an abstract interface for the materialised view, with abstract command and query methods.
Separating the interface from any concrete implementation will allow us to define how the events of an event-sourced
application will be processed, independently of any particular concrete implementation of the materialised view.

We can then implement the abstract interface by extending the library's concrete tracking recorder classes
(:class:`~eventsourcing.popo.POPOTrackingRecorder`, :class:`~eventsourcing.sqlite.SQLiteTrackingRecorder`,
:class:`~eventsourcing.postgres.PostgresTrackingRecorder`). Using these classes to implement concrete materialised
views means that command methods can be more easily implemented to record :ref:`tracking objects <Tracking objects>`
atomically with updates to the materialised view, which is an essential aspect of ensuring the projection of an
event-sourced application will be a reliable deterministic function of the state of the event-sourced application.

Counting events
===============

To show how this can work, let's build a materialised view that can separately count :ref:`"created" events <Created events>` and
:ref:`subsequent events <Subsequent events>`.

For example, the ``CountRecorderInterface`` class, shown below, extends the library's abstract base class
:class:`~eventsourcing.persistence.TrackingRecorder` by defining abstract command and query methods.
The query methods ``get_created_events_counter()`` and ``get_subsequent_events_counter()`` return integers.
The command methods ``incr_created_events_counter()`` and ``incr_subsequent_events_counter()`` have a ``tracking``
argument, which is expected to be an instance of the library's :class:`~eventsourcing.persistence.Tracking` class.

.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: CountRecorderInterface

These abstract methods can be implemented by concrete tracking recorder classes.

For example, the ``POPOCountRecorder`` class, shown below, implements the abstract methods of ``CountRecorderInterface``
using plain old Python objects. It defines "private" attributes ``_created_events_counter`` and
``_subsequent_events_counter`` whose values, being initially zero, can be incremented by the
command methods and returned by the query methods.
It inherits and extends the :class:`~eventsourcing.popo.POPOTrackingRecorder` class,
using its database lock to serialise commands and its "private"
:func:`~eventsourcing.popo.POPOTrackingRecorder._assert_tracking_uniqueness` and
:func:`~eventsourcing.popo.POPOTrackingRecorder._insert_tracking` methods to avoid
processing any event more than once whilst keeping track of which events have been processed
so that event-processing can be resumed correctly.

.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: POPOCountRecorder


Below, we will also implement ``CountRecorderInterface`` to work with PostgreSQL. But now
let's consider how the events of an event-sourced application will be processed.

Event counting projection
=========================

The library's generic abstract base class :class:`~eventsourcing.projection.Projection` can be used to define how
the domain events of an event-sourced application will be processed. It is intended to be subclassed by users.

.. literalinclude:: ../../../eventsourcing/projection.py
    :pyobject: Projection

The :class:`~eventsourcing.projection.Projection` class is a `generic` class because it has one type argument, which is
expected to be the abstract interface of a materialised view that is also a subclass of
:class:`~eventsourcing.persistence.TrackingRecorder`. The type argument should be specified by users when defining a subclass of
:class:`~eventsourcing.projection.Projection`.

The :class:`~eventsourcing.projection.Projection` class has one required constructor argument,
:func:`view <eventsourcing.projection.Projection.__init__>`. This argument's type is bound
to the type argument of the class, and so should be a concrete instance of a materialised view.
This constructor argument will be assigned as an attribute of the constructed projection
object, and will be available to be used by subclass methods via the :data:`~eventsourcing.projection.Projection.view`
property, which is also typed with the type argument of the class.

The :class:`~eventsourcing.projection.Projection` class is an `abstract` class because it defines an abstract method
:func:`~eventsourcing.projection.Projection.process_event` that must be implemented by subclasses. Events will typically
be processed by calling command methods on the projection's tracking recorder, accessed via the :data:`~eventsourcing.projection.Projection.view`
property.

For example, see the ``CountProjection`` class below. It inherits the :class:`~eventsourcing.projection.Projection`
class. It specifies the type argument is ``CountRecorderInterface``. It implements the abstract method
:func:`~eventsourcing.projection.Projection.process_event` by calling ``incr_created_event_count()``
or ``incr_subsequent_event_count()`` on its :data:`~eventsourcing.projection.Projection.view`, according to whether
the given event is an :class:`Aggregate.Created <eventsourcing.domain.Aggregate.Created>` event or an
:class:`Aggregate.Event <eventsourcing.domain.Aggregate.Event>`.

.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: CountProjection


Running the projection
======================

Let's consider how to run the projection, so events of an event-sourced application can be counted.

The library's :class:`~eventsourcing.projection.ProjectionRunner` class is provided for the purpose
of running projections. A projection runner can be constructed with an application class, a projection
class, a tracking recorder class, and an environment that specifies the persistence modules
to be used by the application and the tracking recorder.

The projection runner will construct an instance of the given application class, and an instance of
the given projection class with an instance of the given tracking recorder class.

It will :ref:`subscribe to the application <Subscriptions>`, from the position indicated by the
:func:`~eventsourcing.persistence.TrackingRecorder.max_tracking_id` method of the  projection's
tracking recorder, and then call the :func:`~eventsourcing.projection.Projection.process_event`
method of the projection for each domain event yielded by the application subscription.

Because the projection runner starts a subscription to the application, it will first catch up by
processing already recorded events that have not yet been processed, and then it will continue
to process events that are subsequently recorded in the application's database.

The :class:`~eventsourcing.projection.ProjectionRunner` class has a :func:`~eventsourcing.projection.ProjectionRunner.run_forever`
method, which blocks until an optional timeout, or until an exception is raised by the projection or
by the subscription, or until the projection runner is stopped by calling its :func:`~eventsourcing.projection.ProjectionRunner.stop` method.
This allows an event processing component to be started and run independently as a
separate operating system process for a controllable period of time, and then to terminate in a controlled
way when there is an error. Exceptions raised whilst running the projection will be re-raised by the
:func:`~eventsourcing.projection.ProjectionRunner.run_forever` method. Operators of the system can examine
any errors and resume processing by reconstructing the runner. Some types of errors may be transient operational
errors, such as database connectivity, in which case the processing could be resumed automatically. Some errors
may be programming errors, and will require manual intervention before the event processing can continue.

The :func:`~eventsourcing.persistence.TrackingRecorder.wait` method of tracking recorders can be used
to wait until an event has been processed by the projection before calling a query method on the materialised view.

The ``TestCountProjection`` class shown below constructs a :class:`~eventsourcing.projection.ProjectionRunner`
with the library's :class:`~eventsourcing.application.Application` class, and the ``CountProjection``
and ``POPOCountRecorder`` classes.

Aggregates are created and updated in the "write model". The events are counted by the "read model".

.. literalinclude:: ../../../tests/projection_tests/test_projection.py
    :pyobject: TestCountProjection

If the application "write model" and the tracking recorder "read model" use a durable database, such as
PostgreSQL, any instance of the application can be used to write events, and any instance of the tracking
recorder can be used to query the materialised view. However, in this case, using the :ref:`POPO module <popo-module>`
means that we need to use the same instance of the application and of the recorder.


With PostgreSQL
===============

We can also implement ``CountRecorderInterface`` to work with PostgreSQL. As shown below, the ``_incr_counter()`` method
of ``PostgresCountRecorder`` is used to record a tracking object atomically in the same database transaction as
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

2. Develop a projection that counts dogs and tricks from a ``DogSchool`` application.


Next steps
==========

* To continue this tutorial, please read :doc:`Part 5 </topics/tutorial/part5>`.
* For more information about event-driven projections, please read
  :doc:`the projection module documentation </topics/projection>`.
* See also the :ref:`Example projections`.
