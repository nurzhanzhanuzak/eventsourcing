================================================
:mod:`~eventsourcing.projection` --- Projections
================================================

This module may help you develop event-processing components that project
the state of an :doc:`event-sourced applications </topics/application>` into materialised
views that support arbitrary queries.

The central idea of this module follows the notion from `CQRS <https://en.wikipedia.org/wiki/Command_Query_Responsibility_Segregation>`_
of having separate command and query interfaces. This idea is often implemented in event-sourced systems
by developing distinct and separate "write" and "read" models. The "write model" is an event-sourced application,
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

.. _Subscriptions:

Application subscriptions
=========================

This module provides an :class:`~eventsourcing.projection.ApplicationSubscription` class, which can
be used to "subscribe" to the domain events of an application.

Application subscriptions are useful when running an event processing component
that projects the state of an event-sourced application into a materialised view
of the state of the application.

Application subscription objects are iterators that yield all domain events recorded
in an application sequence. Iterating over an application subscription will block when
all recorded domain events have been yielded, and then continue when new events are recorded.

Application subscription objects can be constructed with an application object, and an integer
position in its application sequence (a notification ID). The application subscription will yield
domain events that have notification IDs greater than the given position.

Application subscription objects use the :func:`~eventsourcing.persistence.ApplicationRecorder.subscribe`
method of the application's recorder to listen to the application's database, selecting notification objects
and converting them into domain events using the application's mapper.

Each yielded domain event is accompanied by a tracking object that identifies the position of the
domain event in its application sequence. The tracking objects yielded by the application subscription
can be recorded atomically along with the new state that results from processing the domain event.

.. code-block:: python

    from eventsourcing.application import Application
    from eventsourcing.domain import Aggregate
    from eventsourcing.projection import ApplicationSubscription

    # Construct an application object.
    app = Application()

    # Record an event.
    aggregate = Aggregate()
    app.save(aggregate)

    # Position in application sequence from which to subscribe.
    max_tracking_id = 0

    with ApplicationSubscription(app, gt=max_tracking_id) as subscription:
        for domain_event, tracking in subscription:
            # Process the event and record new state with tracking information.
            break  # ...so we can continue with the examples

If an event-processing component is using a :ref:`tracking recorder <Tracking recorder>` to record new state atomically
with tracking objects, subscriptions can be started from the notification ID returned from the tracking recorder's
:func:`~eventsourcing.persistence.TrackingRecorder.max_tracking_id` method.


.. _Projection:

Projection
==========

This module provides a generic abstract base class, :class:`~eventsourcing.projection.Projection`,
which can be used to define how the domain events of an application will be processed.

The :class:`~eventsourcing.projection.Projection` class is a generic class because it has one type variable,
which is expected to be a type of tracking recorder that defines the interface of a "materialised view".

The :class:`~eventsourcing.projection.Projection` class has one required constructor argument, :func:`view <eventsourcing.projection.Projection.__init__>`,
which is expected to be a concrete materialised view object of the type specified by the type variable. The constructor
argument is used to initialise the property :py:attr:`~eventsourcing.projection.Projection.view`.

The :class:`~eventsourcing.projection.Projection` class is an abstract class because it defines an abstract method,
:func:`~eventsourcing.projection.Projection.process_event`, that must be implemented by subclasses.

The intention of this class is that it will be subclassed, and that domain events of an application will be processed by
calling an implementation of the :func:`~eventsourcing.projection.Projection.process_event`, which will call command
methods on :py:attr:`~eventsourcing.projection.Projection.view`.

Subclasses of the :class:`~eventsourcing.projection.Projection` class can specify a
:py:attr:`~eventsourcing.projection.Projection.name` attribute, so that environment variables
used by projection runners to construct materialised view objects can be prefixed and (in some cases)
so that database tables can be named after the projection.

Subclasses of the :class:`~eventsourcing.projection.Projection` class can specify a
:py:attr:`~eventsourcing.projection.Projection.topics` attribute, so that events can be
filtered in the application's database by the application subscription.

The examples below indicate how materialised views can be defined.

.. code-block:: python

    from abc import ABC, abstractmethod
    from eventsourcing.persistence import Tracking, TrackingRecorder
    from eventsourcing.popo import POPOTrackingRecorder
    from eventsourcing.postgres import PostgresTrackingRecorder
    from eventsourcing.sqlite import SQLiteTrackingRecorder

    class MyMaterialisedViewInterface(TrackingRecorder, ABC):
        @abstractmethod
        def my_command(self, tracking: Tracking):
            """Updates materialised view"""

    class MyPOPOMaterialisedView(MyMaterialisedViewInterface, POPOTrackingRecorder):
        def my_command(self, tracking: Tracking):
            with self._datastore:
                # Insert tracking record...
                self._insert_tracking(tracking)
                # ...and then update materialised view.

    class MySQLiteMaterialisedView(MyMaterialisedViewInterface, SQLiteTrackingRecorder):
        def my_command(self, tracking: Tracking):
            ...

    class MyPostgresMaterialisedView(MyMaterialisedViewInterface, PostgresTrackingRecorder):
        def my_command(self, tracking: Tracking):
            ...

The example below indicates how a projection can be defined.

.. code-block:: python

    from abc import ABC, abstractmethod
    from eventsourcing.domain import DomainEventProtocol
    from eventsourcing.dispatch import singledispatchmethod
    from eventsourcing.projection import Projection
    from eventsourcing.utils import get_topic

    class MyProjection(Projection[MyMaterialisedViewInterface]):
        name = "myprojection"
        topics = [get_topic(Aggregate.Event)]

        @singledispatchmethod
        def process_event(self, domain_event: DomainEventProtocol, tracking: Tracking) -> None:
            pass

        @process_event.register
        def _(self, domain_event: Aggregate.Event, tracking: Tracking) -> None:
            self.tracking_recorder.my_command(tracking)


.. _Projection runner:

Projection runner
=================

This module provides a :class:`~eventsourcing.projection.ProjectionRunner` class, which can be used to run projections.

Projection runner objects can be constructed by calling the class with an application class, a projection class,
and a tracking recorder class, and an optional environment. An application object will be constructed using the
application class and the environment. An infrastructure factory will be constructed for the tracking recorder,
also using the environment. A projection will also be constructed using the tracking recorder.

The projection runner will then start a subscription to the application, from the position indicated by the tracking
recorder's :func:`~eventsourcing.persistence.TrackingRecorder.max_tracking_id` method.

The projection runner will iterate over the application subscription, calling the projection's
:func:`~eventsourcing.projection.Projection.process_event` method for each domain event
and tracking object yielded by the application subscription.

The projection runner has a method :func:`~eventsourcing.projection.ProjectionRunner.run_forever` which will block
until either :func:`~eventsourcing.projection.Projection.process_event` raises an error, or until
the application subscription raises an error, or until the optional timeout is reached, or until the
:func:`~eventsourcing.projection.ProjectionRunner.stop` method is called.

The example below shows how to run a projection. In this example, the projection runner is used as a context manager.
The event-sourced application is an instance of :class:`~eventsourcing.application.Application` constructed with
the default :mod:`eventsourcing.popo` persistence module. The projection runner's
:func:`~eventsourcing.projection.ProjectionRunner.run_forever` method is called which keeps the projection running.
The :func:`~eventsourcing.projection.ProjectionRunner.stop` method is called by a signal handler after the
process is interrupted.

.. code-block:: python

    import signal
    from eventsourcing.projection import ProjectionRunner

    # For demonstration purposes, interrupt process with SIGINT after 1s.
    import os, threading, time
    threading.Thread(target=lambda: time.sleep(1) or os.kill(os.getpid(), signal.SIGINT)).start()

    # Run projection as a context manager.
    with ProjectionRunner(
        application_class=Application,
        view_class=MyPOPOMaterialisedView,
        projection_class=MyProjection,
        env={},
    ) as runner:

        # Register signal handler.
        signal.signal(signal.SIGINT, lambda *args: runner.stop())

        # Run until interrupted.
        runner.run_forever()


See :doc:`Tutorial - Part 4 </topics/tutorial/part4>` for more guidance on using this module.

Code reference
==============

.. automodule:: eventsourcing.projection
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__, __enter__, __exit__, __iter__, __next__
