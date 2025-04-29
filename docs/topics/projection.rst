================================================
:mod:`~eventsourcing.projection` --- Projections
================================================

This module supports projections of :doc:`event-sourced applications </topics/application>` into
materialised views.

The central idea of this module follows the notion from `CQRS <https://en.wikipedia.org/wiki/Command_Query_Responsibility_Segregation>`_
of having separate command and query interfaces. This idea is often implemented in event-sourced systems
by developing distinct and separate "write" and "read" models. The "write model" is an event-sourced application,
and the "read model" is one or many "materialised views" of the event-sourced application. The event-sourced
application is projected into a materialised view by processing the application's events,
usually with an asynchronous event-processing component so that the materialised view is
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

Application subscriptions are conveniently used by event-processing components that project the state of
an event-sourced application into a materialised view, because they both subscribe to an application
sequence using the application's recorder and convert its stored events into domain event objects
by using its mapper.

Application subscription objects are iterators that yield all domain events recorded
in an application sequence. Iterating over an application subscription will block when
all recorded domain events have been yielded, and then continue when new events are recorded.

The :class:`~eventsourcing.projection.ApplicationSubscription` class has three constructor arguments.
The constructor argument `app` is required, and is expected to be an event-sourced application object.
The constructor argument `gt` is optional, and if given is expected to be either a Python `int`
that indicates a position in the application's sequence (a notification ID) or `None`. The constructor
argument `topics` is optional, and if given is expected to be a `tuple` of `str` objects that are the
topics of domain events to be returned by the application subscription.

The application subscription will yield all domain events in the application sequence, except those which have notification
IDs less than or equal to the position given by `gt`, and except those which do not have topics in the sequence given
by `topics` (if any are given). The selection of events by notification ID and the filtering of events by topic will
usually be done in the application's database server.

Application subscription objects usually open a database session, and either listen to the database for
notifications and then select new event records, or otherwise directly stream records from a database. For
this reason, application susbscription objects support the Python context manager protocol, so that database
connection resources can be freed in a controlled way when the subscription is stopped or exits.

Each yielded domain event is accompanied by a tracking object that identifies the position of the
domain event in the application sequence.

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

    with ApplicationSubscription(app, gt=max_tracking_id, topics=()) as subscription:
        for domain_event, tracking in subscription:
            # Process the event and record new state with tracking information.
            break  # ...so we can continue with the examples

The tracking objects yielded by the application subscription can be recorded by an event-processing component
atomically with new state that results from processing the domain event. If an event-processing component is using
a :ref:`tracking recorder <Tracking recorder>` to record new state atomically with tracking objects,
subscriptions can be started from the notification ID returned from the tracking recorder's
:func:`~eventsourcing.persistence.TrackingRecorder.max_tracking_id` method.


.. _Projection:

Projection
==========

The library's :class:`~eventsourcing.projection.Projection` class is a generic abstract base class.
It can be used to define how the domain events of an application will be processed. It is a generic
class because it accepts one type argument, which is expected to be a type of tracking recorder that
defines the interface of a "materialised view". It is an abstract class because it defines an abstract method,
:func:`~eventsourcing.projection.Projection.process_event`, that must be implemented by subclasses.

The :class:`~eventsourcing.projection.Projection` class has one required constructor argument,
:func:`view <eventsourcing.projection.Projection.__init__>`, which is expected to be a concrete
materialised view object of the type specified by the type argument. The constructor argument is
used to initialise the property :py:attr:`~eventsourcing.projection.Projection.view`. The annotated
type of :py:attr:`~eventsourcing.projection.Projection.view` is bound to the type argument of the
class.

The intention of this class is that it will be subclassed, and that a subclass's implementation of
:func:`~eventsourcing.projection.Projection.process_event` will be called for each domain event in
an application sequence. Implementations of :func:`~eventsourcing.projection.Projection.process_event`
will usually handle domain events of different types by calling a command method on the projection's
:py:attr:`~eventsourcing.projection.Projection.view` object.

Subclasses of the :class:`~eventsourcing.projection.Projection` class can optionally specify a
:py:attr:`~eventsourcing.projection.Projection.name` attribute. This attribute will be used
by a projection runner to distinguish environment variables to be used only for constructing
and configuring a projection's materialised view from those to be used only for constructing
and configuring a projected event-sourced application. In some cases, this name will also be
used by the materialised view to name its database tables.

Subclasses of the :class:`~eventsourcing.projection.Projection` class can optionally specify a
:py:attr:`~eventsourcing.projection.Projection.topics` attribute, so that an application subscription
can be more selective when it is used by a projection runner to obtain events for the projection.

The example below shows how a projection can be defined.

.. code-block:: python

    from abc import ABC, abstractmethod
    from eventsourcing.domain import DomainEventProtocol
    from eventsourcing.dispatch import singledispatchmethod
    from eventsourcing.persistence import Tracking
    from eventsourcing.projection import Projection
    from eventsourcing.utils import get_topic

    class MyProjection(Projection["MyMaterialisedViewInterface"]):
        name = "myprojection"
        topics = [get_topic(Aggregate.Event)]

        @singledispatchmethod
        def process_event(self, domain_event: DomainEventProtocol, tracking: Tracking) -> None:
            pass

        @process_event.register
        def _(self, domain_event: Aggregate.Event, tracking: Tracking) -> None:
            self.tracking_recorder.my_command(tracking)


The example below indicates how the projection's materialised view can be defined.

.. code-block:: python

    from abc import ABC, abstractmethod
    from eventsourcing.persistence import TrackingRecorder
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

.. _Projection runner:

Projection runner
=================

This module provides a :class:`~eventsourcing.projection.ProjectionRunner` class, which can be used to run projections.

Projection runner objects can be constructed by calling the :class:`~eventsourcing.projection.ProjectionRunner`
class with an event-sourced application class, a projection class, a materialised view class, and an optional
mapping object that contains environment variables to be used to configure the application and the materialised
view. An application object will be constructed using the application class and the environment variables. An
infrastructure factory will be constructed for the tracking recorder, also using the environment variables.

A projection object will then be constructed using the materialised view. The projection runner will start
a subscription to the application, from the position indicated by the tracking recorder's
:func:`~eventsourcing.persistence.TrackingRecorder.max_tracking_id` method. In a separate thread, the
projection runner will iterate over the application subscription, calling the projection's
:func:`~eventsourcing.projection.Projection.process_event` method for each domain event
and tracking object yielded by the application subscription.

Projection runner objects support the Python context manager protocol, so that database resourced used by
the application subscription and the materialised view can be freed in a controlled way when the projection
runner is stopped or exits.

The projection runner has a method :func:`~eventsourcing.projection.ProjectionRunner.run_forever` which will block
until either :func:`~eventsourcing.projection.Projection.process_event` raises an error, or until
the application subscription raises an error, or until the optional timeout is reached, or until the
:func:`~eventsourcing.projection.ProjectionRunner.stop` method is called.

The example below shows how to run a projection. In this example, the event-sourced application class is
:class:`~eventsourcing.application.Application`. It is constructed with the default
:mod:`eventsourcing.popo` persistence module. The projection class and the materialised view class are taken
from the examples above. The projection runner is used as a context manager. The projection runner's
:func:`~eventsourcing.projection.ProjectionRunner.run_forever` method is called which keeps the projection running.
The :func:`~eventsourcing.projection.ProjectionRunner.stop` method is called by a signal handler when the
operating system process receives an interupt signal. The example below starts a thread which sends the interupt
signal after 1s.

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
