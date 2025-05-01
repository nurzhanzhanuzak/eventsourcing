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

Application subscription objects are iterators that return domain events from an application sequence.
Each domain event is accompanied by a tracking object that identifies the position of the
domain event in the application sequence. Iterating over an application subscription will block when
all recorded domain events have been returned, and then continue when new events are recorded. Application
subscriptions are conveniently used by event-processing components that project the state of an event-sourced
application into a materialised view, because they continue returning newly recorded events, because they subscribe
to a database rather than an application object, and because they convert the stored events returned by an
application recorder into domain event objects using the application's mapper. Encapsulating all of these concerns
provides a convenient way to follow the domain events of an event-sourced application.

The :class:`~eventsourcing.projection.ApplicationSubscription` class has three constructor arguments,
:data:`app <eventsourcing.projection.ApplicationSubscription.__init__>`,
:data:`gt <eventsourcing.projection.ApplicationSubscription.__init__>`, and
:data:`topics <eventsourcing.projection.ApplicationSubscription.__init__>`.

The constructor argument :data:`app <eventsourcing.projection.ApplicationSubscription.__init__>` is required,
and is expected to be an event-sourced application object.

The constructor argument :data:`gt <eventsourcing.projection.ApplicationSubscription.__init__>` is optional,
and if given is expected to be either a Python :class:`int` that indicates a position in the application's sequence
(a notification ID) or ``None``. This matches the return type of the :func:`~eventsourcing.persistence.TrackingRecorder.max_tracking_id`
method of tracking recorders. The intention here is that the :func:`~eventsourcing.persistence.TrackingRecorder.max_tracking_id`
method of a downstream event-processing component's tracking recorder (or equivalent) can be called and the value used
to start a subscription to an upstream event-sourced application from the correct position.

The constructor argument :data:`topics <eventsourcing.projection.ApplicationSubscription.__init__>` is optional,
and if given is expected to be a Python :class:`tuple` of :class:`str` objects that are the :ref:`topics <Topics>`
of domain events to be returned by the application subscription. The purpose of this argument is to filter events
within the event-sourced application's database, avoiding the cost of transporting and reconstructing events that
will just be ignored by an event-processing component. If a non-empty sequence of topics is provided, only events
that have topics mentioned in this collection will be returned by the subscription. An empty sequence of topics,
which is the default value, will mean events will not be filtered by topic.

An application subscription will return all domain events in the application sequence, except those which have notification
IDs less than or equal to the position given by :data:`gt <eventsourcing.projection.ApplicationSubscription.__init__>`,
and except those which do not have topics in the sequence given by :data:`topics <eventsourcing.projection.ApplicationSubscription.__init__>`
if any are given. The selection of events by notification ID and the filtering of events by topic will
usually be done in the application's database server.

Application subscription objects usually open a database session, and either listen to the database for
notifications and then select new event records, or otherwise directly stream records from a database. For
this reason, application subscription objects support the Python context manager protocol, so that database
connection resources can be freed in a controlled and convenient way when the subscription is stopped or exits.

Alternatively, application subscription objects have a :func:`~eventsourcing.projection.ApplicationSubscription.stop`
method which can be used to stop the subscription to the application recorder in a controlled way.

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
            subscription.stop()  # ...so we can continue with the examples


Please note, the :class:`~eventsourcing.popo.POPOApplicationRecorder` and
:class:`~eventsourcing.postgres.PostgresApplicationRecorder` classes implement the
required :func:`~eventsourcing.persistence.ApplicationRecorder.subscribe`
method, but the :class:`~eventsourcing.sqlite.SQLiteApplicationRecorder` class does not.

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
and tracking object returned by the application subscription.

Projection runner objects support the Python context manager protocol, so that database resources used by
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
operating system process receives an interrupt signal. The example below starts a thread which sends the interrupt
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


The intention of a projection runner is to operate as a separate event-processing component,
with potentially many instances of an upstream event-processing application, and many instances
of a downstream materialised view, operating in different operating system processes. A user interface
may transition from sending a command that results in new events being written to the "write model"
over to presenting the results of querying the "read model". Since the "read model" is eventually-consistent,
and so may not immediately have been updated by processing the new events, running the risk that the view
presented to a user will appear to be stale by not reflecting their recent work, the notification IDs
returned from calls to the event-sourced application's :func:`~eventsourcing.application.Application.save`
method can be used by the user interface to :func:`~eventsourcing.persistence.TrackingRecorder.wait` until
the "read model" has been updated.

See :doc:`Tutorial - Part 4 </topics/tutorial/part4>` for more guidance on using this module.

Code reference
==============

.. automodule:: eventsourcing.projection
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__, __enter__, __exit__, __iter__, __next__
