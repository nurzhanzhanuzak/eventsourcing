from __future__ import annotations

import contextlib
import os
import threading
import weakref
from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from threading import Event, Thread
from traceback import format_exc
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from warnings import warn

from eventsourcing.application import Application, ProcessingEvent
from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.domain import DomainEventProtocol, TAggregateID
from eventsourcing.persistence import (
    InfrastructureFactory,
    Mapper,
    ProcessRecorder,
    Tracking,
    TrackingRecorder,
    TTrackingRecorder,
    WaitInterruptedError,
)
from eventsourcing.utils import Environment, EnvType

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Self


class ApplicationSubscription(
    Iterator[tuple[DomainEventProtocol[TAggregateID], Tracking]]
):
    """An iterator that yields all domain events recorded in an application
    sequence that have notification IDs greater than a given value. The iterator
    will block when all recorded domain events have been yielded, and then
    continue when new events are recorded. Domain events are returned along
    with tracking objects that identify the position in the application sequence.
    """

    def __init__(
        self,
        app: Application[TAggregateID],
        gt: int | None = None,
        topics: Sequence[str] = (),
    ):
        """
        Starts a subscription to application's recorder.
        """
        self.name = app.name
        self.recorder = app.recorder
        self.mapper: Mapper[TAggregateID] = app.mapper
        self.subscription = self.recorder.subscribe(gt=gt, topics=topics)

    def stop(self) -> None:
        """Stops the subscription to the application's recorder."""
        self.subscription.stop()

    def __enter__(self) -> Self:
        """Calls __enter__ on the stored event subscription."""
        self.subscription.__enter__()
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        """Calls __exit__ on the stored event subscription."""
        self.subscription.__exit__(*args, **kwargs)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> tuple[DomainEventProtocol[TAggregateID], Tracking]:
        """Returns the next stored event from subscription to the application's
        recorder. Constructs a tracking object that identifies the position of
        the event in the application sequence. Constructs a domain event object
        from the stored event object using the application's mapper. Returns a
        tuple of the domain event object and the tracking object.
        """
        notification = next(self.subscription)
        tracking = Tracking(self.name, notification.id)
        domain_event = self.mapper.to_domain_event(notification)
        return domain_event, tracking

    def __del__(self) -> None:
        """Stops the stored event subscription."""
        with contextlib.suppress(AttributeError):
            self.stop()


class Projection(ABC, Generic[TTrackingRecorder]):
    name: str = ""
    """
    Name of projection, used to pick prefixed environment
    variables and define database table names.
    """
    topics: tuple[str, ...] = ()
    """
    Event topics, used to filter events in database when subscribing to an application.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        if "name" not in cls.__dict__:
            cls.name = cls.__name__

    def __init__(
        self,
        view: TTrackingRecorder,
    ):
        """Initialises the view property with the given view argument."""
        self._view = view

    @property
    def view(self) -> TTrackingRecorder:
        """Materialised view of an event-sourced application."""
        return self._view

    @singledispatchmethod
    @abstractmethod
    def process_event(
        self, domain_event: DomainEventProtocol[TAggregateID], tracking: Tracking
    ) -> None:
        """Process a domain event and track it."""


class EventSourcedProjection(Application[TAggregateID], ABC):
    """Extends the :py:class:`~eventsourcing.application.Application` class
    by using a process recorder as its application recorder, and by
    processing domain events through its :py:func:`policy` method.
    """

    topics: Sequence[str] = ()

    def __init__(self, env: EnvType | None = None) -> None:
        super().__init__(env)
        self.recorder: ProcessRecorder
        self.processing_lock = threading.Lock()

    def construct_recorder(self) -> ProcessRecorder:
        """Constructs and returns a :class:`~eventsourcing.persistence.ProcessRecorder`
        for the application to use as its application recorder.
        """
        return self.factory.process_recorder()

    def process_event(
        self, domain_event: DomainEventProtocol[TAggregateID], tracking: Tracking
    ) -> None:
        """Calls :func:`~eventsourcing.system.Follower.policy` method with the given
        domain event and a new :class:`~eventsourcing.application.ProcessingEvent`
        constructed with the given tracking object.

        The policy method should collect any new aggregate events on the process
        event object.

        After the policy method returns, the processing event object will be recorded
        by calling :py:func:`~eventsourcing.application.Application._record`,
        which then returns list of :py:class:`~eventsourcing.persistence.Recording`.

        After calling :func:`~eventsourcing.application.Application._take_snapshots`,
        the recordings are passed in a call to
        :py:func:`~eventsourcing.application.Application._notify`.
        """
        processing_event = ProcessingEvent[TAggregateID](tracking=tracking)
        self.policy(domain_event, processing_event)
        recordings = self._record(processing_event)
        self._take_snapshots(processing_event)
        self.notify(processing_event.events)
        self._notify(recordings)

    @singledispatchmethod
    def policy(
        self,
        domain_event: DomainEventProtocol[TAggregateID],
        processing_event: ProcessingEvent[TAggregateID],
    ) -> None:
        """Abstract domain event processing policy method. Must be
        implemented by event processing applications. When
        processing the given domain event, event processing
        applications must use the :func:`~ProcessingEvent.collect_events`
        method of the given :py:class:`~ProcessingEvent` object (not
        the application's :func:`~eventsourcing.application.Application.save`
        method) so that the new domain events will be recorded atomically
        and uniquely with tracking information about the position of the processed
        event in its application sequence.
        """


TApplication = TypeVar("TApplication", bound=Application[Any])
TEventSourcedProjection = TypeVar(
    "TEventSourcedProjection", bound=EventSourcedProjection[Any]
)


class BaseProjectionRunner(Generic[TApplication]):
    def __init__(
        self,
        *,
        projection: EventSourcedProjection[Any] | Projection[Any],
        application_class: type[TApplication],
        tracking_recorder: TrackingRecorder,
        topics: Sequence[str],
        env: EnvType | None = None,
    ) -> None:
        self._projection = projection
        self._is_interrupted = Event()
        self._has_called_stop = False

        # Construct the application.
        self.app: TApplication = application_class(env)

        self._tracking_recorder = tracking_recorder

        # Subscribe to the application.
        self._subscription = ApplicationSubscription(
            app=self.app,
            gt=self._tracking_recorder.max_tracking_id(self.app.name),
            topics=topics,
        )

        # Start a thread to stop the subscription when the runner is interrupted.
        self._thread_error: BaseException | None = None
        self._stop_thread = Thread(
            target=self._stop_subscription_when_stopping,
            kwargs={
                "subscription": self._subscription,
                "is_stopping": self._is_interrupted,
            },
        )
        self._stop_thread.start()

        # Start a thread to iterate over the subscription.
        self._processing_thread = Thread(
            target=self._process_events_loop,
            kwargs={
                "subscription": self._subscription,
                "projection": self._projection,
                "is_stopping": self._is_interrupted,
                "runner": weakref.ref(self),
            },
        )
        self._processing_thread.start()

    @property
    def is_interrupted(self) -> Event:
        return self._is_interrupted

    @staticmethod
    def _construct_env(name: str, env: EnvType | None = None) -> Environment:
        """Constructs environment from which projection will be configured."""
        _env: dict[str, str] = {}
        _env.update(os.environ)
        if env is not None:
            _env.update(env)
        return Environment(name, _env)

    def stop(self) -> None:
        """Sets the "interrupted" event."""
        self._has_called_stop = True
        self._is_interrupted.set()

    @staticmethod
    def _stop_subscription_when_stopping(
        subscription: ApplicationSubscription[TAggregateID],
        is_stopping: Event,
    ) -> None:
        """Stops the application subscription, which
        will stop the event-processing thread.
        """
        try:
            is_stopping.wait()
        finally:
            is_stopping.set()
            subscription.stop()

    @staticmethod
    def _process_events_loop(
        subscription: ApplicationSubscription[TAggregateID],
        projection: EventSourcedProjection[Any] | Projection[Any],
        is_stopping: Event,
        runner: weakref.ReferenceType[
            ProjectionRunner[Application[Any], TrackingRecorder]
        ],
    ) -> None:
        """Iterates over the subscription and calls process_event()."""
        try:
            with subscription:
                for domain_event, tracking in subscription:
                    projection.process_event(domain_event, tracking)
        except BaseException as e:
            _runner = runner()  # get reference from weakref
            if _runner is not None:
                _runner._thread_error = e  # noqa: SLF001
            else:
                msg = "ProjectionRunner was deleted before error could be assigned:\n"
                msg += format_exc()
                warn(
                    msg,
                    RuntimeWarning,
                    stacklevel=2,
                )
        finally:
            is_stopping.set()

    def run_forever(self, timeout: float | None = None) -> None:
        """Blocks until timeout, or until the runner is stopped or errors. Re-raises
        any error otherwise exits normally
        """
        if (
            self._is_interrupted.wait(timeout=timeout)
            and self._thread_error is not None
        ):
            error = self._thread_error
            self._thread_error = None
            raise error

    def wait(self, notification_id: int | None, timeout: float = 1.0) -> None:
        """Blocks until timeout, or until the materialised view has recorded a tracking
        object that is greater than or equal to the given notification ID.
        """
        try:
            self._tracking_recorder.wait(
                application_name=self.app.name,
                notification_id=notification_id,
                timeout=timeout,
                interrupt=self._is_interrupted,
            )
        except WaitInterruptedError:
            if self._thread_error:
                error = self._thread_error
                self._thread_error = None
                raise error from None
            if self._has_called_stop:
                return
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Calls stop() and waits for the event-processing thread to exit."""
        self.stop()
        self._stop_thread.join()
        self._processing_thread.join()
        if self._thread_error:
            error = self._thread_error
            self._thread_error = None
            raise error

    def __del__(self) -> None:
        """Calls stop()."""
        with contextlib.suppress(AttributeError):
            self.stop()


class ProjectionRunner(
    BaseProjectionRunner[TApplication], Generic[TApplication, TTrackingRecorder]
):
    def __init__(
        self,
        *,
        application_class: type[TApplication],
        projection_class: type[Projection[TTrackingRecorder]],
        view_class: type[TTrackingRecorder],
        env: EnvType | None = None,
    ):
        """Constructs application from given application class with given environment.
        Also constructs a materialised view from given class using an infrastructure
        factory constructed with an environment named after the projection. Also
        constructs a projection with the constructed materialised view object.
        Starts a subscription to application and, in a separate event-processing
        thread, calls projection's process_event() method for each event and tracking
        object pair received from the subscription.
        """
        # Construct the materialised view using an infrastructure factory.
        self.view = (
            InfrastructureFactory[TTrackingRecorder]
            .construct(env=self._construct_env(name=projection_class.name, env=env))
            .tracking_recorder(view_class)
        )

        # Construct the projection using the materialised view.
        self.projection = projection_class(view=self.view)

        super().__init__(
            projection=self.projection,
            application_class=application_class,
            tracking_recorder=self.view,
            topics=self.projection.topics,
            env=env,
        )


class EventSourcedProjectionRunner(
    BaseProjectionRunner[TApplication], Generic[TApplication, TEventSourcedProjection]
):
    def __init__(
        self,
        *,
        application_class: type[TApplication],
        projection_class: type[TEventSourcedProjection],
        env: EnvType | None = None,
    ):
        self.projection: TEventSourcedProjection = projection_class(
            env=self._construct_env(name=projection_class.name, env=env)
        )

        super().__init__(
            projection=self.projection,
            application_class=application_class,
            tracking_recorder=self.projection.recorder,
            topics=self.projection.topics,
            env=env,
        )
