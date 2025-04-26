from __future__ import annotations

import contextlib
import os
import weakref
from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from threading import Event, Thread
from traceback import format_exc
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from warnings import warn

from eventsourcing.application import Application
from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.domain import DomainEventProtocol
from eventsourcing.persistence import (
    InfrastructureFactory,
    Tracking,
    TrackingRecorder,
    TTrackingRecorder,
    WaitInterruptedError,
)
from eventsourcing.utils import Environment, EnvType

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Self


class ApplicationSubscription(Iterator[tuple[DomainEventProtocol, Tracking]]):
    """
    An iterator that yields all domain events recorded in an application
    sequence that have notification IDs greater than a given value. The iterator
    will block when all recorded domain events have been yielded, and then
    continue when new events are recorded. Domain events are returned along
    with tracking objects that identify the position in the application sequence.
    """

    def __init__(
        self,
        app: Application,
        gt: int | None = None,
        topics: Sequence[str] = (),
    ):
        """
        Starts subscription to application's stored events using application's recorder.
        """
        self.name = app.name
        self.recorder = app.recorder
        self.mapper = app.mapper
        self.subscription = self.recorder.subscribe(gt=gt, topics=topics)

    def stop(self) -> None:
        """
        Stops the stored event subscription.
        """
        self.subscription.stop()

    def __enter__(self) -> Self:
        """
        Calls __enter__ on the stored event subscription.
        """
        self.subscription.__enter__()
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        """
        Calls __exit__ on the stored event subscription.
        """
        self.subscription.__exit__(*args, **kwargs)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> tuple[DomainEventProtocol, Tracking]:
        """
        Returns the next stored event from the stored event subscription.
        Constructs a tracking object that identifies the position of
        the event in the application sequence, and reconstructs a domain
        event object from the stored event object.
        """
        notification = next(self.subscription)
        tracking = Tracking(self.name, notification.id)
        domain_event = self.mapper.to_domain_event(notification)
        return domain_event, tracking

    def __del__(self) -> None:
        """
        Stops the stored event subscription.
        """
        self.stop()


class Projection(ABC, Generic[TTrackingRecorder]):
    name: str = ""
    """
    Name of projection, used to pick prefixed environment
    variables and define database table names.
    """
    topics: tuple[str, ...] = ()
    """
    Filter events in database when subscribing to an application.
    """

    def __init__(
        self,
        view: TTrackingRecorder,
    ):
        """Initialises a projection instance."""
        self._view = view

    @property
    def view(self) -> TTrackingRecorder:
        """Materialised view of an event-sourced application."""
        return self._view

    @singledispatchmethod
    @abstractmethod
    def process_event(
        self, domain_event: DomainEventProtocol, tracking: Tracking
    ) -> None:
        """
        Process a domain event and track it.
        """


TApplication = TypeVar("TApplication", bound=Application)


class ProjectionRunner(Generic[TApplication, TTrackingRecorder]):
    def __init__(
        self,
        *,
        application_class: type[TApplication],
        projection_class: type[Projection[TTrackingRecorder]],
        view_class: type[TTrackingRecorder],
        env: EnvType | None = None,
    ):
        """
        Constructs application from given application class with given environment.
        Also constructs a materialised view from given class using an infrastructure
        factory constructed with an environment named after the projection. Also
        constructs a projection with the constructed materialised view object.
        Starts a subscription to application and, in a separate event-processing
        thread, calls projection's process_event() method for each event and tracking
        object pair received from the subscription.
        """
        self._is_interrupted = Event()
        self._has_called_stop = False

        self.app: TApplication = application_class(env)

        self.view = (
            InfrastructureFactory[TTrackingRecorder]
            .construct(
                env=self._construct_env(
                    name=projection_class.name or projection_class.__name__, env=env
                )
            )
            .tracking_recorder(view_class)
        )

        self.projection = projection_class(
            view=self.view,
        )
        self.subscription = ApplicationSubscription(
            app=self.app,
            gt=self.view.max_tracking_id(self.app.name),
            topics=self.projection.topics,
        )
        self._thread_error: BaseException | None = None
        self._stop_thread = Thread(
            target=self._stop_subscription_when_stopping,
            kwargs={
                "subscription": self.subscription,
                "is_stopping": self._is_interrupted,
            },
        )
        self._stop_thread.start()
        self._processing_thread = Thread(
            target=self._process_events_loop,
            kwargs={
                "subscription": self.subscription,
                "projection": self.projection,
                "is_stopping": self._is_interrupted,
                "runner": weakref.ref(self),
            },
        )
        self._processing_thread.start()

    @property
    def is_interrupted(self) -> Event:
        return self._is_interrupted

    def _construct_env(self, name: str, env: EnvType | None = None) -> Environment:
        """
        Constructs environment from which projection will be configured.
        """
        _env: dict[str, str] = {}
        _env.update(os.environ)
        if env is not None:
            _env.update(env)
        return Environment(name, _env)

    def stop(self) -> None:
        """
        Sets the "interrupted" event.
        """
        self._has_called_stop = True
        self._is_interrupted.set()

    @staticmethod
    def _stop_subscription_when_stopping(
        subscription: ApplicationSubscription,
        is_stopping: Event,
    ) -> None:
        """
        Stops the application subscription, which will stop the event-processing thread.
        """
        try:
            is_stopping.wait()
        finally:
            is_stopping.set()
            subscription.stop()

    @staticmethod
    def _process_events_loop(
        subscription: ApplicationSubscription,
        projection: Projection[TrackingRecorder],
        is_stopping: Event,
        runner: weakref.ReferenceType[ProjectionRunner[Application, TrackingRecorder]],
    ) -> None:
        try:
            with subscription:
                for domain_event, tracking in subscription:
                    projection.process_event(domain_event, tracking)
        except BaseException as e:
            _runner = runner()  # get reference from weakref
            if _runner is not None:
                _runner._thread_error = e
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
        """
        Blocks until timeout, or until the runner is stopped or errors. Re-raises
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
        """
        Blocks until timeout, or until the materialised view has recorded a tracking
        object that is greater than or equal to the given notification ID.
        """
        try:
            self.projection.view.wait(
                application_name=self.subscription.name,
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
        """
        Calls stop() and waits for the event-processing thread to exit.
        """
        self.stop()
        self._stop_thread.join()
        self._processing_thread.join()
        if self._thread_error:
            error = self._thread_error
            self._thread_error = None
            raise error

    def __del__(self) -> None:
        """
        Calls stop().
        """
        with contextlib.suppress(AttributeError):
            self.stop()
