from __future__ import annotations

import os
import weakref
from abc import ABC, abstractmethod
from threading import Event, Thread
from traceback import format_exc
from typing import TYPE_CHECKING, Any, Dict, Generic, Iterator, Tuple, Type, TypeVar
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
    from typing_extensions import Self


class ApplicationSubscription(Iterator[Tuple[DomainEventProtocol, Tracking]]):
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
    ):
        self.name = app.name
        self.recorder = app.recorder
        self.mapper = app.mapper
        self.subscription = self.recorder.subscribe(gt=gt)

    def __enter__(self) -> Self:
        self.subscription.__enter__()
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        self.subscription.__exit__(*args, **kwargs)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Tuple[DomainEventProtocol, Tracking]:
        notification = next(self.subscription)
        tracking = Tracking(self.name, notification.id)
        domain_event = self.mapper.to_domain_event(notification)
        return domain_event, tracking

    def __del__(self) -> None:
        self.stop()

    def stop(self) -> None:
        self.subscription.stop()


class Projection(ABC, Generic[TTrackingRecorder]):
    name: str = ""

    def __init__(
        self,
        tracking_recorder: TTrackingRecorder,
    ):
        self.tracking_recorder = tracking_recorder

    @singledispatchmethod
    @abstractmethod
    def process_event(
        self, domain_event: DomainEventProtocol, tracking: Tracking
    ) -> None:
        """
        Process a domain event and track it.
        """


TProjection = TypeVar("TProjection", bound=Projection[Any])


TApplication = TypeVar("TApplication", bound=Application)


class ProjectionRunner(Generic[TApplication, TTrackingRecorder]):
    def __init__(
        self,
        *,
        application_class: Type[TApplication],
        projection_class: Type[Projection[TTrackingRecorder]],
        tracking_recorder_class: Type[TTrackingRecorder] | None = None,
        env: EnvType | None = None,
    ):
        self.app: TApplication = application_class(env)

        projection_environment = self._construct_env(
            name=projection_class.name or projection_class.__name__, env=env
        )
        self.projection_factory: InfrastructureFactory[TTrackingRecorder] = (
            InfrastructureFactory.construct(env=projection_environment)
        )
        self.tracking_recorder: TTrackingRecorder = (
            self.projection_factory.tracking_recorder(tracking_recorder_class)
        )

        self.subscription = ApplicationSubscription(
            app=self.app,
            gt=self.tracking_recorder.max_tracking_id(self.app.name),
        )
        self.projection = projection_class(
            tracking_recorder=self.tracking_recorder,
        )
        self._has_error = Event()
        self.thread_error: BaseException | None = None
        self.processing_thread = Thread(
            target=self._process_events_loop,
            kwargs={
                "subscription": self.subscription,
                "projection": self.projection,
                "has_error": self._has_error,
                "runner": weakref.ref(self),
            },
        )
        self.processing_thread.start()

    def _construct_env(self, name: str, env: EnvType | None = None) -> Environment:
        """
        Constructs environment from which projection will be configured.
        """
        _env: Dict[str, str] = {}
        _env.update(os.environ)
        if env is not None:
            _env.update(env)
        return Environment(name, _env)

    def stop(self) -> None:
        self.subscription.stop()

    @staticmethod
    def _process_events_loop(
        subscription: ApplicationSubscription,
        projection: Projection[TrackingRecorder],
        has_error: Event,
        runner: weakref.ReferenceType[ProjectionRunner[Application, TrackingRecorder]],
    ) -> None:
        try:
            with subscription:
                for domain_event, tracking in subscription:
                    projection.process_event(domain_event, tracking)
        except BaseException as e:
            _runner = runner()  # get reference from weakref
            if _runner is not None:
                _runner.thread_error = e
            else:
                msg = "ProjectionRunner was deleted before error could be assigned:\n"
                msg += format_exc()
                warn(
                    msg,
                    RuntimeWarning,
                    stacklevel=2,
                )

            has_error.set()
            subscription.subscription.stop()

    def run_forever(self, timeout: float | None = None) -> None:
        if self._has_error.wait(timeout=timeout):
            assert self.thread_error is not None  # for mypy
            raise self.thread_error

    def wait(self, notification_id: int, timeout: float = 1.0) -> None:
        try:
            self.projection.tracking_recorder.wait(
                application_name=self.subscription.name,
                notification_id=notification_id,
                timeout=timeout,
                interrupt=self._has_error,
            )
        except WaitInterruptedError:
            assert self.thread_error is not None  # for mypy
            raise self.thread_error from None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        self.stop()
        self.processing_thread.join()

    def __del__(self) -> None:
        self.stop()
