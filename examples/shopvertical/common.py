from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, cast

from eventsourcing.application import Application
from eventsourcing.utils import get_topic
from examples.aggregate7.orjsonpydantic import OrjsonTranscoder, PydanticMapper
from examples.shopvertical.events import DomainEvent

if TYPE_CHECKING:
    from uuid import UUID


class Command(ABC):
    @abstractmethod
    def handle(self, events: tuple[DomainEvent, ...]) -> tuple[DomainEvent, ...]:
        pass  # pragma: no cover

    @abstractmethod
    def execute(self) -> int | None:
        pass  # pragma: no cover


class Query(ABC):
    @abstractmethod
    def execute(self) -> Any:
        pass  # pragma: no cover


def construct_application() -> Application:
    return Application(
        env={
            "TRANSCODER_TOPIC": get_topic(OrjsonTranscoder),
            "MAPPER_TOPIC": get_topic(PydanticMapper),
        }
    )


class _Globals:
    app = construct_application()


def reset_application() -> None:
    _Globals.app = construct_application()


def get_events(originator_id: UUID) -> tuple[DomainEvent, ...]:
    return cast(tuple[DomainEvent, ...], tuple(_Globals.app.events.get(originator_id)))


def put_events(events: tuple[DomainEvent, ...]) -> int | None:
    recordings = _Globals.app.events.put(events)
    return recordings[-1].notification.id if recordings else None


def get_all_events() -> tuple[DomainEvent, ...]:
    return cast(
        tuple[DomainEvent, ...],
        tuple(
            map(
                _Globals.app.mapper.to_domain_event,
                _Globals.app.recorder.select_notifications(start=0, limit=1000000),
            )
        ),
    )
