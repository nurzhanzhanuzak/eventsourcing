from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from eventsourcing.domain import (
    BaseAggregate,
    CanInitAggregate,
    CanMutateAggregate,
    CanSnapshotAggregate,
    MetaDomainEvent,
    event,
)

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(frozen=True)
class DomainEvent(metaclass=MetaDomainEvent):
    originator_id: str
    originator_version: int
    timestamp: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.originator_id, str):
            msg = (
                f"{type(self)} "
                f"was initialized with a non-str originator_id: "
                f"{self.originator_id!r}"
            )
            raise TypeError(msg)


@dataclass(frozen=True)
class Snapshot(DomainEvent, CanSnapshotAggregate[str]):
    topic: str
    state: dict[str, Any]


class Aggregate(BaseAggregate[str]):
    @dataclass(frozen=True)
    class Event(DomainEvent, CanMutateAggregate[str]):
        pass

    @dataclass(frozen=True)
    class Created(Event, CanInitAggregate[str]):
        originator_topic: str


class Dog(Aggregate):
    INITIAL_VERSION = 0

    @staticmethod
    def create_id() -> str:
        return "dog-" + str(uuid.uuid4())

    @event("Registered")
    def __init__(self, name: str) -> None:
        self.name = name
        self.tricks: list[str] = []

    @event("TrickAdded")
    def add_trick(self, trick: str) -> None:
        self.tricks.append(trick)
