from __future__ import annotations

from datetime import datetime
from typing import Any, List
from uuid import UUID

from pydantic import BaseModel, ConfigDict, TypeAdapter

from eventsourcing.domain import (
    Aggregate as BaseAggregate,
    CanInitAggregate,
    CanMutateAggregate,
    CanSnapshotAggregate,
    event,
)


class DomainEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    originator_id: UUID
    originator_version: int
    timestamp: datetime


datetime_adapter = TypeAdapter(datetime)


class SnapshotState(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")

    def __init__(self, **kwargs: Any) -> None:
        for key in ["_created_on", "_modified_on"]:
            kwargs[key] = datetime_adapter.validate_python(kwargs[key])
        super().__init__(**kwargs)


class AggregateSnapshot(DomainEvent, CanSnapshotAggregate):
    topic: str
    state: SnapshotState


class Aggregate(BaseAggregate):
    class Event(DomainEvent, CanMutateAggregate):
        pass

    class Created(Event, CanInitAggregate):
        originator_topic: str


class Trick(BaseModel):
    name: str


class DogSnapshotState(SnapshotState):
    name: str
    tricks: List[Trick]


class Dog(Aggregate):
    class Snapshot(AggregateSnapshot):
        state: DogSnapshotState

    @event("Registered")
    def __init__(self, name: str) -> None:
        self.name = name
        self.tricks: List[Trick] = []

    @event("TrickAdded")
    def add_trick(self, trick: Trick) -> None:
        self.tricks.append(trick)
