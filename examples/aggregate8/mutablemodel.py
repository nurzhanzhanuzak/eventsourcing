from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, TypeAdapter

from eventsourcing.domain import (
    BaseAggregate,
    CanInitAggregate,
    CanMutateAggregate,
    CanSnapshotAggregate,
)
from examples.aggregate7.immutablemodel import DomainEvent

datetime_adapter = TypeAdapter(datetime)


class SnapshotState(BaseModel, frozen=True):
    model_config = ConfigDict(extra="allow")

    def __init__(self, **kwargs: Any) -> None:
        for key in ["_created_on", "_modified_on"]:
            kwargs[key] = datetime_adapter.validate_python(kwargs[key])
        super().__init__(**kwargs)


class AggregateSnapshot(DomainEvent, CanSnapshotAggregate, frozen=True):
    topic: str
    state: SnapshotState


class Aggregate(BaseAggregate):
    class Event(DomainEvent, CanMutateAggregate, frozen=True):
        pass

    class Created(Event, CanInitAggregate, frozen=True):
        originator_topic: str
