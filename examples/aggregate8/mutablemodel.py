from __future__ import annotations

import typing
from datetime import datetime
from typing import Any

from pydantic import ConfigDict, TypeAdapter

from eventsourcing.domain import (
    BaseAggregate,
    CanInitAggregate,
    CanMutateAggregate,
    CanSnapshotAggregate,
)
from examples.aggregate7.immutablemodel import DomainEvent, Immutable

datetime_adapter = TypeAdapter(datetime)


class SnapshotState(Immutable):
    model_config = ConfigDict(extra="allow")

    def __init__(self, **kwargs: Any) -> None:
        for key in ["_created_on", "_modified_on"]:
            kwargs[key] = datetime_adapter.validate_python(kwargs[key])
        super().__init__(**kwargs)


class AggregateSnapshot(DomainEvent, CanSnapshotAggregate):
    topic: str
    state: Any

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        type_of_snapshot_state = typing.get_type_hints(cls)["state"]
        try:
            assert issubclass(
                type_of_snapshot_state, SnapshotState
            ), type_of_snapshot_state
        except (TypeError, AssertionError) as e:
            msg = (
                f"Subclass of {SnapshotState}"
                f" is required as the annotated type of 'state' on "
                f"{cls}, got: {type_of_snapshot_state}"
            )
            raise TypeError(msg) from e


class Aggregate(BaseAggregate):
    class Event(DomainEvent, CanMutateAggregate):
        pass

    class Created(Event, CanInitAggregate):
        originator_topic: str
