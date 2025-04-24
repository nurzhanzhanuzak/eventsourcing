from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from eventsourcing.domain import datetime_now_with_tzinfo
from eventsourcing.utils import get_topic

if TYPE_CHECKING:
    from collections.abc import Iterable


class DomainEvent(BaseModel, frozen=True):
    model_config = ConfigDict(extra="forbid")

    originator_id: UUID
    originator_version: int
    timestamp: datetime


class Aggregate(BaseModel, frozen=True):
    model_config = ConfigDict(extra="forbid")
    id: UUID

    version: int
    created_on: datetime
    modified_on: datetime


class Snapshot(DomainEvent, frozen=True):
    topic: str
    state: dict[str, Any]

    @classmethod
    def take(cls, aggregate: Aggregate) -> Snapshot:
        return Snapshot(
            originator_id=aggregate.id,
            originator_version=aggregate.version,
            timestamp=datetime_now_with_tzinfo(),
            topic=get_topic(type(aggregate)),
            state=aggregate.model_dump(),
        )


TAggregate = TypeVar("TAggregate", bound=Aggregate)

MutatorFunction = Callable[..., Optional[TAggregate]]


def aggregate_projector(
    mutator: MutatorFunction[TAggregate],
) -> Callable[[TAggregate | None, Iterable[DomainEvent]], TAggregate | None]:
    def project_aggregate(
        aggregate: TAggregate | None, events: Iterable[DomainEvent]
    ) -> TAggregate | None:
        for event in events:
            aggregate = mutator(event, aggregate)
        return aggregate

    return project_aggregate
