from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterable, Optional, TypeVar

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID


@dataclass(frozen=True)
class DomainEvent:
    originator_id: UUID
    originator_version: int
    timestamp: datetime


@dataclass(frozen=True)
class Aggregate:
    id: UUID
    version: int
    created_on: datetime
    modified_on: datetime


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
