from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Type, TypeVar

from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.domain import datetime_now_with_tzinfo
from eventsourcing.utils import get_topic

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

TAggregate = TypeVar("TAggregate", bound="Aggregate")


@dataclass(frozen=True)
class DomainEvent:
    originator_version: int
    originator_id: UUID
    timestamp: datetime


@dataclass
class Aggregate:
    id: UUID
    version: int
    created_on: datetime
    modified_on: datetime
    _pending_events: List[DomainEvent]

    @dataclass(frozen=True)
    class Snapshot(DomainEvent):
        topic: str
        state: Dict[str, Any]

        @classmethod
        def take(
            cls,
            aggregate: Aggregate,
        ) -> Aggregate.Snapshot:
            aggregate_state = dict(aggregate.__dict__)
            aggregate_state.pop("_pending_events")
            return Aggregate.Snapshot(
                originator_id=aggregate.id,
                originator_version=aggregate.version,
                timestamp=datetime_now_with_tzinfo(),
                topic=get_topic(type(aggregate)),
                state=aggregate_state,
            )

    def trigger_event(
        self,
        event_class: Type[DomainEvent],
        **kwargs: Any,
    ) -> None:
        kwargs = kwargs.copy()
        kwargs.update(
            originator_id=self.id,
            originator_version=self.version + 1,
            timestamp=datetime_now_with_tzinfo(),
        )
        new_event = event_class(**kwargs)
        self.apply(new_event)
        self._pending_events.append(new_event)

    def collect_events(self) -> List[DomainEvent]:
        events, self._pending_events = self._pending_events, []
        return events

    @singledispatchmethod
    def apply(self, event: DomainEvent) -> None:
        msg = f"For {type(event).__qualname__}"
        raise NotImplementedError(msg)

    @apply.register(Snapshot)
    def _(self, event: Snapshot) -> None:
        self.__dict__.update(event.state)

    @classmethod
    def projector(
        cls: Type[TAggregate],
        _: TAggregate | None,
        events: Iterable[DomainEvent],
    ) -> TAggregate | None:
        aggregate: TAggregate = object.__new__(cls)
        aggregate._pending_events = []
        for event in events:
            aggregate.apply(event)
        return aggregate
