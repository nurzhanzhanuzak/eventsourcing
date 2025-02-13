from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable, List, Type, TypeVar, cast
from uuid import UUID, uuid4

from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.domain import Snapshot, datetime_now_with_tzinfo

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(frozen=True)
class DomainEvent:
    originator_version: int
    originator_id: UUID
    timestamp: datetime


TAggregate = TypeVar("TAggregate", bound="Aggregate")


class Aggregate:
    id: UUID
    version: int
    created_on: datetime
    _pending_events: List[DomainEvent]

    def __init__(self, event: DomainEvent):
        self.id = event.originator_id
        self.version = event.originator_version
        self.created_on = event.timestamp

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

    @singledispatchmethod
    def apply(self, event: DomainEvent) -> None:
        """Applies event to aggregate."""

    def collect_events(self) -> List[DomainEvent]:
        events, self._pending_events = self._pending_events, []
        return events

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


class Dog(Aggregate):
    @dataclass(frozen=True)
    class Registered(DomainEvent):
        name: str

    @dataclass(frozen=True)
    class TrickAdded(DomainEvent):
        trick: str

    @classmethod
    def register(cls, name: str) -> Dog:
        event = cls.Registered(
            originator_id=uuid4(),
            originator_version=1,
            timestamp=datetime_now_with_tzinfo(),
            name=name,
        )
        dog = cast(Dog, cls.projector(None, [event]))
        dog._pending_events.append(event)
        return dog

    def add_trick(self, trick: str) -> None:
        self.trigger_event(self.TrickAdded, trick=trick)

    @singledispatchmethod
    def apply(self, event: DomainEvent) -> None:
        """Applies event to aggregate."""

    @apply.register(Registered)
    def _(self, event: Registered) -> None:
        super().__init__(event)
        self.name = event.name
        self.tricks: List[str] = []

    @apply.register(TrickAdded)
    def _(self, event: TrickAdded) -> None:
        self.tricks.append(event.trick)
        self.version = event.originator_version

    @apply.register(Snapshot)
    def _(self, event: Snapshot) -> None:
        self.__dict__.update(event.state)
