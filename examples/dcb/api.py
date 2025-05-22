from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from eventsourcing.persistence import InfrastructureFactory, TTrackingRecorder

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence


@dataclass
class DCBQueryItem:
    types: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)


@dataclass
class DCBQuery:
    items: list[DCBQueryItem] = field(default_factory=list)


@dataclass
class DCBAppendCondition:
    fail_if_events_match: DCBQuery = field(default_factory=DCBQuery)
    after: int | None = None


@dataclass
class DCBEvent:
    type: str
    data: bytes
    tags: list[str] = field(default_factory=list)


@dataclass
class DCBSequencedEvent:
    event: DCBEvent
    position: int


class DCBEventStore(ABC):
    @abstractmethod
    def read(
        self,
        query: DCBQuery | None = None,
        after: int | None = None,
        limit: int | None = None,
    ) -> Iterator[DCBSequencedEvent]:
        """
        Returns all events, unless 'after' is given then only those with position
        greater than 'after', and unless any query items are given, then only those
        that match at least one query item. An event matches a query item if its type
        is in the item types or there are no item types, and if all the item tags are
        in the event tags.
        """

    def get(
        self,
        query: DCBQuery | None = None,
        after: int | None = None,
        limit: int | None = None,
    ) -> Sequence[DCBSequencedEvent]:
        """
        A convenient method to get a sequence of events rather than an iterator.
        """
        return list(self.read(query=query, after=after, limit=limit))

    @abstractmethod
    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        """
        Appends given events to the event store, unless the condition fails.
        """


class DCBInfrastructureFactory(InfrastructureFactory[TTrackingRecorder], ABC):
    @abstractmethod
    def dcb_event_store(self) -> DCBEventStore:
        pass  # pragma: no cover
