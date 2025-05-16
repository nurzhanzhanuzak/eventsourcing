from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import TYPE_CHECKING

from eventsourcing.persistence import IntegrityError

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence


@dataclass
class DCBQueryItem:
    types: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)


@dataclass
class DCBQuery:
    items: Sequence[DCBQueryItem] = field(default_factory=list)


@dataclass
class DCBAppendCondition:
    fail_if_events_match: DCBQuery
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


class DCBEventStore:
    def __init__(self) -> None:
        self.events: list[DCBSequencedEvent] = []
        self.position_sequence = self._position_sequence_generator()
        self._lock = RLock()

    def _position_sequence_generator(self) -> Iterator[int]:
        position = 1
        while True:
            yield position
            position += 1

    def get(
        self, query: DCBQuery, after: int | None = None
    ) -> Sequence[DCBSequencedEvent]:
        return list(self.read(query=query, after=after))

    def read(
        self, query: DCBQuery, after: int | None = None
    ) -> Iterator[DCBSequencedEvent]:
        # Returns all events, unless 'after' is given then only those with position
        # greater than 'after', and unless any query items are given, then only those
        # that match at least one query item. An event matches a query item if its type
        # is in the item types or there are no item types, and if all the item tags are
        # in the event tags.
        with self._lock:
            return (
                event
                for event in self.events
                if (after is None or event.position > after)
                and (
                    not query.items
                    or any(
                        (not item.types or event.event.type in item.types)
                        and (set(event.event.tags) >= set(item.tags))
                        for item in query.items
                    )
                )
            )

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        with self._lock:
            if condition is not None:
                try:
                    next(
                        self.read(
                            query=condition.fail_if_events_match,
                            after=condition.after,
                        )
                    )
                    raise IntegrityError
                except StopIteration:
                    pass
            self.events.extend(
                DCBSequencedEvent(
                    position=next(self.position_sequence),
                    event=event,
                )
                for event in events
            )
            return self.events[-1].position
