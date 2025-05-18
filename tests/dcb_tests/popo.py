from __future__ import annotations

from threading import RLock
from typing import TYPE_CHECKING

from eventsourcing.persistence import IntegrityError
from tests.dcb_tests.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBEventStore,
    DCBQuery,
    DCBSequencedEvent,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence


class InMemoryDCBEventStore(DCBEventStore):
    def __init__(self) -> None:
        self.events: list[DCBSequencedEvent] = []
        self.position_sequence = self._position_sequence_generator()
        self._lock = RLock()

    def read(
        self,
        query: DCBQuery | None = None,
        after: int | None = None,
        limit: int | None = None,
    ) -> Iterator[DCBSequencedEvent]:
        query = query or DCBQuery()
        with self._lock:
            events = (
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
            for i, event in enumerate(events):
                if limit is not None and i >= limit:
                    return
                yield event

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        self._assert_len_events(events)
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

    def _position_sequence_generator(self) -> Iterator[int]:
        position = 1
        while True:
            yield position
            position += 1
