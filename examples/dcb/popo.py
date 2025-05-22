from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from eventsourcing.persistence import IntegrityError, ProgrammingError
from eventsourcing.popo import POPOFactory, POPORecorder, POPOTrackingRecorder
from examples.dcb.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBEventStore,
    DCBInfrastructureFactory,
    DCBQuery,
    DCBSequencedEvent,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence


class InMemoryDCBEventStore(DCBEventStore, POPORecorder):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[DCBSequencedEvent] = []
        self.position_sequence = self._position_sequence_generator()

    def read(
        self,
        query: DCBQuery | None = None,
        after: int | None = None,
        limit: int | None = None,
    ) -> Iterator[DCBSequencedEvent]:
        query = query or DCBQuery()
        with self._database_lock:
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
                yield deepcopy(event)

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        if len(events) == 0:
            msg = "Should be at least one event. Avoid this elsewhere"
            raise ProgrammingError(msg)
        with self._database_lock:
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
                    event=deepcopy(event),
                )
                for event in events
            )
            return self.events[-1].position

    def _position_sequence_generator(self) -> Iterator[int]:
        position = 1
        while True:
            yield position
            position += 1


class DCBPOPOFactory(POPOFactory, DCBInfrastructureFactory[POPOTrackingRecorder]):

    def dcb_event_store(self) -> DCBEventStore:
        return InMemoryDCBEventStore()
