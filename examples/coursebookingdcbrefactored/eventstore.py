from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import msgspec
from msgspec import Struct

from eventsourcing.utils import get_topic, resolve_topic
from examples.dcb.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBEventStore,
    DCBQuery,
    DCBQueryItem,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


class DomainEvent(Struct):
    tags: list[str]


class Mapper:
    def to_dcb_event(self, event: DomainEvent) -> DCBEvent:
        return DCBEvent(
            type=get_topic(type(event)),
            data=msgspec.msgpack.encode(event),
            tags=event.tags,
        )

    def to_domain_event(self, event: DCBEvent) -> DomainEvent:
        return msgspec.msgpack.decode(
            event.data,
            type=resolve_topic(event.type),
        )


class Selector:
    def __init__(
        self, types: Sequence[type[DomainEvent]] = (), tags: Sequence[str] = ()
    ):
        self.types = types
        self.tags = tags


class EventStore:
    def __init__(self, mapper: Mapper, recorder: DCBEventStore):
        self.mapper = mapper
        self.recorder = recorder

    def put(
        self,
        *events: DomainEvent,
        cb: Selector | Sequence[Selector] | None = None,
        after: int | None = None,
    ) -> int:
        if not cb and not after:
            condition = None
        else:
            query = self._cb_to_dcb_query(cb)
            condition = DCBAppendCondition(
                fail_if_events_match=query,
                after=after,
            )
        return self.recorder.append(
            events=[self.mapper.to_dcb_event(e) for e in events],
            condition=condition,
        )

    @overload
    def get(
        self,
        cb: Selector | Sequence[Selector] | None = None,
        *,
        after: int | None = None,
    ) -> Sequence[DomainEvent]:
        pass  # pragma: no cover

    @overload
    def get(
        self,
        cb: Selector | Sequence[Selector] | None = None,
        *,
        with_last_position: Literal[True],
        after: int | None = None,
    ) -> tuple[Sequence[DomainEvent], int]:
        pass  # pragma: no cover

    @overload
    def get(
        self,
        cb: Selector | Sequence[Selector] | None = None,
        *,
        with_positions: Literal[True],
        after: int | None = None,
    ) -> Sequence[tuple[DomainEvent, int]]:
        pass  # pragma: no cover

    def get(
        self,
        cb: Selector | Sequence[Selector] | None = None,
        *,
        after: int | None = None,
        with_positions: bool = False,
        with_last_position: bool = False,
    ) -> (
        Sequence[tuple[DomainEvent, int]]
        | tuple[Sequence[DomainEvent], int]
        | Sequence[DomainEvent]
    ):
        query = self._cb_to_dcb_query(cb)
        dcb_sequenced_events = self.recorder.read(
            query=query,
            after=after,
        )
        if not (with_positions or with_last_position):
            return tuple(
                self.mapper.to_domain_event(s.event) for s in dcb_sequenced_events
            )
        events_and_positions = tuple(
            (self.mapper.to_domain_event(s.event), s.position)
            for s in dcb_sequenced_events
        )
        if with_last_position:
            domain_events = [ep[0] for ep in events_and_positions]
            last_position = max([ep[1] for ep in events_and_positions])
            return domain_events, last_position
        return events_and_positions

    @staticmethod
    def _cb_to_dcb_query(
        cb: Selector | Sequence[Selector] | None = None,
    ) -> DCBQuery:
        cb = [cb] if isinstance(cb, Selector) else cb or []
        return DCBQuery(
            items=[
                DCBQueryItem(
                    types=[get_topic(t) for t in s.types],
                    tags=list(s.tags),
                )
                for s in cb
            ]
        )
