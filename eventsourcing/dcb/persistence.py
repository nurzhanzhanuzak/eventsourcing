from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal, TypeVar, overload

from eventsourcing.dcb.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBQuery,
    DCBQueryItem,
    DCBRecorder,
)
from eventsourcing.dcb.domain import (
    CanMutateEnduringObject,
    Group,
    Selector,
)
from eventsourcing.persistence import InfrastructureFactory, TTrackingRecorder
from eventsourcing.utils import get_topic

if TYPE_CHECKING:
    from collections.abc import Sequence

TGroup = TypeVar("TGroup", bound=Group)


class DCBMapper(ABC):
    @abstractmethod
    def to_dcb_event(self, event: CanMutateEnduringObject) -> DCBEvent:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def to_domain_event(self, event: DCBEvent) -> CanMutateEnduringObject:
        raise NotImplementedError  # pragma: no cover


class DCBEventStore:
    def __init__(self, mapper: DCBMapper, recorder: DCBRecorder):
        self.mapper = mapper
        self.recorder = recorder

    def put(
        self,
        *events: CanMutateEnduringObject,
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
    ) -> Sequence[CanMutateEnduringObject]:
        pass  # pragma: no cover

    @overload
    def get(
        self,
        cb: Selector | Sequence[Selector] | None = None,
        *,
        with_last_position: Literal[True],
        after: int | None = None,
    ) -> tuple[Sequence[CanMutateEnduringObject], int | None]:
        pass  # pragma: no cover

    @overload
    def get(
        self,
        cb: Selector | Sequence[Selector] | None = None,
        *,
        with_positions: Literal[True],
        after: int | None = None,
    ) -> Sequence[tuple[CanMutateEnduringObject, int]]:
        pass  # pragma: no cover

    def get(
        self,
        cb: Selector | Sequence[Selector] | None = None,
        *,
        after: int | None = None,
        with_positions: bool = False,
        with_last_position: bool = False,
    ) -> (
        Sequence[tuple[CanMutateEnduringObject, int]]
        | tuple[Sequence[CanMutateEnduringObject], int | None]
        | Sequence[CanMutateEnduringObject]
    ):
        query = self._cb_to_dcb_query(cb)
        dcb_sequenced_events, head = self.recorder.read(
            query=query,
            after=after,
        )
        if not (with_positions or with_last_position):
            return tuple(
                self.mapper.to_domain_event(s.event) for s in dcb_sequenced_events
            )
        if with_last_position:
            return (
                tuple(
                    [self.mapper.to_domain_event(s.event) for s in dcb_sequenced_events]
                ),
                head,
            )
        return tuple(
            (self.mapper.to_domain_event(s.event), s.position)
            for s in dcb_sequenced_events
        )

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


class NotFoundError(Exception):
    pass


class DCBInfrastructureFactory(InfrastructureFactory[TTrackingRecorder], ABC):
    @abstractmethod
    def dcb_event_store(self) -> DCBRecorder:
        pass  # pragma: no cover
