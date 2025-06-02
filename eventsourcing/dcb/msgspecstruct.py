from __future__ import annotations

from typing import Any

import msgspec

from eventsourcing.dcb.api import DCBEvent
from eventsourcing.dcb.domain import Initialises, Mutates
from eventsourcing.dcb.persistence import DCBMapper
from eventsourcing.utils import get_topic, resolve_topic


class Decision(msgspec.Struct, Mutates):
    tags: list[str]

    def _as_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self.__struct_fields__}


class MsgspecStructMapper(DCBMapper):
    def to_dcb_event(self, event: Mutates) -> DCBEvent:
        return DCBEvent(
            type=get_topic(type(event)),
            data=msgspec.msgpack.encode(event),
            tags=event.tags,
        )

    def to_domain_event(self, event: DCBEvent) -> Mutates:
        return msgspec.msgpack.decode(
            event.data,
            type=resolve_topic(event.type),
        )


class InitialDecision(Decision, Initialises):
    originator_topic: str
