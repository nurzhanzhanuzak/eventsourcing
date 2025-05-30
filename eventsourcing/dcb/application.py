from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, ClassVar

from eventsourcing.dcb.domain import (
    CanInitialiseEnduringObject,
    CanMutateEnduringObject,
    EnduringObject,
    Perspective,
    Selector,
)
from eventsourcing.dcb.persistence import (
    DCBEventStore,
    DCBInfrastructureFactory,
    NotFoundError,
    TGroup,
)
from eventsourcing.utils import Environment, EnvType

if TYPE_CHECKING:
    from collections.abc import Sequence

    from typing_extensions import Self


class DCBApplication:
    name = "DCBApplication"
    env: ClassVar[dict[str, str]] = {"PERSISTENCE_MODULE": "eventsourcing.dcb.popo"}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        if "name" not in cls.__dict__:
            cls.name = cls.__name__

    def __init__(self, env: EnvType | None = None):
        self.env = self.construct_env(self.name, env)  # type: ignore[misc]
        self.factory = DCBInfrastructureFactory.construct(self.env)
        self.recorder = self.factory.dcb_event_store()

    def construct_env(self, name: str, env: EnvType | None = None) -> Environment:
        """Constructs environment from which application will be configured."""
        _env = dict(type(self).env)
        _env.update(os.environ)
        if env is not None:
            _env.update(env)
        return Environment(name, _env)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        self.factory.close()


class DCBRepository:
    def __init__(self, eventstore: DCBEventStore):
        self.eventstore = eventstore

    def save(self, obj: Perspective) -> int:
        new_events = obj.collect_events()
        return self.eventstore.put(
            *new_events, cb=obj.cb, after=obj.last_known_position
        )

    def get(
        self,
        enduring_object_id: str,
        cb_types: Sequence[type[CanMutateEnduringObject]] = (),
    ) -> EnduringObject:
        cb = [Selector(tags=[enduring_object_id], types=cb_types)]
        events, head = self.eventstore.get(*cb, with_last_position=True)
        obj: EnduringObject | None = None
        for event in events:
            obj = event.mutate(obj)
        if obj is None:
            raise NotFoundError
        obj.last_known_position = head
        obj.cb_types = cb_types
        return obj

    def get_many(
        self,
        *enduring_object_ids: str,
        cb_types: Sequence[type[CanMutateEnduringObject]] = (),
    ) -> list[EnduringObject | None]:
        cb = [
            Selector(tags=[enduring_object_id], types=cb_types)
            for enduring_object_id in enduring_object_ids
        ]
        events, head = self.eventstore.get(cb, with_last_position=True)
        objs: dict[str, EnduringObject | None] = dict.fromkeys(enduring_object_ids)
        for event in events:
            for tag in event.tags:
                obj = objs.get(tag)
                if not isinstance(event, CanInitialiseEnduringObject) and not obj:
                    continue
                obj = event.mutate(obj)
                objs[tag] = obj
        for obj in objs.values():
            if obj is not None:
                obj.last_known_position = head
                obj.cb_types = cb_types
        return list(objs.values())

    def get_group(self, cls: type[TGroup], *enduring_object_ids: str) -> TGroup:
        enduring_objects = self.get_many(*enduring_object_ids, cb_types=cls.cb_types)
        perspective = cls(*enduring_objects)
        last_known_positions = [
            o.last_known_position
            for o in enduring_objects
            if o and o.last_known_position
        ]
        perspective.last_known_position = (
            max(last_known_positions) if last_known_positions else None
        )
        return perspective
