from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast, overload, Callable
from uuid import uuid4

import msgspec
from msgspec import Struct
from typing_extensions import Self

from eventsourcing.domain import ProgrammingError, filter_kwargs_for_method_params, \
    decorated_funcs, CommandMethodDecorator, decorator_event_classes, AbstractDCBEvent
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

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self.__dict__ == other.__dict__


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
        # started = datetime_now_with_tzinfo()
        return self.recorder.append(
            events=[self.mapper.to_dcb_event(e) for e in events],
            condition=condition,
        )
        # duration = int(
        #     (datetime_now_with_tzinfo() - started).total_seconds() * 1000000
        # )
        # print("Appendeded", events, "at position", position, f"in {duration} us")
        # return position

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
    ) -> tuple[Sequence[DomainEvent], int | None]:
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
        | tuple[Sequence[DomainEvent], int | None]
        | Sequence[DomainEvent]
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


class Repository:
    def __init__(self, eventstore: EventStore):
        self.eventstore = eventstore

    def save(self, obj: EnduringObject) -> int:
        new_events = obj.collect_events()
        return self.eventstore.put(
            *new_events, cb=obj.cb, after=obj.last_known_position
        )

    def get(self, enduring_object_id: str) -> EnduringObject:
        cb = [Selector(tags=[enduring_object_id])]
        events, head = self.eventstore.get(*cb, with_last_position=True)
        obj: EnduringObject | None = None
        for event in events:
            obj = event.mutate(obj)
        if obj is None:
            raise NotFoundError
        obj.last_known_position = head
        return obj

    def get_many(self, *enduring_object_ids: str) -> list[EnduringObject | None]:
        cb = [
            Selector(tags=[enduring_object_id])
            for enduring_object_id in enduring_object_ids
        ]
        events, head = self.eventstore.get(cb, with_last_position=True)
        objs: dict[str, EnduringObject | None] = dict.fromkeys(enduring_object_ids)
        for event in events:
            for tag in event.tags:
                obj = objs.get(tag, None)
                if not isinstance(event, InitEvent) and not obj:
                    continue
                obj = event.mutate(obj)
                objs[tag] = obj
        for obj in objs.values():
            if obj is not None:
                obj.last_known_position = head
        return list(objs.values())


class NotFoundError(Exception):
    pass


_enduring_object_init_classes: dict[type[Any], type[InitEvent]] = {}


class CanMutateEnduringObject(AbstractDCBEvent):
    tags: list[str]

    def _as_dict(self) -> dict[str, Any]:
        raise NotImplementedError

    def mutate(self, obj: EnduringObject | None) -> EnduringObject | None:
        assert obj is not None
        self.apply(obj)
        return obj

    def apply(self, obj: Any) -> None:
        pass


class CanInitEnduringObject(CanMutateEnduringObject):
    originator_topic: str

    def mutate(self, obj: EnduringObject | None) -> EnduringObject | None:
        kwargs = self._as_dict()
        originator_topic = kwargs.pop("originator_topic")
        enduring_object_cls = cast(
            type[EnduringObject], resolve_topic(originator_topic)
        )
        enduring_object = enduring_object_cls.__new__(enduring_object_cls)
        kwargs.pop("tags")
        common_kwargs = {
            "id": kwargs.pop(enduring_object_cls.id_attr_name),
        }
        enduring_object.__base_init__(**common_kwargs)
        enduring_object.__post_init__()
        try:
            enduring_object.__init__(**kwargs)  # type: ignore[misc]
        except TypeError as e:
            msg = (f"{self.__qualname__} can't __init__ "
                   f"{enduring_object_cls.__qualname__} "
                   f"with kwargs {kwargs}: {e}")
            raise TypeError(msg) from e
        return enduring_object


T = TypeVar("T")

class MetaPerspective(type):
    def __init__(self, name: str, bases: tuple[type, ...], namespace: dict[str, Any]) -> None:
        super().__init__(name, bases, namespace)
        for attr, value in namespace.items():
            if isinstance(value, CommandMethodDecorator):
                # Just keep things simple.
                # TODO: Maybe support event name strings, maybe not....
                assert value.given_event_cls is not None, "Event class not given"
                # TODO: Actually maybe enforce that given event class is nested on self.
                event_cls_qualname = f"{self.__qualname__}.{value.given_event_cls.__name__}"
                event_cls_dict = {
                    # "__annotations__": annotations,
                    "__module__": self.__module__,
                    "__qualname__": event_cls_qualname,
                }

                event_subclass = type(name, (DecoratorEvent, value.given_event_cls), event_cls_dict)
                namespace[attr] = event_subclass
                decorator_event_classes[value] = event_subclass
                decorated_funcs[event_subclass] = value.decorated_func


class Perspective(metaclass=MetaPerspective):
    pass


class MetaEnduringObject(MetaPerspective):
    def __init__(cls, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        for item in cls.__dict__.values():
            if isinstance(item, type) and issubclass(item, InitEvent):
                _enduring_object_init_classes[cls] = item
                break

    def __call__(cls: type[T], **kwargs: Any) -> T:
        # TODO: For convenience, make this error out in the same way
        #  as it would if the arguments didn't match the __init__
        #  method and __init__was called directly, and verify the
        #  event's __init__ is valid when initialising the class.

        assert issubclass(cls, EnduringObject)
        try:
            init_enduring_object_class = _enduring_object_init_classes[cls]
        except KeyError:
            msg = (
                f"Enduring object class {cls.__name__} has no CanInitEnduringObject "
                f"class. Please define a subclass of CanInitEnduringObject"
                f"as a nested class on {cls.__name__}."
            )
            raise ProgrammingError(msg) from None

        return cast(
            T,
            cls._create(
                event_class=init_enduring_object_class,
                **kwargs,
            ),
        )

    @property
    def id_attr_name(cls) -> str:
        return f"{cls.__name__.lower()}_id"


class DomainEvent(Struct, CanMutateEnduringObject):
    tags: list[str]

    def _as_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self.__struct_fields__}


class InitEvent(DomainEvent, CanInitEnduringObject):
    originator_topic: str


class EnduringObject(Perspective, metaclass=MetaEnduringObject):
    @classmethod
    def _create(cls: type[Self], event_class: type[InitEvent], **kwargs: Any) -> Self:
        enduring_object_id = cls._create_id()
        init_event_kwargs: dict[str, Any] = {cls.id_attr_name: enduring_object_id}
        init_event_kwargs.update(kwargs)
        init_event_kwargs["originator_topic"] = get_topic(cls)
        init_event_kwargs["tags"] = [enduring_object_id]
        try:
            init_event = event_class(**init_event_kwargs)
        except TypeError as e:
            msg = (
                f"Unable to construct {event_class.__qualname__} event "
                f"with kwargs {init_event_kwargs}: {e}"
            )
            raise TypeError(msg) from e
        enduring_object = cast(Self, init_event.mutate(None))
        assert enduring_object is not None
        enduring_object.pending_events.append(init_event)
        return enduring_object

    @classmethod
    def _create_id(cls) -> str:
        return f"{cls.__name__.lower()}-{uuid4()}"

    def __base_init__(self, id: str) -> None:  # noqa: A002
        self.id = id
        self.pending_events: list[DomainEvent] = []
        self.last_known_position: int | None = None

    def __post_init__(self) -> None:
        pass

    def collect_events(self) -> list[DomainEvent]:
        collected, self.pending_events = self.pending_events, []
        return collected

    @property
    def cb(self) -> list[Selector]:
        return [Selector(tags=[self.id])]

    def trigger_event(
        self, event_class: type[DomainEvent], *, tags: Sequence[str] = (), **kwargs: Any
    ) -> None:
        tags = [self.id, *tags]
        event = event_class(tags=tags, **kwargs)
        event.mutate(self)
        self.pending_events.append(event)


class DecoratorEvent(DomainEvent):
    def apply(self, obj: Perspective) -> None:
        """Applies event to perspective by calling method decorated by @event."""
        # Identify the function that was decorated.
        decorated_func = decorated_funcs[type(self)]

        # Select event attributes mentioned in function signature.
        self_dict = self._as_dict()
        kwargs = filter_kwargs_for_method_params(self_dict, decorated_func)

        # Call the original method with event attribute values.
        decorated_method = decorated_func.__get__(obj, type(obj))
        decorated_method(**kwargs)

        # Call super method, just in case any base classes need it.
        super().apply(obj)


