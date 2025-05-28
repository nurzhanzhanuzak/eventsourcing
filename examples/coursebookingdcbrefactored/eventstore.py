from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast, overload
from uuid import uuid4

import msgspec
from typing_extensions import Self

from eventsourcing.dcb.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBEventStore,
    DCBQuery,
    DCBQueryItem,
)
from eventsourcing.domain import (
    AbstractDCBEvent,
    CallableType,
    CommandMethodDecorator,
    ProgrammingError,
    decorated_funcs,
    decorator_event_classes,
    filter_kwargs_for_method_params,
    underscore_method_decorators,
)
from eventsourcing.utils import construct_topic, get_topic, resolve_topic

if TYPE_CHECKING:
    from collections.abc import Sequence


_enduring_object_init_classes: dict[type[Any], type[CanInitialiseEnduringObject]] = {}


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


class CanInitialiseEnduringObject(CanMutateEnduringObject):
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
            msg = (
                f"{type(self).__qualname__} can't __init__ "
                f"{enduring_object_cls.__qualname__} "
                f"with kwargs {kwargs}: {e}"
            )
            raise TypeError(msg) from e
        return enduring_object


class DecoratorEvent(CanMutateEnduringObject):
    def apply(self, obj: Perspective) -> None:
        """Applies event to perspective by calling method decorated by @event."""

        event_class_topic = construct_topic(type(self))

        try:
            decorated_func_collection = cross_cutting_decorated_funcs[event_class_topic]
            assert type(obj) in decorated_func_collection
            decorated_func = decorated_func_collection[type(obj)]

        except KeyError:
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


T = TypeVar("T")


class MetaPerspective(type):
    def __call__(cls: type[T], *args: Any, **kwargs: Any) -> T:
        perspective = cls.__new__(cls)
        perspective.__base_init__(*args, **kwargs)  # type: ignore[attr-defined]
        perspective.__init__(*args, **kwargs)  # type: ignore[misc]
        return perspective


class Perspective(metaclass=MetaPerspective):
    def __base_init__(self, *args: Any, **kwargs: Any) -> None:
        self.new_decisions: list[CanMutateEnduringObject] = []
        self.last_known_position: int | None = None

    def collect_events(self) -> list[CanMutateEnduringObject]:
        collected, self.new_decisions = self.new_decisions, []
        return collected

    @property
    def cb(self) -> list[Selector]:
        raise NotImplementedError


cross_cutting_event_classes: dict[str, type[CanMutateEnduringObject]] = {}
cross_cutting_decorated_funcs: dict[str, dict[type, CallableType]] = {}


class MetaEnduringObject(MetaPerspective):
    def __init__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, Any]
    ) -> None:
        super().__init__(name, bases, namespace)
        # Find and remember the "initialised" class.
        for item in cls.__dict__.values():
            if isinstance(item, type) and issubclass(item, CanInitialiseEnduringObject):
                _enduring_object_init_classes[cls] = item
                break

        # Process the event decorators.
        for attr, value in namespace.items():
            if isinstance(value, CommandMethodDecorator):
                if attr == "_":
                    # Deal with cross cutting events later.
                    continue

                event_class = value.given_event_cls
                # Just keep things simple by only supporting given classes (not names).
                assert event_class is not None, "Event class not given"
                assert issubclass(event_class, CanMutateEnduringObject)
                # TODO: Maybe support event name strings, maybe not....
                event_class_qual = event_class.__qualname__

                assert event_class_qual.startswith(cls.__qualname__ + ".")

                # Subclass given class to make a "decorator class".
                event_subclass_dict = {
                    # "__annotations__": annotations,
                    "__module__": cls.__module__,
                    "__qualname__": event_class_qual,
                }

                subclass_name = event_class.__name__
                decorator_event_subclass = cast(
                    type[DecoratorEvent],
                    type(
                        subclass_name,
                        (DecoratorEvent, event_class),
                        event_subclass_dict,
                    ),
                )
                # Update the enduring object class dict.
                namespace[attr] = decorator_event_subclass
                # Remember which event event class to trigger when method is called.
                # TODO: Unify DecoratorEvent with core library somehow.
                decorator_event_classes[value] = decorator_event_subclass  # type: ignore[assignment]
                # Remember which method body to execute when event is applied.
                decorated_funcs[decorator_event_subclass] = value.decorated_func

        # Deal with cross-cutting events.
        enduring_object_class_topic = construct_topic(cls)
        for topic, decorator in underscore_method_decorators:
            if topic.startswith(enduring_object_class_topic):

                event_class = decorator.given_event_cls
                # Just keep things simple by only supporting given classes (not names).
                assert event_class is not None, "Event class not given"
                assert issubclass(event_class, CanMutateEnduringObject)
                # TODO: Maybe support event name strings, maybe not....
                event_class_qual = event_class.__qualname__

                # Assume this is a cross-cutting event, and we need to register
                # multiple handler methods for the same class. Expect its mutate
                # method will be called once for each enduring object tagged in
                # its instances. The decorator event can then select which
                # method body to call, according to the 'obj' argument of its
                # apply() method. This means we do need to subclass the given
                # event once only.

                event_class_topic = construct_topic(event_class)
                try:
                    # Get the cross-cutting event subclass if already subclassed.
                    event_subclass = cross_cutting_event_classes[event_class_topic]
                except KeyError:
                    # Subclass the cross-cutting event class.
                    # Keep things simple by only supporting non-nested classes.
                    assert (
                        "." not in event_class_qual
                    ), "Nested cross-cutting classes aren't supported"
                    # Get the global namespace for the event class.
                    event_class_globalns = getattr(
                        sys.modules.get(event_class.__module__, None),
                        "__dict__",
                        {},
                    )
                    assert event_class_qual in event_class_globalns
                    event_subclass_dict = {
                        # "__annotations__": annotations,
                        "__module__": cls.__module__,
                        "__qualname__": event_class_qual,
                    }
                    subclass_name = event_class.__name__
                    event_subclass = cast(
                        type[DecoratorEvent],
                        type(
                            subclass_name,
                            (DecoratorEvent, event_class),
                            event_subclass_dict,
                        ),
                    )
                    cross_cutting_event_classes[event_class_topic] = event_subclass
                    event_class_globalns[event_class_qual] = event_subclass

                # Register decorated func for event class / enduring object class.
                try:
                    decorated_func_collection = cross_cutting_decorated_funcs[
                        event_class_topic
                    ]
                except KeyError:
                    decorated_func_collection = {}
                    cross_cutting_decorated_funcs[event_class_topic] = (
                        decorated_func_collection
                    )

                decorated_func_collection[cls] = decorator.decorated_func

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
                decision_cls=init_enduring_object_class,
                **kwargs,
            ),
        )

    @property
    def id_attr_name(cls) -> str:
        return f"{cls.__name__.lower()}_id"


class EnduringObject(Perspective, metaclass=MetaEnduringObject):
    @classmethod
    def _create(
        cls: type[Self], decision_cls: type[CanInitialiseEnduringObject], **kwargs: Any
    ) -> Self:
        enduring_object_id = cls._create_id()
        initial_kwargs: dict[str, Any] = {cls.id_attr_name: enduring_object_id}
        initial_kwargs.update(kwargs)
        initial_kwargs["originator_topic"] = get_topic(cls)
        initial_kwargs["tags"] = [enduring_object_id]
        try:
            initialised = decision_cls(**initial_kwargs)
        except TypeError as e:
            msg = (
                f"Unable to construct {decision_cls.__qualname__} event "
                f"with kwargs {initial_kwargs}: {e}"
            )
            raise TypeError(msg) from e
        enduring_object = cast(Self, initialised.mutate(None))
        assert enduring_object is not None
        enduring_object.new_decisions.append(initialised)
        return enduring_object

    @classmethod
    def _create_id(cls) -> str:
        return f"{cls.__name__.lower()}-{uuid4()}"

    def __base_init__(self, id: str) -> None:  # noqa: A002
        super().__base_init__()
        self.id = id

    def __post_init__(self) -> None:
        pass

    @property
    def cb(self) -> list[Selector]:
        return [Selector(tags=[self.id])]

    def trigger_event(
        self,
        decision_cls: type[CanMutateEnduringObject],
        *,
        tags: Sequence[str] = (),
        **kwargs: Any,
    ) -> None:
        tags = [self.id, *tags]
        kwargs["tags"] = tags
        decision = decision_cls(**kwargs)
        decision.mutate(self)
        self.new_decisions.append(decision)


class Group(Perspective):
    @property
    def cb(self) -> list[Selector]:
        return [
            cb
            for cbs in [
                o.cb for o in self.__dict__.values() if isinstance(o, EnduringObject)
            ]
            for cb in cbs
        ]

    def trigger_event(
        self,
        decision_cls: type[CanMutateEnduringObject],
        *,
        tags: Sequence[str] = (),
        **kwargs: Any,
    ) -> None:
        objs = [o for o in self.__dict__.values() if isinstance(o, EnduringObject)]
        tags = [o.id for o in objs] + list(tags)
        kwargs["tags"] = tags
        decision = decision_cls(**kwargs)
        for o in objs:
            decision.mutate(o)
        self.new_decisions.append(decision)


TGroup = TypeVar("TGroup", bound=Group)


class DCBMapper(ABC):
    @abstractmethod
    def to_dcb_event(self, event: CanMutateEnduringObject) -> DCBEvent:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def to_domain_event(self, event: DCBEvent) -> CanMutateEnduringObject:
        raise NotImplementedError  # pragma: no cover


class Selector:
    def __init__(
        self,
        types: Sequence[type[CanMutateEnduringObject]] = (),
        tags: Sequence[str] = (),
    ):
        self.types = types
        self.tags = tags

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self.__dict__ == other.__dict__


class EventStore:
    def __init__(self, mapper: DCBMapper, recorder: DCBEventStore):
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


class Repository:
    def __init__(self, eventstore: EventStore):
        self.eventstore = eventstore

    def save(self, obj: Perspective) -> int:
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
                obj = objs.get(tag)
                if not isinstance(event, CanInitialiseEnduringObject) and not obj:
                    continue
                obj = event.mutate(obj)
                objs[tag] = obj
        for obj in objs.values():
            if obj is not None:
                obj.last_known_position = head
        return list(objs.values())

    def get_group(self, cls: type[TGroup], *enduring_object_ids: str) -> TGroup:
        enduring_objects = self.get_many(*enduring_object_ids)
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


class NotFoundError(Exception):
    pass


# Introduce and support msgspec.Struct for event definition and serialisation.


class MsgspecStructMapper(DCBMapper):
    def to_dcb_event(self, event: CanMutateEnduringObject) -> DCBEvent:
        return DCBEvent(
            type=get_topic(type(event)),
            data=msgspec.msgpack.encode(event),
            tags=event.tags,
        )

    def to_domain_event(self, event: DCBEvent) -> CanMutateEnduringObject:
        return msgspec.msgpack.decode(
            event.data,
            type=resolve_topic(event.type),
        )


class Decision(msgspec.Struct, CanMutateEnduringObject):
    tags: list[str]

    def _as_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self.__struct_fields__}


class InitialDecision(Decision, CanInitialiseEnduringObject):
    originator_topic: str
