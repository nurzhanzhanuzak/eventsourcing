from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import uuid4

from typing_extensions import Self

from eventsourcing.domain import (
    AbstractDCBEvent,
    AbstractDecoratedFuncCaller,
    CallableType,
    CommandMethodDecorator,
    ProgrammingError,
    decorated_func_callers,
    decorated_funcs,
    filter_kwargs_for_method_params,
    underscore_method_decorators,
)
from eventsourcing.persistence import IntegrityError
from eventsourcing.utils import construct_topic, get_topic, resolve_topic

if TYPE_CHECKING:
    from collections.abc import Sequence

_enduring_object_init_classes: dict[type[Any], type[CanInitialiseEnduringObject]] = {}


class CanMutateEnduringObject(AbstractDCBEvent):
    tags: list[str]

    def _as_dict(self) -> dict[str, Any]:
        raise NotImplementedError  # pragma: no cover

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
        originator_topic = resolve_topic(kwargs.pop("originator_topic"))
        enduring_object_cls = cast(type[EnduringObject], originator_topic)
        enduring_object_id = kwargs.pop(self.id_attr_name(enduring_object_cls))
        kwargs.pop("tags")
        try:
            enduring_object = type.__call__(enduring_object_cls, **kwargs)
        except TypeError as e:
            msg = (
                f"{type(self).__qualname__} cannot __init__ "
                f"{enduring_object_cls.__qualname__} "
                f"with kwargs {kwargs}: {e}"
            )
            raise TypeError(msg) from e
        enduring_object.id = enduring_object_id
        enduring_object.__post_init__()
        return enduring_object

    @classmethod
    def id_attr_name(cls, enduring_object_class: type[EnduringObject]) -> str:
        return f"{enduring_object_class.__name__.lower()}_id"


class DecoratedFuncCaller(CanMutateEnduringObject, AbstractDecoratedFuncCaller):
    def apply(self, obj: EnduringObject) -> None:
        """Applies event by calling method decorated by @event."""

        event_class_topic = construct_topic(type(self))

        # Identify the function that was decorated.
        try:
            # Either an "underscore" non-command method.
            decorated_func_collection = cross_cutting_decorated_funcs[event_class_topic]
            assert type(obj) in decorated_func_collection
            decorated_func = decorated_func_collection[type(obj)]

        except KeyError:
            # Or a normal command method.
            decorated_func = decorated_funcs[type(self)]

        # Select event attributes mentioned in function signature.
        self_dict = self._as_dict()
        kwargs = filter_kwargs_for_method_params(self_dict, decorated_func)

        # Call the original method with event attribute values.
        decorated_method = decorated_func.__get__(obj, type(obj))
        decorated_method(**kwargs)

        # Call super method, just in case.
        super().apply(obj)


T = TypeVar("T")


class MetaPerspective(type):
    pass


class Perspective(metaclass=MetaPerspective):
    last_known_position: int | None
    cb_types: Sequence[type[CanMutateEnduringObject]] = ()
    new_decisions: tuple[CanMutateEnduringObject, ...]

    def __new__(cls, *_: Any, **__: Any) -> Self:
        perspective = super().__new__(cls)
        perspective.last_known_position = None
        perspective.cb_types = cls.cb_types
        perspective.new_decisions = ()
        return perspective

    def collect_events(self) -> Sequence[CanMutateEnduringObject]:
        collected, self.new_decisions = self.new_decisions, ()
        return collected

    @property
    def cb(self) -> list[Selector]:
        raise NotImplementedError  # pragma: no cover

    def check_cb_types(self, decision_cls: type[CanMutateEnduringObject]) -> None:
        if self.cb_types and decision_cls not in self.cb_types:
            msg = (
                f"Decision type {decision_cls.__qualname__} "
                f"not in consistency boundary types: {self.cb_types}"
            )
            raise IntegrityError(msg)


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
                    # Deal with cross-cutting events later.
                    continue

                event_class = value.given_event_cls
                # Just keep things simple by only supporting given classes (not names).
                assert event_class is not None, "Event class not given"
                assert issubclass(event_class, CanMutateEnduringObject)
                # TODO: Maybe support event name strings, maybe not....
                event_class_qual = event_class.__qualname__

                # Keep things simple by only supporting nested classes.
                assert event_class_qual.startswith(cls.__qualname__ + ".")
                assert cls.__dict__[event_class.__name__] is event_class

                # Subclass given class to make a "decorator class".
                event_subclass_dict = {
                    "__module__": cls.__module__,
                    "__qualname__": event_class_qual,
                }

                subclass_name = event_class.__name__
                event_subclass = cast(
                    type[DecoratedFuncCaller],
                    type(
                        subclass_name,
                        (DecoratedFuncCaller, event_class),
                        event_subclass_dict,
                    ),
                )
                # Update the enduring object class dict.
                setattr(cls, event_class.__name__, event_subclass)
                # Remember which event class to trigger when method is called.
                decorated_func_callers[value] = event_subclass
                # Remember which method body to execute when event is applied.
                decorated_funcs[event_subclass] = value.decorated_func

        # Deal with cross-cutting events.
        enduring_object_class_topic = construct_topic(cls)
        for topic, decorator in underscore_method_decorators:
            if topic.startswith(enduring_object_class_topic):

                event_class = decorator.given_event_cls
                # Keep things simple by only supporting given classes (not names).
                # TODO: Maybe support event name strings, maybe not....
                assert event_class is not None, "Event class not given"
                # Make sure event decorator has a CanMutateEnduringObject class.
                assert issubclass(event_class, CanMutateEnduringObject)

                # Assume this is a cross-cutting event, and we need to register
                # multiple handler methods for the same class. Expect its mutate
                # method will be called once for each enduring object tagged in
                # its instances. The decorator event can then select which
                # method body to call, according to the 'obj' argument of its
                # apply() method. This means we do need to subclass the given
                # event once only.
                event_class_topic = construct_topic(event_class)
                if event_class_topic not in cross_cutting_event_classes:
                    # Subclass the cross-cutting event class once only.
                    #  - keep things simple by only supporting non-nested classes
                    event_class_qual = event_class.__qualname__
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
                        "__module__": cls.__module__,
                        "__qualname__": event_class_qual,
                    }
                    subclass_name = event_class.__name__
                    event_subclass = cast(
                        type[DecoratedFuncCaller],
                        type(
                            subclass_name,
                            (DecoratedFuncCaller, event_class),
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
        #  event's __init__ is valid when initialising the class,
        #  just like we do for event-sourced aggregates.

        assert issubclass(cls, EnduringObject)
        try:
            init_enduring_object_class = _enduring_object_init_classes[cls]
        except KeyError:
            msg = (
                f"Enduring object class {cls.__name__} has no "
                f"CanInitialiseEnduringObject class. Please define a subclass of "
                f"CanInitialiseEnduringObject as a nested class on {cls.__name__}."
            )
            raise ProgrammingError(msg) from None

        return cast(
            T,
            cls._create(
                decision_cls=init_enduring_object_class,
                **kwargs,
            ),
        )


class EnduringObject(Perspective, metaclass=MetaEnduringObject):
    id: str

    @classmethod
    def _create(
        cls: type[Self], decision_cls: type[CanInitialiseEnduringObject], **kwargs: Any
    ) -> Self:
        enduring_object_id = cls._create_id()
        id_attr_name = decision_cls.id_attr_name(cls)
        assert id_attr_name not in kwargs
        assert "originator_topic" not in kwargs
        assert "tags" not in kwargs
        initial_kwargs: dict[str, Any] = {
            id_attr_name: enduring_object_id,
            "originator_topic": get_topic(cls),
            "tags": [enduring_object_id],
        }
        initial_kwargs.update(kwargs)
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
        enduring_object.new_decisions += (initialised,)
        return enduring_object

    @classmethod
    def _create_id(cls) -> str:
        return f"{cls.__name__.lower()}-{uuid4()}"

    def __post_init__(self) -> None:
        pass

    @property
    def cb(self) -> list[Selector]:
        return [Selector(tags=[self.id], types=self.cb_types)]

    def trigger_event(
        self,
        decision_cls: type[CanMutateEnduringObject],
        *,
        tags: Sequence[str] = (),
        **kwargs: Any,
    ) -> None:
        tags = [self.id, *tags]
        kwargs["tags"] = tags
        self.check_cb_types(decision_cls)
        assert issubclass(decision_cls, DecoratedFuncCaller), decision_cls
        decision = decision_cls(**kwargs)
        decision.mutate(self)
        self.new_decisions += (decision,)


class Group(Perspective):
    @property
    def cb(self) -> list[Selector]:
        return [
            Selector(types=tuple(self.cb_types) + tuple(cb.types), tags=cb.tags)
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
        self.check_cb_types(decision_cls)
        objs = self.enduring_objects
        tags = [o.id for o in objs] + list(tags)
        kwargs["tags"] = tags
        decision = decision_cls(**kwargs)
        for o in objs:
            decision.mutate(o)
        self.new_decisions += (decision,)

    @property
    def enduring_objects(self) -> Sequence[EnduringObject]:
        return [o for o in self.__dict__.values() if isinstance(o, EnduringObject)]

    def collect_events(self) -> Sequence[CanMutateEnduringObject]:
        group_events = list(super().collect_events())
        for o in self.enduring_objects:
            group_events.extend(o.collect_events())
        return group_events


@dataclass
class Selector:
    types: Sequence[type[CanMutateEnduringObject]] = ()
    tags: Sequence[str] = ()
