from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, cast
from uuid import uuid4

from typing_extensions import Self, TypeVar

from eventsourcing.domain import (
    AbstractDCBEvent,
    AbstractDecoratedFuncCaller,
    CallableType,
    ProgrammingError,
    all_func_decorators,
    decorated_func_callers,
    filter_kwargs_for_method_params,
)
from eventsourcing.utils import construct_topic, get_topic, resolve_topic

if TYPE_CHECKING:
    from collections.abc import Sequence

_enduring_object_init_classes: dict[type[Any], type[Initialises]] = {}


class Mutates(AbstractDCBEvent):
    tags: list[str]

    def _as_dict(self) -> dict[str, Any]:
        raise NotImplementedError  # pragma: no cover

    def mutate(self, obj: TPerspective | None) -> TPerspective | None:
        assert obj is not None
        self.apply(obj)
        return obj

    def apply(self, obj: Any) -> None:
        pass


class Initialises(Mutates):
    originator_topic: str

    def mutate(self, obj: TPerspective | None) -> TPerspective | None:
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
    def id_attr_name(cls, enduring_object_class: type[EnduringObject[Any]]) -> str:
        return f"{enduring_object_class.__name__.lower()}_id"


class DecoratedFuncCaller(Mutates, AbstractDecoratedFuncCaller):
    def apply(self, obj: Perspective) -> None:
        """Applies event by calling method decorated by @event."""

        # Identify the function that was decorated.
        try:
            decorated_func = cross_cutting_decorated_funcs[(type(obj), type(self))]
        except KeyError:
            return

        # Select event attributes mentioned in function signature.
        self_dict = self._as_dict()
        kwargs = filter_kwargs_for_method_params(self_dict, decorated_func)

        # Call the original method with event attribute values.
        decorated_method = decorated_func.__get__(obj, type(obj))
        try:
            decorated_method(**kwargs)
        except TypeError as e:  # pragma: no cover
            # TODO: Write a test that does this...
            msg = (
                f"Failed to apply {type(self).__qualname__} to "
                f"{type(obj).__qualname__} with kwargs {kwargs}: {e}"
            )
            raise TypeError(msg) from e

        # Call super method, just in case.
        super().apply(obj)


T = TypeVar("T")


class MetaPerspective(type):
    pass


class Perspective(metaclass=MetaPerspective):
    last_known_position: int | None
    new_decisions: tuple[Mutates, ...]

    def __new__(cls, *_: Any, **__: Any) -> Self:
        perspective = super().__new__(cls)
        perspective.last_known_position = None
        perspective.new_decisions = ()
        return perspective

    def append(self, *events: Mutates) -> None:
        self.new_decisions += (*events,)

    def collect_events(self) -> Sequence[Mutates]:
        collected, self.new_decisions = self.new_decisions, ()
        return collected

    @property
    def cb(self) -> list[Selector]:
        raise NotImplementedError  # pragma: no cover


TPerspective = TypeVar("TPerspective", bound=Perspective)


given_event_subclasses: dict[type[Mutates], type[DecoratedFuncCaller]] = {}
cross_cutting_decorated_funcs: dict[
    tuple[MetaPerspective, type[Mutates]], CallableType
] = {}


class MetaEnduringObject(MetaPerspective):
    def __init__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, Any]
    ) -> None:
        super().__init__(name, bases, namespace)
        # Find and remember the "initialised" class.
        for item in cls.__dict__.values():
            if isinstance(item, type) and issubclass(item, Initialises):
                _enduring_object_init_classes[cls] = item
                break

        # Find the event decorators.
        topic_prefix = construct_topic(cls) + "."

        for decorator in all_func_decorators:
            func_topic = construct_topic(decorator.decorated_func)
            if not func_topic.startswith(topic_prefix):
                # Only deal with decorators on this slice.
                continue

            given_event_class = decorator.given_event_cls
            # Keep things simple by only supporting given classes (not names).
            # TODO: Maybe support event name strings, maybe not....
            assert given_event_class is not None, "Event class not given"
            # Make sure event decorator has a Mutates class.
            assert issubclass(given_event_class, Mutates)

            # Assume this is a cross-cutting event, and we need to register
            # multiple handler methods for the same class. Expect its mutate
            # method will be called once for each enduring object tagged in
            # its instances. The decorator event can then select which
            # method body to call, according to the type of the 'obj' argument
            # of its apply() method. This means we do need to subclass the given
            # event class, but only once.
            if given_event_class in given_event_subclasses:  # pragma: no cover
                # TODO: We never get here...
                # Decorator is seeing an original event class.
                assert not issubclass(given_event_class, DecoratedFuncCaller)
                event_subclass = given_event_subclasses[given_event_class]
            elif issubclass(given_event_class, DecoratedFuncCaller):
                # Decorator is already seeing a subclass of an original class.
                assert given_event_class in given_event_subclasses.values()
                event_subclass = given_event_class
            else:
                # Subclass the original event class once only.
                event_class_topic = construct_topic(given_event_class)

                event_class_globalns = getattr(
                    sys.modules.get(given_event_class.__module__, None),
                    "__dict__",
                    {},
                )

                if event_class_topic.startswith(topic_prefix):
                    # Looks like a nested event on this class.
                    assert given_event_class.__name__ in cls.__dict__
                    assert cls.__dict__[given_event_class.__name__] is given_event_class
                elif "." not in given_event_class.__qualname__:
                    # Looks like a module-level event class.
                    # Get the global namespace for the event class.
                    # Check the given event class is in the globalns.
                    assert given_event_class.__name__ in event_class_globalns
                    assert (
                        event_class_globalns[given_event_class.__name__]
                        is given_event_class
                    )
                else:  # pragma: no cover
                    # TODO: Write a test that does this....
                    msg = f"Decorating {cls} with {given_event_class} is not supported"
                    raise ProgrammingError(msg)
                # Define a subclass of the given event class.
                event_subclass_dict = {
                    "__module__": cls.__module__,
                    "__qualname__": given_event_class.__qualname__,
                }
                subclass_name = given_event_class.__name__
                event_subclass = cast(
                    type[DecoratedFuncCaller],
                    type(
                        subclass_name,
                        (DecoratedFuncCaller, given_event_class),
                        event_subclass_dict,
                    ),
                )

                # Replace the given event class in its namespace.
                if event_class_topic.startswith(topic_prefix):
                    setattr(cls, subclass_name, event_subclass)
                else:
                    event_class_globalns[subclass_name] = event_subclass

                # Remember which subclass for given event class.
                given_event_subclasses[given_event_class] = event_subclass

            # Register decorated func for (event class, enduring object class) tuple.
            cross_cutting_decorated_funcs[(cls, event_subclass)] = (
                decorator.decorated_func
            )

            # Maybe remember which event class to trigger when method is called.
            if not func_topic.endswith("._"):
                decorated_func_callers[decorator] = event_subclass

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
                f"Initialises class. Please define a subclass of "
                f"Initialises as a nested class on {cls.__name__}."
            )
            raise ProgrammingError(msg) from None

        return cast(
            T,
            cls._create(
                decision_cls=init_enduring_object_class,
                **kwargs,
            ),
        )


TID = TypeVar("TID", bound=str, default=str)


class EnduringObject(Perspective, Generic[TID], metaclass=MetaEnduringObject):
    id: TID

    @classmethod
    def _create(
        cls: type[Self], decision_cls: type[Initialises], **kwargs: Any
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
    def _create_id(cls) -> TID:
        return cast(TID, f"{cls.__name__.lower()}-{uuid4()}")

    def __post_init__(self) -> None:
        pass

    @property
    def cb(self) -> list[Selector]:
        return [Selector(tags=[self.id])]

    def trigger_event(
        self,
        decision_cls: type[Mutates],
        *,
        tags: Sequence[str] = (),
        **kwargs: Any,
    ) -> None:
        tags = [self.id, *tags]
        kwargs["tags"] = tags
        assert issubclass(decision_cls, DecoratedFuncCaller), decision_cls
        decision = decision_cls(**kwargs)
        decision.mutate(self)
        self.new_decisions += (decision,)


class Group(Perspective):
    @property
    def cb(self) -> list[Selector]:
        return [
            Selector(tags=cb.tags)
            for cbs in [
                o.cb for o in self.__dict__.values() if isinstance(o, EnduringObject)
            ]
            for cb in cbs
        ]

    def trigger_event(
        self,
        decision_cls: type[Mutates],
        *,
        tags: Sequence[str] = (),
        **kwargs: Any,
    ) -> None:
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

    def collect_events(self) -> Sequence[Mutates]:
        group_events = list(super().collect_events())
        for o in self.enduring_objects:
            group_events.extend(o.collect_events())
        return group_events


@dataclass
class Selector:
    types: Sequence[type[Mutates]] = ()
    tags: Sequence[str] = ()


class MetaSlice(MetaPerspective):
    def __init__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, Any]
    ) -> None:
        super().__init__(name, bases, namespace)

        # Find the event decorators.
        slice_topic = construct_topic(cls)
        expected_func_prefix = slice_topic + "."

        for decorator in all_func_decorators:
            func_topic = construct_topic(decorator.decorated_func)
            if not func_topic.startswith(expected_func_prefix):
                # Only deal with decorators on this slice.
                continue

            given_event_class = decorator.given_event_cls
            # Keep things simple by only supporting given classes (not names).
            # TODO: Maybe support event name strings, maybe not....
            assert given_event_class is not None, "Event class not given"
            # Make sure event decorator has a Mutates class.
            assert issubclass(given_event_class, Mutates)

            # Assume this is a cross-cutting event, and we need to register
            # multiple handler methods for the same class. Expect its mutate
            # method will be called once for each enduring object tagged in
            # its instances. The decorator event can then select which
            # method body to call, according to the type of the 'obj' argument
            # of its apply() method. This means we do need to subclass the given
            # event class, but only once.
            if given_event_class in given_event_subclasses:  # pragma: no cover
                # Decorator is seeing an original event class.
                # --- apparently we never get here.
                assert not issubclass(given_event_class, DecoratedFuncCaller)
                event_subclass = given_event_subclasses[given_event_class]
            elif issubclass(given_event_class, DecoratedFuncCaller):
                # Decorator is already seeing a subclass of an original class.
                assert given_event_class in given_event_subclasses.values()
                event_subclass = given_event_class
            else:
                # Subclass the cross-cutting event class once only.
                #  - keep things simple by only supporting non-nested classes
                event_class_qual = given_event_class.__qualname__
                assert (
                    "." not in event_class_qual
                ), "Nested cross-cutting classes aren't supported"
                # Get the global namespace for the event class.
                event_class_globalns = getattr(
                    sys.modules.get(given_event_class.__module__, None),
                    "__dict__",
                    {},
                )
                # Check the given event class is in the globalns.
                assert event_class_qual in event_class_globalns
                assert event_class_globalns[event_class_qual] is given_event_class
                # Define a subclass of the given event class.
                event_subclass_dict = {
                    "__module__": cls.__module__,
                    "__qualname__": event_class_qual,
                }
                subclass_name = given_event_class.__name__
                event_subclass = cast(
                    type[DecoratedFuncCaller],
                    type(
                        subclass_name,
                        (DecoratedFuncCaller, given_event_class),
                        event_subclass_dict,
                    ),
                )
                # Replace the given event class in its namespace.
                event_class_globalns[event_class_qual] = event_subclass

                # Remember which subclass for given event class.
                given_event_subclasses[given_event_class] = event_subclass

            # Register decorated func for event class / enduring object class.
            cross_cutting_decorated_funcs[(cls, event_subclass)] = (
                decorator.decorated_func
            )


class Slice(Perspective, metaclass=MetaSlice):
    def execute(self) -> None:
        pass


TSlice = TypeVar("TSlice", bound=Slice)
TGroup = TypeVar("TGroup", bound=Group)
