from __future__ import annotations

import dataclasses
import importlib
import inspect
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, tzinfo
from functools import cache
from types import FunctionType, WrapperDescriptorType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Protocol,
    TypeVar,
    Union,
    cast,
    overload,
    runtime_checkable,
)
from uuid import UUID, uuid4
from warnings import warn

from eventsourcing.utils import get_method_name, get_topic, resolve_topic

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from typing_extensions import Self


TZINFO: tzinfo = resolve_topic(os.getenv("TZINFO_TOPIC", "datetime:timezone.utc"))
"""
A Python :py:obj:`tzinfo` object that defaults to UTC (:py:obj:`timezone.utc`). Used
as the timezone argument in :func:`~eventsourcing.domain.datetime_now_with_tzinfo`.

Set environment variable ``TZINFO_TOPIC`` to the topic of a different :py:obj:`tzinfo`
object so that all your domain model event timestamps are located in that timezone
(not recommended). It is generally recommended to locate all timestamps in the UTC
domain and convert to local timezones when presenting values in user interfaces.
"""


class EventsourcingType(type):
    """Base type for event sourcing domain model types (aggregates and events)."""


_T = TypeVar("_T")


def patch_dataclasses_process_class() -> None:
    dataclasses_module = importlib.import_module("dataclasses")
    original_process_class_func = dataclasses_module.__dict__["_process_class"]

    def _patched_dataclasses_process_class(
        cls: type[_T], *args: Any, **kwargs: Any
    ) -> type[_T]:
        # Avoid processing aggregate and event dataclasses twice,
        # because doing so screws up non-init and default fields.
        if (
            cls
            and isinstance(cls, EventsourcingType)
            and "__dataclass_fields__" in cls.__dict__
        ):
            return cls
        return original_process_class_func(cls, *args, **kwargs)

    dataclasses_module.__dict__["_process_class"] = _patched_dataclasses_process_class


patch_dataclasses_process_class()


@runtime_checkable
class DomainEventProtocol(Protocol):
    """Protocol for domain event objects.

    A protocol is defined to allow the event sourcing mechanisms
    to work with different kinds of domain event classes. Whilst
    the library by default uses frozen dataclasses to implement
    its domain event classes, it is also possible to use other
    kinds of domain event classes, such as Pydantic classes.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass  # pragma: no cover

    @property
    def originator_id(self) -> UUID:
        """UUID identifying an aggregate to which the event belongs."""
        raise NotImplementedError  # pragma: no cover

    @property
    def originator_version(self) -> int:
        """Integer identifying the version of the aggregate when the event occurred."""
        raise NotImplementedError  # pragma: no cover


TDomainEvent = TypeVar("TDomainEvent", bound=DomainEventProtocol)
SDomainEvent = TypeVar("SDomainEvent", bound=DomainEventProtocol)


class MutableAggregateProtocol(Protocol):
    """Protocol for mutable aggregate objects.

    A protocol is defined to allow the event sourcing mechanisms
    to work with different kinds of aggregate classes. Whilst
    the library by default recommends using mutable classes to
    implement aggregate classes, it is also possible to implement
    immutable aggregate classes, and this is supported by this library.
    """

    @property
    def id(self) -> UUID:
        """Mutable aggregates have a read-only ID that is a UUID."""
        raise NotImplementedError  # pragma: no cover

    @property
    def version(self) -> int:
        """Mutable aggregates have a read-write version that is an int."""
        raise NotImplementedError  # pragma: no cover

    @version.setter
    def version(self, value: int) -> None:
        """Mutable aggregates have a read-write version that is an int."""
        raise NotImplementedError  # pragma: no cover


class ImmutableAggregateProtocol(Protocol):
    """Protocol for immutable aggregate objects.

    A protocol is defined to allow the event sourcing mechanisms
    to work with different kinds of aggregate classes. Whilst
    the library by default recommends using mutable classes to
    implement aggregate classes, it is also possible to implement
    immutable aggregate classes, and this is supported by this library.
    """

    @property
    def id(self) -> UUID:
        """Immutable aggregates have a read-only ID that is a UUID."""
        raise NotImplementedError  # pragma: no cover

    @property
    def version(self) -> int:
        """Immutable aggregates have a read-only version that is an int."""
        raise NotImplementedError  # pragma: no cover


MutableOrImmutableAggregate = Union[
    ImmutableAggregateProtocol, MutableAggregateProtocol
]
"""Type alias defining a union of mutable and immutable aggregate protocols."""


TMutableOrImmutableAggregate = TypeVar(
    "TMutableOrImmutableAggregate", bound=MutableOrImmutableAggregate
)
"""Type variable bound by the union of mutable and immutable aggregate protocols."""


@runtime_checkable
class CollectEventsProtocol(Protocol):
    """Protocol for aggregates that support collecting pending events."""

    def collect_events(self) -> Sequence[DomainEventProtocol]:
        """Returns a sequence of events."""
        raise NotImplementedError  # pragma: no cover


@runtime_checkable
class CanMutateProtocol(DomainEventProtocol, Protocol[TMutableOrImmutableAggregate]):
    """Protocol for events that have a mutate method."""

    def mutate(
        self, aggregate: TMutableOrImmutableAggregate | None
    ) -> TMutableOrImmutableAggregate | None:
        """
        Evolves the state of the given aggregate, either by
        returning the given aggregate instance with modified attributes
        or by constructing and returning a new aggregate instance.
        """


def datetime_now_with_tzinfo() -> datetime:
    """
    Constructs a timezone-aware :class:`datetime`
    object for the current date and time.

    Uses :py:obj:`TZINFO` as the timezone.
    """
    return datetime.now(tz=TZINFO)


def create_utc_datetime_now() -> datetime:
    """Deprected in favour of :func:`~eventsourcing.domain.datetime_now_with_tzinfo`."""
    msg = (
        "'create_utc_datetime_now()' is deprecated, "
        "use 'datetime_now_with_tzinfo()' instead"
    )
    warn(msg, DeprecationWarning, stacklevel=2)
    return datetime_now_with_tzinfo()


class CanCreateTimestamp:
    """Provides a create_timestamp() method to subclasses."""

    @staticmethod
    def create_timestamp() -> datetime:
        """Constructs a timezone-aware :class:`datetime` object
        representing when an event occurred.
        """
        return datetime_now_with_tzinfo()


TAggregate = TypeVar("TAggregate", bound="BaseAggregate")


class HasOriginatorIDVersion:
    """Declares ``originator_id`` and ``originator_version`` attributes."""

    originator_id: UUID
    """UUID identifying an aggregate to which the event belongs."""
    originator_version: int
    """Integer identifying the version of the aggregate when the event occurred."""


class CanMutateAggregate(HasOriginatorIDVersion, CanCreateTimestamp):
    """Implements a :func:`~eventsourcing.domain.CanMutateAggregate.mutate`
    method that evolves the state of an aggregate.
    """

    # TODO: Move this to a HasTimestamp? Why is it here??
    timestamp: datetime
    """Timezone-aware :class:`datetime` object representing when an event occurred."""

    def mutate(self, aggregate: TAggregate | None) -> TAggregate | None:
        """Validates and adjustes the attributes of the given ``aggregate`
         argument. The argument typed as ``Optional`` but the value is
         expected to be not ``None``.

        Validates the ``aggregate`` argument by checking the event's
        :py:attr:`~eventsourcing.domain.HasOriginatorIDVersion.originator_id` equals the
        ``aggregate``'s :py:attr:`~eventsourcing.domain.Aggregate.id`, and the event's
        :py:attr:`~eventsourcing.domain.HasOriginatorIDVersion.originator_version` is
        one greater than the ``aggregate``'s current
        :py:attr:`~eventsourcing.domain.Aggregate.version`.
        If the ``aggregate`` argument is not valid, an exception is raised.

        If the ``aggregate`` argument is valid, the
        :func:`~eventsourcing.domain.CanMutateAggregate.apply` method is called, and
        then :py:attr:`~eventsourcing.domain.HasOriginatorIDVersion.originator_id` is
        assigned to the aggregate's :py:attr:`~eventsourcing.domain.Aggregate.version`
        attribute, and the ``timestamp`` is assigned to the aggregate's
        :py:attr:`~eventsourcing.domain.Aggregate.modified_on` attribute.
        """
        assert aggregate is not None

        # Check this event belongs to this aggregate.
        if self.originator_id != aggregate.id:
            raise OriginatorIDError(self.originator_id, aggregate.id)

        # Check this event is the next in its sequence.
        next_version = aggregate.version + 1
        if self.originator_version != next_version:
            raise OriginatorVersionError(self.originator_version, next_version)

        # Call apply() before mutating values, in case exception is raised.
        self.apply(aggregate)

        # Update the aggregate's 'version' number.
        aggregate.version = self.originator_version

        # Update the aggregate's 'modified on' time.
        aggregate.modified_on = self.timestamp

        # Return the mutated aggregate.
        return aggregate

    def apply(self, aggregate: Any) -> None:
        """Applies the domain event to its aggregate.

        This method does nothing but exist to be
        overridden as a convenient way for users
        to define how an event evolves the state
        of an aggregate.
        """


class CanInitAggregate(CanMutateAggregate):
    """Implements a :func:`~eventsourcing.domain.CanMutateAggregate.mutate`
    method that constructs the initial state of an aggregate.
    """

    originator_topic: str
    """String describing the path to an aggregate class."""

    def mutate(self, aggregate: TAggregate | None) -> TAggregate | None:
        """Constructs an aggregate instance according to the attributes of an event.

        The ``aggregate`` argument is typed as an optional argument, but the
        value is expected to be ``None``.
        """
        assert aggregate is None

        # Resolve originator topic.
        aggregate_class: type[TAggregate] = resolve_topic(self.originator_topic)

        # Construct an aggregate object (a "shell" of the correct object type).
        agg = aggregate_class.__new__(aggregate_class)

        # Pick out event attributes for the aggregate base class init method.
        base_kwargs = _filter_kwargs_for_method_params(
            self.__dict__, type(agg).__base_init__
        )

        # Call the base class init method (so we don't need to always write
        # a call to super().__init__() in every aggregate __init__() method).
        agg.__base_init__(**base_kwargs)

        # Pick out event attributes for aggregate subclass class init method.
        init_kwargs = _filter_kwargs_for_method_params(
            self.__dict__, type(agg).__init__
        )

        # Provide the aggregate id, if the __init__ method expects it.
        if aggregate_class in _init_mentions_id:
            init_kwargs["id"] = self.__dict__["originator_id"]

        # Call the aggregate subclass class init method.
        agg.__init__(**init_kwargs)  # type: ignore[misc]

        # Call the event apply method (alternative to using __init__())
        self.apply(agg)

        # Return the constructed and initialised aggregate object.
        return agg


class MetaDomainEvent(EventsourcingType):
    """Metaclass which ensures all domain event classes are frozen dataclasses."""

    def __new__(
        cls, name: str, bases: tuple[type[TDomainEvent], ...], cls_dict: dict[str, Any]
    ) -> type[TDomainEvent]:
        event_cls = cast(
            "type[TDomainEvent]", super().__new__(cls, name, bases, cls_dict)
        )
        event_cls = dataclasses.dataclass(frozen=True)(event_cls)
        event_cls.__signature__ = inspect.signature(event_cls.__init__)  # type: ignore[attr-defined]
        return event_cls


@dataclass(frozen=True)
class DomainEvent(CanCreateTimestamp, metaclass=MetaDomainEvent):
    """Frozen data class representing domain model events."""

    originator_id: UUID
    """UUID identifying an aggregate to which the event belongs."""
    originator_version: int
    """Integer identifying the version of the aggregate when the event occurred."""
    timestamp: datetime
    """Timezone-aware :class:`datetime` object representing when an event occurred."""


class AggregateEvent(CanMutateAggregate, DomainEvent):
    """Frozen data class representing aggregate events.

    Subclasses represent original decisions made by domain model aggregates.
    """


@dataclass(frozen=True)
class AggregateCreated(CanInitAggregate, AggregateEvent):
    """Frozen data class representing the initial creation of an aggregate."""

    originator_topic: str
    """String describing the path to an aggregate class."""


class EventSourcingError(Exception):
    """Base exception class."""


class ProgrammingError(EventSourcingError):
    """Exception class for domain model programming errors."""


class LogEvent(DomainEvent):
    """Deprecated: Inherit from DomainEvent instead.

    Base class for the events of event-sourced logs.
    """


def _filter_kwargs_for_method_params(
    kwargs: dict[str, Any], method: Callable[..., Any]
) -> dict[str, Any]:
    names = _spec_filter_kwargs_for_method_params(method)
    return {k: v for k, v in kwargs.items() if k in names}


@cache
def _spec_filter_kwargs_for_method_params(method: Callable[..., Any]) -> set[str]:
    method_signature = inspect.signature(method)
    return set(method_signature.parameters)


if TYPE_CHECKING:
    EventSpecType = Union[str, type[CanMutateAggregate]]

CallableType = Callable[..., None]
DecoratableType = Union[CallableType, property]
TDecoratableType = TypeVar("TDecoratableType", bound=DecoratableType)


class CommandMethodDecorator:
    def __init__(
        self,
        event_spec: EventSpecType | None,
        decorated_obj: DecoratableType,
    ):
        self.is_name_inferred_from_method = False
        self.given_event_cls: type[CanMutateAggregate] | None = None
        self.event_cls_name: str | None = None
        self.decorated_property: property | None = None
        self.is_property_setter = False
        self.property_setter_arg_name: str | None = None
        self.decorated_func: CallableType

        # Event name has been specified.
        if isinstance(event_spec, str):
            if event_spec == "":
                msg = "Can't use empty string as name of event class"
                raise ValueError(msg)
            self.event_cls_name = event_spec

        # Event class has been specified.
        elif isinstance(event_spec, type) and issubclass(
            event_spec, CanMutateAggregate
        ):
            if event_spec in _given_event_classes:
                name = event_spec.__name__
                msg = f"{name} event class used in more than one decorator"
                raise TypeError(msg)
            self.given_event_cls = event_spec
            _given_event_classes.add(event_spec)

        # Process a decorated property.
        if isinstance(decorated_obj, property):
            # Disallow putting event decorator on property getter.
            if decorated_obj.fset is None:
                assert decorated_obj.fget, "Property has no getter"
                method_name = decorated_obj.fget.__name__
                msg = f"@event can't decorate {method_name}() property getter"
                raise TypeError(msg)

            # Remember we are decorating a property.
            self.decorated_property = decorated_obj

            # TODO: Disallow unusual property setters in more detail.
            assert isinstance(decorated_obj.fset, FunctionType)

            # Disallow deriving event class names from property names.
            if not self.given_event_cls and not self.event_cls_name:
                method_name = decorated_obj.fset.__name__
                msg = (
                    f"@event decorator on @{method_name}.setter "
                    f"requires event name or class"
                )
                raise TypeError(msg)

            # Remember property "setter" as the decorated function.
            self.decorated_func = decorated_obj.fset

            # Remember the name of the second setter arg.
            setter_arg_names = list(inspect.signature(self.decorated_func).parameters)
            assert len(setter_arg_names) == 2
            self.property_setter_arg_name = setter_arg_names[1]

        # Process a decorated function.
        elif isinstance(decorated_obj, FunctionType):
            # Remember the decorated obj as the decorated method.
            self.decorated_func = decorated_obj

            # If necessary, derive an event class name from the method.
            if not self.given_event_cls and not self.event_cls_name:
                original_method_name = self.decorated_func.__name__
                if original_method_name != "__init__":
                    self.is_name_inferred_from_method = True
                    self.event_cls_name = "".join(
                        [s.capitalize() for s in original_method_name.split("_")]
                    )

        # Disallow decorating other types of object.
        else:
            msg = f"{decorated_obj} is not a function or property"
            raise TypeError(msg)

        # Disallow using methods with variable params to define event class.
        if self.event_cls_name:
            _raise_type_error_if_func_has_variable_params(self.decorated_func)

        # Disallow using methods with positional only params to define event class.
        if self.event_cls_name:
            _raise_type_error_if_func_has_positional_only_params(self.decorated_func)

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        # Initialised decorator was called directly, presumably by
        # a decorating property that has this decorator as its fset.
        # So trigger an event.
        assert self.is_property_setter
        assert self.property_setter_arg_name
        assert len(args) == 2
        assert len(kwargs) == 0
        assert isinstance(args[0], Aggregate)
        aggregate_instance = args[0]
        bound = BoundCommandMethodDecorator(self, aggregate_instance)
        property_setter_arg_value = args[1]
        kwargs = {self.property_setter_arg_name: property_setter_arg_value}
        bound.trigger(**kwargs)

    @overload
    def __get__(
        self, instance: None, owner: type[BaseAggregate]
    ) -> UnboundCommandMethodDecorator | property:
        """
        Descriptor protocol for getting decorated method or property on class object.
        """

    @overload
    def __get__(
        self, instance: BaseAggregate, owner: type[BaseAggregate]
    ) -> BoundCommandMethodDecorator | Any:
        """
        Descriptor protocol for getting decorated method or property on instance object.
        """

    def __get__(
        self, instance: BaseAggregate | None, owner: type[BaseAggregate]
    ) -> BoundCommandMethodDecorator | UnboundCommandMethodDecorator | property | Any:
        """Descriptor protocol for getting decorated method or property."""
        # If we are decorating a property, then delegate to the property's __get__.
        if self.decorated_property:
            return self.decorated_property.__get__(instance, owner)

        # If we are decorating an __init__ method, then delegate to the __init__ method.
        if self.decorated_func.__name__ == "__init__":
            return self.decorated_func.__get__(instance, owner)

        # Return a "bound" command method decorator if we have an instance.
        if instance:
            return BoundCommandMethodDecorator(self, instance)

        # Return an "unbound" command method decorator if we have no instance.
        return UnboundCommandMethodDecorator(self)

    def __set__(self, instance: BaseAggregate, value: Any) -> None:
        """Descriptor protocol for assigning to decorated property."""
        # Set decorated property indirectly by triggering an event.
        assert self.property_setter_arg_name
        b = BoundCommandMethodDecorator(self, instance)
        kwargs = {self.property_setter_arg_name: value}
        b.trigger(**kwargs)


@overload
def event(arg: TDecoratableType) -> TDecoratableType:
    """Signature for calling ``@event`` decorator with decorated method."""


@overload
def event(
    arg: EventSpecType,
) -> Callable[[TDecoratableType], TDecoratableType]:
    """Signature for calling ``@event`` decorator with event specification."""


@overload
def event(
    arg: None = None,
) -> Callable[[TDecoratableType], TDecoratableType]:
    """Signature for calling ``@event`` decorator without event specification."""


def event(
    arg: EventSpecType | TDecoratableType | None = None,
) -> TDecoratableType | Callable[[TDecoratableType], TDecoratableType]:
    """Event-triggering decorator for aggregate command methods and property setters.

    Can be used to decorate an aggregate method or property setter so that an
    event will be triggered when the method is called or the property is set.
    The body of the method will be used to apply the event to the aggregate,
    both when the event is triggered and when the aggregate is reconstructed
    from stored events.

    .. code-block:: python

        class MyAggregate(Aggregate):
            @event("NameChanged")
            def set_name(self, name: str):
                self.name = name

    ...is equivalent to...

    .. code-block:: python

        class MyAggregate(Aggregate):
            def set_name(self, name: str):
                self.trigger_event(self.NameChanged, name=name)

            class NameChanged(Aggregate.Event):
                name: str

                def apply(self, aggregate):
                    aggregate.name = self.name

    In the example above, the event "NameChanged" is defined automatically
    by inspecting the signature of the ``set_name()`` method. If it is
    preferred to declare the event class explicitly, for example to define
    upcasting of old events, the event class itself can be mentioned in the
    event decorator rather than just providing the name of the event as a
    string.

    .. code-block:: python

        class MyAggregate(Aggregate):

            class NameChanged(Aggregate.Event):
                name: str

            @event(NameChanged)
            def set_name(self, name: str):
                aggregate.name = self.name


    """
    if isinstance(arg, (FunctionType, property)):
        command_method_decorator = CommandMethodDecorator(
            event_spec=None,
            decorated_obj=arg,
        )
        return cast(
            "Callable[[TDecoratableType], TDecoratableType]", command_method_decorator
        )

    if (
        arg is None
        or isinstance(arg, str)
        or (isinstance(arg, type) and issubclass(arg, CanMutateAggregate))
    ):
        event_spec = arg

        def create_command_method_decorator(
            decorated_obj: TDecoratableType,
        ) -> TDecoratableType:
            command_method_decorator = CommandMethodDecorator(
                event_spec=event_spec,
                decorated_obj=decorated_obj,
            )
            return cast("TDecoratableType", command_method_decorator)

        return create_command_method_decorator

    msg = (
        f"{arg} is not a str, function, property, or subclass of "
        f"{CanMutateAggregate.__name__}"
    )
    raise TypeError(msg)


triggers = event


class UnboundCommandMethodDecorator:
    """Wraps a CommandMethodDecorator instance when accessed on an aggregate class."""

    def __init__(self, event_decorator: CommandMethodDecorator):
        """:param CommandMethodDecorator event_decorator:"""
        self.event_decorator = event_decorator
        self.__module__ = event_decorator.decorated_func.__module__
        self.__name__ = event_decorator.decorated_func.__name__
        self.__qualname__ = event_decorator.decorated_func.__qualname__
        self.__annotations__ = event_decorator.decorated_func.__annotations__
        self.__doc__ = event_decorator.decorated_func.__doc__
        # self.__wrapped__ = event_decorator.decorated_method
        # functools.update_wrapper(self, event_decorator.decorated_method)

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        # Expect first argument is an aggregate instance.
        if len(args) < 1 or not isinstance(args[0], Aggregate):
            msg = "Expected aggregate as first argument"
            raise TypeError(msg)
        aggregate: Aggregate = args[0]
        assert isinstance(aggregate, Aggregate)
        BoundCommandMethodDecorator(self.event_decorator, aggregate)(
            *args[1:], **kwargs
        )


class BoundCommandMethodDecorator:
    """Binds a CommandMethodDecorator with an aggregate instance so calls to
    decorated command methods can be intercepted and will trigger an event.
    """

    def __init__(
        self, event_decorator: CommandMethodDecorator, aggregate: BaseAggregate
    ):
        """:param CommandMethodDecorator event_decorator:
        :param Aggregate aggregate:
        """
        self.event_decorator = event_decorator
        self.__module__ = event_decorator.decorated_func.__module__
        self.__name__ = event_decorator.decorated_func.__name__
        self.__qualname__ = event_decorator.decorated_func.__qualname__
        self.__annotations__ = event_decorator.decorated_func.__annotations__
        self.__doc__ = event_decorator.decorated_func.__doc__
        self.aggregate = aggregate

    def trigger(self, *args: Any, **kwargs: Any) -> None:
        kwargs = _coerce_args_to_kwargs(
            self.event_decorator.decorated_func, args, kwargs
        )
        event_cls = decorator_event_classes[self.event_decorator]
        kwargs = _filter_kwargs_for_method_params(kwargs, event_cls)
        self.aggregate.trigger_event(event_cls, **kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        self.trigger(*args, **kwargs)


class DecoratorEvent(CanMutateAggregate):
    def apply(self, aggregate: BaseAggregate) -> None:
        """Applies event to aggregate by calling method decorated by @event."""
        # Identify the function that was decorated.
        decorated_func = _decorated_funcs[type(self)]

        # Select event attributes mentioned in function signature.
        kwargs = _filter_kwargs_for_method_params(self.__dict__, decorated_func)

        # Call the original method with event attribute values.
        decorated_method = decorated_func.__get__(aggregate, type(aggregate))
        decorated_method(**kwargs)

        # Call super method, just in case any base classes need it.
        super().apply(aggregate)


_given_event_classes: set[type] = set()
_decorated_funcs: dict[type, CallableType] = {}
_created_event_classes: dict[type, list[type[CanInitAggregate]]] = {}


decorator_event_classes: dict[CommandMethodDecorator, type[DecoratorEvent]] = {}


def _raise_type_error_if_func_has_variable_params(method: CallableType) -> None:
    for param in inspect.signature(method).parameters.values():
        if param.kind is param.VAR_POSITIONAL:
            msg = f"*{param.name} not supported by decorator on {method.__name__}()"
            raise TypeError(msg)
            # TODO: Support VAR_POSITIONAL?
            # annotations["__star_args__"] = "typing.Any"

        if param.kind is param.VAR_KEYWORD:
            # TODO: Support VAR_KEYWORD?
            # annotations["__star_kwargs__"] = "typing.Any"
            msg = f"**{param.name} not supported by decorator on {method.__name__}()"
            raise TypeError(msg)


def _raise_type_error_if_func_has_positional_only_params(method: CallableType) -> None:
    # TODO: Support POSITIONAL_ONLY?
    positional_only_params = []
    for param in inspect.signature(method).parameters.values():
        if param.name == "self":
            continue
        if param.kind is param.POSITIONAL_ONLY:
            positional_only_params.append(param.name)

    if positional_only_params:
        msg = (
            f"positional only args arg not supported by @event decorator on "
            f"{method.__name__}(): {', '.join(positional_only_params)}"
        )
        raise TypeError(msg)


def _coerce_args_to_kwargs(
    method: CallableType,
    args: Iterable[Any],
    kwargs: dict[str, Any],
    *,
    expects_id: bool = False,
) -> dict[str, Any]:
    # __init__ methods are WrapperDescriptorType, other method are FunctionType.
    assert isinstance(method, (FunctionType, WrapperDescriptorType)), method

    args = tuple(args)
    enumerated_args_names, keyword_defaults_items = _spec_coerce_args_to_kwargs(
        method=method,
        len_args=len(args),
        kwargs_keys=tuple(kwargs.keys()),
        expects_id=expects_id,
    )

    copy_kwargs = dict(kwargs)
    copy_kwargs.update({name: args[i] for i, name in enumerated_args_names})
    copy_kwargs.update(keyword_defaults_items)
    return copy_kwargs


@cache
def _spec_coerce_args_to_kwargs(
    method: CallableType,
    len_args: int,
    kwargs_keys: tuple[str],
    *,
    expects_id: bool,
) -> tuple[tuple[tuple[int, str], ...], tuple[tuple[str, Any], ...]]:
    method_signature = inspect.signature(method)
    positional_names = []
    keyword_defaults = {}
    required_positional = []
    required_keyword_only = []
    if expects_id:
        positional_names.append("id")
        required_positional.append("id")
    for name, param in method_signature.parameters.items():
        if name == "self":
            continue
        # elif param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
        if param.kind is param.KEYWORD_ONLY:
            required_keyword_only.append(name)
        if param.kind is param.POSITIONAL_OR_KEYWORD:
            positional_names.append(name)
            if param.default == param.empty:
                required_positional.append(name)
        if param.default != param.empty:
            keyword_defaults[name] = param.default
    # if not required_keyword_only and not positional_names:
    #     if args or kwargs:
    #         raise TypeError(f"{method.__name__}() takes no args")
    method_name = get_method_name(method)
    for name in kwargs_keys:
        if name not in required_keyword_only and name not in positional_names:
            msg = f"{method_name}() got an unexpected keyword argument '{name}'"
            raise TypeError(msg)
    if len_args > len(positional_names):
        msg = (
            f"{method_name}() takes {len(positional_names) + 1} "
            f"positional argument{'' if len(positional_names) + 1 == 1 else 's'} "
            f"but {len_args + 1} were given"
        )
        raise TypeError(msg)
    required_positional_not_in_kwargs = [
        n for n in required_positional if n not in kwargs_keys
    ]
    num_missing = len(required_positional_not_in_kwargs) - len_args
    if num_missing > 0:
        missing_names = [
            f"'{name}'" for name in required_positional_not_in_kwargs[len_args:]
        ]
        msg = (
            f"{method_name}() missing {num_missing} required positional "
            f"argument{'' if num_missing == 1 else 's'}: "
        )
        _raise_missing_names_type_error(missing_names, msg)
    args_names = []
    for counter, name in enumerate(positional_names):
        if counter + 1 > len_args:
            break
        if name in kwargs_keys:
            msg = f"{method_name}() got multiple values for argument '{name}'"
            raise TypeError(msg)
        args_names.append(name)
    missing_keyword_only_arguments = [
        name for name in required_keyword_only if name not in kwargs_keys
    ]
    if missing_keyword_only_arguments:
        missing_names = [f"'{name}'" for name in missing_keyword_only_arguments]
        msg = (
            f"{method_name}() missing {len(missing_names)} "
            "required keyword-only argument"
            f"{'' if len(missing_names) == 1 else 's'}: "
        )
        _raise_missing_names_type_error(missing_names, msg)
    for key in tuple(keyword_defaults.keys()):
        if key in args_names or key in kwargs_keys:
            keyword_defaults.pop(key)
    enumerated_args_names = tuple(enumerate(args_names))
    keyword_defaults_items = tuple(keyword_defaults.items())
    return enumerated_args_names, keyword_defaults_items


def _raise_missing_names_type_error(missing_names: list[str], msg: str) -> None:
    msg += missing_names[0]
    if len(missing_names) == 2:
        msg += f" and {missing_names[1]}"
    elif len(missing_names) > 2:
        msg += ", " + ", ".join(missing_names[1:-1])
        msg += f", and {missing_names[-1]}"
    raise TypeError(msg)


_annotations_mention_id: set[type[BaseAggregate]] = set()
_init_mentions_id: set[type[BaseAggregate]] = set()
_create_id_param_names: dict[type[BaseAggregate], list[str]] = defaultdict(list)


class MetaAggregate(EventsourcingType, Generic[TAggregate], type):
    """Metaclass for aggregate classes."""

    def _define_event_class(
        cls,
        name: str,
        bases: tuple[type[CanMutateAggregate], ...],
        apply_method: CallableType | None,
    ) -> type[CanMutateAggregate]:
        # Define annotations for the event class (specs the init method).
        annotations = {}
        if apply_method is not None:
            method_signature = inspect.signature(apply_method)
            supers = {
                s for b in bases for s in b.__mro__ if hasattr(s, "__annotations__")
            }
            super_annotations = {a for s in supers for a in s.__annotations__}
            for param_name, param in list(method_signature.parameters.items())[1:]:
                # Don't define 'id' on a "created" class.
                if param_name == "id" and apply_method.__name__ == "__init__":
                    continue
                # Don't override super class annotations, unless no default on param.
                if param_name not in super_annotations or param.default == param.empty:
                    annotations[param_name] = param.annotation or "typing.Any"
        event_cls_qualname = f"{cls.__qualname__}.{name}"
        event_cls_dict = {
            "__annotations__": annotations,
            "__module__": cls.__module__,
            "__qualname__": event_cls_qualname,
        }

        # Create the event class object.
        _new_class = type(name, bases, event_cls_dict)
        return cast("type[CanMutateAggregate]", _new_class)

    def __call__(
        cls: MetaAggregate[TAggregate], *args: Any, **kwargs: Any
    ) -> TAggregate:
        if cls is BaseAggregate:
            msg = "BaseAggregate class cannot be instantiated directly"
            raise TypeError(msg)
        created_event_classes = _created_event_classes[cls]
        # Here, unlike when calling _create(), we don't have a given event class,
        # so we need to check that there is one "created" event class to use here.
        # We don't check this in __init_subclass__ to allow for alternatives that
        # can be selected by developers by calling _create(event_class=...).
        if len(created_event_classes) > 1:
            msg = (
                f"{cls.__qualname__} can't decide which of many "
                '"created" event classes to use: '
                f"""'{"', '".join(c.__name__ for c in created_event_classes)}'. """
                "Please use class arg 'created_event_name' or"
                " @event decorator on __init__ method."
            )
            raise TypeError(msg)

        kwargs = _coerce_args_to_kwargs(
            cls.__init__,  # type: ignore[misc]
            args,
            kwargs,
            expects_id=cls in _annotations_mention_id,
        )
        return cls._create(
            event_class=created_event_classes[0],
            **kwargs,
        )

    def _create(
        cls: MetaAggregate[TAggregate],
        event_class: type[CanInitAggregate],
        **kwargs: Any,
    ) -> TAggregate:
        # Just define method signature for the __call__() method.
        raise NotImplementedError  # pragma: no cover


class BaseAggregate(metaclass=MetaAggregate):
    """Base class for aggregates."""

    INITIAL_VERSION = 1

    @staticmethod
    def create_id(*_: Any, **__: Any) -> UUID:
        """Returns a new aggregate ID."""
        return uuid4()

    @classmethod
    def _create(
        cls: type[Self],
        event_class: type[CanInitAggregate],
        *,
        id: UUID | None = None,  # noqa: A002
        **kwargs: Any,
    ) -> Self:
        """Constructs a new aggregate object instance."""
        # Construct the domain event with an ID and a
        # version, and a topic for the aggregate class.
        create_id_kwargs = {
            k: v for k, v in kwargs.items() if k in _create_id_param_names[cls]
        }
        if id is not None:
            originator_id = id
            if not isinstance(originator_id, UUID):
                msg = f"Given id was not a UUID: {originator_id}"
                raise TypeError(msg)
        else:
            originator_id = cls.create_id(**create_id_kwargs)
            if not isinstance(originator_id, UUID):
                msg = (
                    f"{cls.create_id.__module__}.{cls.create_id.__qualname__}"
                    f" did not return UUID, it returned: {originator_id}"
                )
                raise TypeError(msg)

        # Impose the required common "created" event attribute values.
        kwargs = kwargs.copy()
        kwargs.update(
            originator_topic=get_topic(cls),
            originator_id=originator_id,
            originator_version=cls.INITIAL_VERSION,
        )
        if kwargs.get("timestamp") is None:
            kwargs["timestamp"] = event_class.create_timestamp()

        try:
            created_event = event_class(**kwargs)
        except TypeError as e:
            msg = f"Unable to construct '{event_class.__qualname__}' event: {e}"
            raise TypeError(msg) from e
        # Construct the aggregate object.
        agg = cast("Self", created_event.mutate(None))

        assert agg is not None
        # Append the domain event to pending list.
        agg.pending_events.append(created_event)
        # Return the aggregate.
        return agg

    def __base_init__(
        self, originator_id: UUID, originator_version: int, timestamp: datetime
    ) -> None:
        """Initialises an aggregate object with an :data:`id`, a :data:`version`
        number, and a :data:`timestamp`.
        """
        self._id = originator_id
        self._version = originator_version
        self._created_on = timestamp
        self._modified_on = timestamp
        self._pending_events: list[CanMutateAggregate] = []

    @property
    def id(self) -> UUID:
        """The ID of the aggregate."""
        return self._id

    @property
    def version(self) -> int:
        """The version number of the aggregate."""
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    @property
    def created_on(self) -> datetime:
        """The date and time when the aggregate was created."""
        return self._created_on

    @property
    def modified_on(self) -> datetime:
        """The date and time when the aggregate was last modified."""
        return self._modified_on

    @modified_on.setter
    def modified_on(self, modified_on: datetime) -> None:
        self._modified_on = modified_on

    @property
    def pending_events(self) -> list[CanMutateAggregate]:
        """A list of pending events."""
        return self._pending_events

    def trigger_event(
        self,
        event_class: type[CanMutateAggregate],
        **kwargs: Any,
    ) -> None:
        """Triggers domain event of given type, by creating
        an event object and using it to mutate the aggregate.
        """
        # Construct the domain event as the
        # next in the aggregate's sequence.
        # Use counting to generate the sequence.
        next_version = self.version + 1

        # Impose the required common domain event attribute values.
        kwargs = kwargs.copy()
        kwargs.update(
            originator_id=self.id,
            originator_version=next_version,
        )
        if kwargs.get("timestamp") is None:
            kwargs["timestamp"] = event_class.create_timestamp()

        try:
            new_event = event_class(**kwargs)
        except TypeError as e:
            msg = f"Can't construct event {event_class}: {e}"
            raise TypeError(msg) from None

        # Mutate aggregate with domain event.
        new_event.mutate(self)
        # Append the domain event to pending list.
        self._pending_events.append(new_event)

    def collect_events(self) -> Sequence[CanMutateAggregate]:
        """Collects and returns a list of pending aggregate
        :class:`AggregateEvent` objects.
        """
        collected = []
        while self._pending_events:
            collected.append(self._pending_events.pop(0))
        return collected

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        attrs = [
            f"{k.lstrip('_')}={v!r}"
            for k, v in self.__dict__.items()
            if k != "_pending_events"
        ]
        return f"{type(self).__name__}({', '.join(attrs)})"

    def __init_subclass__(
        cls: type[BaseAggregate], *, created_event_name: str = ""
    ) -> None:
        """
        Initialises aggregate subclass by defining __init__ method and event classes.
        """
        super().__init_subclass__()

        # Ensure we aren't defining another instance of the same class,
        # because annotations can get confused when using singledispatchmethod
        # during class definition e.g. on an aggregate projector function.
        _module = importlib.import_module(cls.__module__)
        if cls.__name__ in _module.__dict__:
            msg = f"Name '{cls.__name__}' already defined in '{cls.__module__}' module"
            raise ProgrammingError(msg)

        # Get the class annotations.
        class_annotations = cls.__dict__.get("__annotations__", {})
        try:
            class_annotations.pop("id")
            _annotations_mention_id.add(cls)
        except KeyError:
            pass

        if "id" in cls.__dict__:
            msg = f"Setting attribute 'id' on class '{cls.__name__}' is not allowed"
            raise ProgrammingError(msg)

        # Process the class as a dataclass, if there are annotations.
        if (
            class_annotations
            or cls in _annotations_mention_id
            or any(dataclasses.is_dataclass(base) for base in cls.__bases__)
        ):
            dataclasses.dataclass(eq=False, repr=False)(cls)

        # Remember if __init__ mentions ID.
        for param_name in inspect.signature(cls.__init__).parameters:
            if param_name == "id":
                _init_mentions_id.add(cls)
                break

        # Analyse __init__ attribute, to get __init__ method and @event decorator.
        init_attr: FunctionType | CommandMethodDecorator | None = cls.__dict__.get(
            "__init__"
        )
        if init_attr is None:
            # No method, no decorator.
            init_method: CallableType | None = None
            init_decorator: CommandMethodDecorator | None = None
        elif isinstance(init_attr, CommandMethodDecorator):
            # Method decorated with @event.
            init_method = init_attr.decorated_func
            init_decorator = init_attr
        else:
            # Undecorated __init__ method.
            init_decorator = None
            init_method = init_attr

        # Identify or define a base event class for this aggregate.
        base_event_name = "Event"
        base_event_cls: type[CanMutateAggregate]
        try:
            base_event_cls = cls.__dict__[base_event_name]
        except KeyError:
            base_event_cls = cls._define_event_class(
                name=base_event_name,
                bases=(getattr(cls, base_event_name, AggregateEvent),),
                apply_method=None,
            )
            setattr(cls, base_event_name, base_event_cls)

        # Ensure all events defined on this class are subclasses of base event class.
        created_event_classes: dict[str, type[CanInitAggregate]] = {}
        for name, value in tuple(cls.__dict__.items()):
            if name == base_event_name:
                # Don't subclass the base event class again.
                continue
            if name.lower() == name:
                # Don't subclass lowercase named attributes.
                continue
            if isinstance(value, type) and issubclass(value, CanMutateAggregate):
                if not issubclass(value, base_event_cls):
                    event_class = cls._define_event_class(
                        name, (value, base_event_cls), None
                    )
                    setattr(cls, name, event_class)
                else:
                    event_class = value

                # Remember all "created" event classes defined on this class.
                if issubclass(event_class, CanInitAggregate):
                    created_event_classes[name] = event_class

        # Identify or define the aggregate's "created" event class.
        created_event_class: type[CanInitAggregate] | None = None

        # Analyse __init__ method decorator.
        if init_decorator:

            # Does the decorator specify an event class?
            if init_decorator.given_event_cls:

                # Disallow conflicts between 'created_event_name' and given class.
                if (
                    created_event_name
                    and created_event_name != init_decorator.given_event_cls.__name__
                ):
                    msg = (
                        "Given 'created_event_name' conflicts "
                        "with decorator on __init__"
                    )
                    raise TypeError(msg)

                # Check given event class can init aggregate.
                if not issubclass(init_decorator.given_event_cls, CanInitAggregate):
                    msg = (
                        f"class '{init_decorator.given_event_cls.__name__}' "
                        f'not a "created" event class'
                    )
                    raise TypeError(msg)

                # Have we already subclassed the given event class?
                for sub_class in created_event_classes.values():
                    if issubclass(sub_class, init_decorator.given_event_cls):
                        created_event_class = sub_class
                        break
                else:
                    created_event_class = init_decorator.given_event_cls

            # Does the decorator specify an event name?
            elif init_decorator.event_cls_name:
                # Disallow conflicts between 'created_event_name' and given name.
                if (
                    created_event_name
                    and created_event_name != init_decorator.event_cls_name
                ):
                    msg = (
                        "Given 'created_event_name' conflicts "
                        "with decorator on __init__"
                    )
                    raise TypeError(msg)

                created_event_name = init_decorator.event_cls_name

            # Disallow using decorator on __init__ without event name or class.
            else:
                msg = "@event decorator on __init__ has neither event name nor class"
                raise TypeError(msg)

        # Do we need to define a created event class?
        if not created_event_class:
            # If we have a "created" event class that matches the name, then use it.
            if created_event_name in created_event_classes:
                created_event_class = created_event_classes[created_event_name]
            # Otherwise, if we have no name and only one class defined, then use it.
            elif not created_event_name and len(created_event_classes) == 1:
                created_event_class = next(iter(created_event_classes.values()))

            # Otherwise, if there are no "created" events, or a name is
            # specified that hasn't matched, then define a "created" event class.
            elif len(created_event_classes) == 0 or created_event_name:
                # Decide the base "created" event class.

                try:
                    # Look for a base class with the same name.
                    base_created_event_cls = cast(
                        "type[CanInitAggregate]",
                        getattr(cls, created_event_name),
                    )
                except AttributeError:
                    # Look for base class with one nominated "created" event.
                    for base_cls in cls.__mro__:
                        if (
                            base_cls in _created_event_classes
                            and len(_created_event_classes[base_cls]) == 1
                        ):
                            base_created_event_cls = _created_event_classes[base_cls][0]
                            break
                    else:
                        msg = (
                            "Can't identify suitable base class for "
                            f"\"created\" event class on class '{cls.__name__}'"
                        )
                        raise TypeError(msg) from None

                if not created_event_name:
                    created_event_name = base_created_event_cls.__name__

                # Disallow init method from having variable params, because
                # we are using it to define a "created" event class.
                if init_method:
                    _raise_type_error_if_func_has_variable_params(init_method)

                # Don't subclass from base event class twice.
                assert isinstance(base_created_event_cls, type), base_created_event_cls
                assert not issubclass(
                    base_created_event_cls, base_event_cls
                ), base_created_event_cls

                # Define "created" event class.
                assert created_event_name
                assert issubclass(base_created_event_cls, CanInitAggregate)
                created_event_class_bases = (base_created_event_cls, base_event_cls)
                created_event_class = cast(
                    "type[CanInitAggregate]",
                    cls._define_event_class(
                        created_event_name,
                        created_event_class_bases,
                        init_method,
                    ),
                )
                # Set the event class as an attribute of the aggregate class.
                setattr(cls, created_event_name, created_event_class)

        assert created_event_class or len(created_event_classes) > 1

        if created_event_class:
            _created_event_classes[cls] = [created_event_class]
        else:
            # Prepare to disallow ambiguity of choice between created event classes.
            _created_event_classes[cls] = list(created_event_classes.values())

        # Find and analyse any @event decorators.
        for attr_name, attr_value in tuple(cls.__dict__.items()):
            event_decorator: CommandMethodDecorator | None = None

            # Ignore a decorator on the __init__ method.
            if isinstance(attr_value, CommandMethodDecorator) and (
                attr_value.decorated_func.__name__ == "__init__"
            ):
                continue

            # Handle @property.setter decorator on top of @event decorator.
            if isinstance(attr_value, property) and isinstance(
                attr_value.fset, CommandMethodDecorator
            ):
                event_decorator = attr_value.fset
                # Inspect the setter method.
                method_signature = inspect.signature(event_decorator.decorated_func)
                assert len(method_signature.parameters) == 2
                event_decorator.is_property_setter = True
                event_decorator.property_setter_arg_name = list(
                    method_signature.parameters
                )[1]
                if event_decorator.decorated_func.__name__ != attr_name:
                    attr = cls.__dict__[event_decorator.decorated_func.__name__]
                    if isinstance(attr, CommandMethodDecorator):
                        # This is the "x = property(getx, setx) form" where setx
                        # is a decorated method.
                        continue
                        # Otherwise, it's "x = property(getx, event(setx))".
                elif event_decorator.is_name_inferred_from_method:
                    # This is the "@property.setter \ @event" form. We don't want
                    # event class name inferred from property (not past participle).
                    method_name = event_decorator.decorated_func.__name__
                    msg = (
                        f"@event decorator under @{method_name}.setter "
                        "requires event name or class"
                    )
                    raise TypeError(msg)

            elif isinstance(attr_value, CommandMethodDecorator):
                event_decorator = attr_value

            if event_decorator is not None:
                if event_decorator.given_event_cls:
                    # Check this is not a "created" event class.
                    if issubclass(event_decorator.given_event_cls, CanInitAggregate):
                        msg = (
                            f"{event_decorator.given_event_cls} "
                            f"is subclass of {CanInitAggregate.__name__}"
                        )
                        raise TypeError(msg)

                    # Define event class as subclass of given class.
                    given_subclass = cast(
                        "type[CanMutateAggregate]",
                        getattr(cls, event_decorator.given_event_cls.__name__),
                    )
                    event_cls = cls._define_event_class(
                        event_decorator.given_event_cls.__name__,
                        (DecoratorEvent, given_subclass),
                        None,
                    )

                else:
                    # Check event class isn't already defined.
                    assert event_decorator.event_cls_name
                    if event_decorator.event_cls_name in cls.__dict__:
                        msg = (
                            f"{event_decorator.event_cls_name} "
                            f"event already defined on {cls.__name__}"
                        )
                        raise TypeError(msg)

                    # Define event class from signature of original method.
                    event_cls = cls._define_event_class(
                        event_decorator.event_cls_name,
                        (DecoratorEvent, base_event_cls),
                        event_decorator.decorated_func,
                    )

                # Cache the decorated method for the event class to use.
                _decorated_funcs[event_cls] = event_decorator.decorated_func

                # Set the event class as an attribute of the aggregate class.
                setattr(cls, event_cls.__name__, event_cls)

                # Remember which event class to trigger.
                decorator_event_classes[event_decorator] = cast(
                    "type[DecoratorEvent]", event_cls
                )

        # Check any create_id() method defined on this class is static or class method.
        if "create_id" in cls.__dict__ and not isinstance(
            cls.__dict__["create_id"], (staticmethod, classmethod)
        ):
            msg = (
                f"{cls.create_id} is not a static or class method: "
                f"{type(cls.create_id)}"
            )
            raise TypeError(msg)

        # Get the parameters of the create_id method that will be used by this class.
        for name, param in inspect.signature(cls.create_id).parameters.items():
            if param.kind in [param.KEYWORD_ONLY, param.POSITIONAL_OR_KEYWORD]:
                _create_id_param_names[cls].append(name)

        # Define event classes for all events on all bases if not defined on this class.
        for aggregate_base_class in cls.__bases__:
            for name, value in aggregate_base_class.__dict__.items():
                if (
                    isinstance(value, type)
                    and issubclass(value, AggregateEvent)
                    and name not in cls.__dict__
                    and name.lower() != name
                ):
                    event_class = cls._define_event_class(
                        name, (base_event_cls, value), None
                    )
                    setattr(cls, name, event_class)


class Aggregate(BaseAggregate):
    class Event(AggregateEvent):
        pass

    class Created(Event, AggregateCreated):
        pass


@overload
def aggregate(*, created_event_name: str) -> Callable[[Any], type[Aggregate]]:
    pass  # pragma: no cover


@overload
def aggregate(cls: Any) -> type[Aggregate]:
    pass  # pragma: no cover


def aggregate(
    cls: Any | None = None,
    *,
    created_event_name: str = "",
) -> type[Aggregate] | Callable[[Any], type[Aggregate]]:
    """Converts the class that was passed in to inherit from Aggregate.

    .. code-block:: python

        @aggregate
        class MyAggregate:
            pass

    ...is equivalent to...

    .. code-block:: python

        class MyAggregate(Aggregate):
            pass
    """

    def decorator(cls_: Any) -> type[Aggregate]:
        if issubclass(cls_, Aggregate):
            msg = f"{cls_.__qualname__} is already an Aggregate"
            raise TypeError(msg)
        bases = cls_.__bases__
        if bases == (object,):
            bases = (Aggregate,)
        else:
            bases += (Aggregate,)
        cls_dict = {}
        cls_dict.update(cls_.__dict__)
        cls_ = MetaAggregate(
            cls_.__qualname__,
            bases,
            cls_dict,
            created_event_name=created_event_name,
        )
        assert issubclass(cls_, Aggregate)
        return cls_

    if cls:
        return decorator(cls)
    return decorator


class OriginatorIDError(EventSourcingError):
    """Raised when a domain event can't be applied to
    an aggregate due to an ID mismatch indicating
    the domain event is not in the aggregate's
    sequence of events.
    """


class OriginatorVersionError(EventSourcingError):
    """Raised when a domain event can't be applied to
    an aggregate due to version mismatch indicating
    the domain event is not the next in the aggregate's
    sequence of events.
    """


class SnapshotProtocol(DomainEventProtocol, Protocol):
    @property
    def state(self) -> dict[str, Any]:
        """Snapshots have a read-only 'state'."""
        raise NotImplementedError  # pragma: no cover

    # TODO: Improve on this 'Any'.
    @classmethod
    def take(cls: Any, aggregate: Any) -> Any:
        """Snapshots have a 'take()' class method."""


TCanSnapshotAggregate = TypeVar("TCanSnapshotAggregate", bound="CanSnapshotAggregate")


class CanSnapshotAggregate(HasOriginatorIDVersion, CanCreateTimestamp):
    topic: str
    state: Any

    def __init__(
        self,
        originator_id: UUID,
        originator_version: int,
        timestamp: datetime,
        topic: str,
        state: Any,
    ) -> None:
        raise NotImplementedError  # pragma: no cover

    @classmethod
    def take(
        cls,
        aggregate: MutableOrImmutableAggregate,
    ) -> Self:
        """Creates a snapshot of the given :class:`Aggregate` object."""
        aggregate_state = dict(aggregate.__dict__)
        class_version = getattr(type(aggregate), "class_version", 1)
        if class_version > 1:
            aggregate_state["class_version"] = class_version
        if isinstance(aggregate, Aggregate):
            aggregate_state.pop("_id")
            aggregate_state.pop("_version")
            aggregate_state.pop("_pending_events")
        return cls(
            originator_id=aggregate.id,
            originator_version=aggregate.version,
            timestamp=cls.create_timestamp(),
            topic=get_topic(type(aggregate)),
            state=aggregate_state,
        )

    def mutate(self, _: None) -> Aggregate:
        """Reconstructs the snapshotted :class:`Aggregate` object."""
        cls = cast("type[Aggregate]", resolve_topic(self.topic))
        aggregate_state = dict(self.state)
        from_version = aggregate_state.pop("class_version", 1)
        class_version = getattr(cls, "class_version", 1)
        while from_version < class_version:
            upcast_name = f"upcast_v{from_version}_v{from_version + 1}"
            upcast = getattr(cls, upcast_name)
            upcast(aggregate_state)
            from_version += 1

        aggregate_state["_id"] = self.originator_id
        aggregate_state["_version"] = self.originator_version
        aggregate_state["_pending_events"] = []
        aggregate = object.__new__(cls)
        aggregate.__dict__.update(aggregate_state)
        return aggregate


@dataclass(frozen=True)
class Snapshot(CanSnapshotAggregate, DomainEvent):
    """Snapshots represent the state of an aggregate at a particular
    version.

    Constructor arguments:

    :param UUID originator_id: ID of originating aggregate.
    :param int originator_version: version of originating aggregate.
    :param datetime timestamp: date-time of the event
    :param str topic: string that includes a class and its module
    :param dict state: version of originating aggregate.
    """

    topic: str
    state: dict[str, Any]
