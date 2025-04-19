from __future__ import annotations

import contextlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from unittest import TestCase

from eventsourcing.application import Application
from eventsourcing.domain import (
    Aggregate,
    AggregateCreated,
    AggregateEvent,
    aggregate,
    event,
    triggers,
)
from eventsourcing.utils import get_method_name


class TestAggregateDecorator(TestCase):
    def test_decorate_class_with_no_bases(self) -> None:
        @aggregate
        class MyAgg:
            """My doc"""

            a: int

        self.assertTrue(issubclass(MyAgg, Aggregate))
        self.assertTrue(issubclass(MyAgg, MyAgg))
        self.assertTrue(MyAgg.__name__, "MyAgg")
        self.assertTrue(MyAgg.__doc__, "My doc")
        self.assertEqual(MyAgg.__bases__, (Aggregate,))
        self.assertEqual(MyAgg.__annotations__, {"a": "int"})

        agg = MyAgg(a=1)  # type: ignore[call-arg]
        self.assertEqual(agg.a, 1)
        self.assertEqual(len(agg.pending_events), 1)  # type: ignore[attr-defined]
        self.assertIsInstance(agg, Aggregate)
        self.assertIsInstance(agg, MyAgg)

    def test_decorate_class_with_one_base(self) -> None:
        class MyBase:
            "My base doc"

        @aggregate
        class MyAgg(MyBase):
            """My doc"""

            a: int

        self.assertTrue(issubclass(MyAgg, Aggregate))
        self.assertTrue(issubclass(MyAgg, MyAgg))
        self.assertTrue(issubclass(MyAgg, MyBase))
        self.assertTrue(MyAgg.__name__, "MyAgg")
        self.assertTrue(MyAgg.__doc__, "My doc")
        self.assertEqual(MyAgg.__bases__, (MyBase, Aggregate))
        self.assertEqual(MyAgg.__annotations__, {"a": "int"})

        agg = MyAgg(a=1)  # type: ignore[call-arg]
        self.assertEqual(agg.a, 1)
        self.assertEqual(len(agg.pending_events), 1)  # type: ignore[attr-defined]
        self.assertIsInstance(agg, Aggregate)
        self.assertIsInstance(agg, MyAgg)
        self.assertIsInstance(agg, MyBase)

    def test_decorate_class_with_two_bases(self) -> None:
        class MyAbstract:
            "My base doc"

        class MyBase(MyAbstract):
            "My base doc"

        @aggregate
        class MyAgg(MyBase):
            """My doc"""

            a: int

        self.assertTrue(issubclass(MyAgg, Aggregate))
        self.assertTrue(issubclass(MyAgg, MyAgg))
        self.assertTrue(issubclass(MyAgg, MyBase))
        self.assertTrue(issubclass(MyAgg, MyAbstract))
        self.assertTrue(MyAgg.__name__, "MyAgg")
        self.assertTrue(MyAgg.__doc__, "My doc")
        self.assertEqual(MyAgg.__bases__, (MyBase, Aggregate))
        self.assertEqual(MyAgg.__annotations__, {"a": "int"})

        agg = MyAgg(a=1)  # type: ignore[call-arg]
        self.assertEqual(agg.a, 1)
        self.assertEqual(len(agg.pending_events), 1)  # type: ignore[attr-defined]
        self.assertIsInstance(agg, Aggregate)
        self.assertIsInstance(agg, MyAgg)
        self.assertIsInstance(agg, MyBase)
        self.assertIsInstance(agg, MyAbstract)

    def test_raises_when_decorating_aggregate_subclass(self) -> None:
        with self.assertRaises(TypeError) as cm:

            @aggregate
            class MyAgg(Aggregate):
                pass

        self.assertIn("MyAgg is already an Aggregate", cm.exception.args[0])

    def test_aggregate_on_dataclass(self) -> None:
        @aggregate
        @dataclass
        class MyAgg:
            value: int

        a = MyAgg(1)
        self.assertIsInstance(a, MyAgg)
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 1)  # type: ignore[attr-defined]

    def test_dataclass_on_aggregate(self) -> None:
        @dataclass
        @aggregate
        class MyAgg:
            value: int

        a = MyAgg(1)
        self.assertIsInstance(a, MyAgg)
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 1)  # type: ignore[attr-defined]

    def test_aggregate_decorator_called_with_create_event_name(self) -> None:
        @aggregate(created_event_name="Started")
        class MyAgg:
            value: int

        a = MyAgg(1)  # type: ignore[call-arg]
        self.assertIsInstance(a, MyAgg)
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 1)  # type: ignore[attr-defined]
        self.assertEqual(type(a.pending_events[0]).__name__, "Started")  # type: ignore[attr-defined]


class TestEventDecorator(TestCase):
    def test_event_name_inferred_from_method_no_args(self) -> None:
        class MyAgg(Aggregate):
            @event
            def heartbeat(self) -> None:
                pass

        a = MyAgg()
        self.assertIsInstance(a, MyAgg)
        a.heartbeat()
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(a.version, 2)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.Heartbeat)  # type: ignore[attr-defined]

    def test_event_decorator_called_without_args(self) -> None:
        class MyAgg(Aggregate):
            @event()
            def heartbeat(self) -> None:
                pass

        a = MyAgg()
        self.assertIsInstance(a, MyAgg)
        a.heartbeat()
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(a.version, 2)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.Heartbeat)  # type: ignore[attr-defined]

    def test_event_name_inferred_from_method_with_arg(self) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, value: int) -> None:
                self.value = value

        a = MyAgg()
        self.assertIsInstance(a, MyAgg)
        a.value_changed(1)
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(a.version, 2)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_event_name_inferred_from_method_with_kwarg(self) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, value: int) -> None:
                self.value = value

        a = MyAgg()
        self.assertIsInstance(a, MyAgg)
        a.value_changed(value=1)
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_event_name_inferred_from_method_with_default_kwarg(self) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, value: int = 3) -> None:
                self.value = value

        a = MyAgg()
        self.assertIsInstance(a, MyAgg)
        a.value_changed()
        self.assertEqual(a.value, 3)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_method_name_same_on_class_and_instance(self) -> None:
        # Check this works with Python object class.
        class MyClass:
            def value_changed(self) -> None:
                pass

        a = MyClass()

        self.assertEqual(
            get_method_name(a.value_changed), get_method_name(MyClass.value_changed)
        )

        # Check this works with Aggregate class and @event decorator.
        class MyAggregate(Aggregate):
            @event
            def value_changed(self) -> None:
                pass

        a1 = MyAggregate()

        self.assertEqual(
            get_method_name(a1.value_changed),
            get_method_name(MyAggregate.value_changed),
        )

        self.assertTrue(
            get_method_name(a1.value_changed).endswith("value_changed"),
        )

        self.assertTrue(
            get_method_name(MyAggregate.value_changed).endswith("value_changed"),
        )

        # Check this works with Aggregate class and @event decorator.
        class MyAggregate2(Aggregate):
            @event()
            def value_changed(self) -> None:
                pass

        a2 = MyAggregate2()

        self.assertEqual(
            get_method_name(a2.value_changed),
            get_method_name(MyAggregate2.value_changed),
        )

        self.assertTrue(
            get_method_name(a2.value_changed).endswith("value_changed"),
        )

        self.assertTrue(
            get_method_name(MyAggregate2.value_changed).endswith("value_changed"),
        )

    def test_raises_when_method_takes_1_positional_argument_but_2_were_given(
        self,
    ) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self) -> None:
                pass

        class Data:
            def value_changed(self) -> None:
                pass

        def assert_raises(cls: type[MyAgg | Data]) -> None:
            obj = cls()
            with self.assertRaises(TypeError) as cm:
                obj.value_changed(1)  # type: ignore[call-arg]

            name = get_method_name(cls.value_changed)

            self.assertEqual(
                f"{name}() takes 1 positional argument but 2 were given",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_method_takes_2_positional_argument_but_3_were_given(
        self,
    ) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, value: int) -> None:
                pass

        class Data:
            def value_changed(self, value: int) -> None:
                pass

        def assert_raises(cls: type[MyAgg | Data]) -> None:
            obj = cls()
            with self.assertRaises(TypeError) as cm:
                obj.value_changed(1, 2)  # type: ignore[call-arg]
            name = get_method_name(cls.value_changed)
            self.assertEqual(
                f"{name}() takes 2 positional arguments but 3 were given",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_method_missing_1_required_positional_argument(self) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, a: int) -> None:
                pass

        class Data:
            def value_changed(self, a: int) -> None:
                pass

        def assert_raises(cls: type[MyAgg | Data]) -> None:
            obj = cls()
            with self.assertRaises(TypeError) as cm:
                obj.value_changed()  # type: ignore[call-arg]
            name = get_method_name(cls.value_changed)
            self.assertEqual(
                f"{name}() missing 1 required positional argument: 'a'",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_method_missing_2_required_positional_arguments(self) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, a: int, b: int) -> None:
                pass

        class Data:
            def value_changed(self, a: int, b: int) -> None:
                pass

        def assert_raises(cls: type[MyAgg | Data]) -> None:
            obj = cls()
            with self.assertRaises(TypeError) as cm:
                obj.value_changed()  # type: ignore[call-arg]
            name = get_method_name(obj.value_changed)
            self.assertEqual(
                f"{name}() missing 2 required positional arguments: 'a' and 'b'",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_method_missing_3_required_positional_arguments(self) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, a: int, b: int, c: int) -> None:
                pass

        class Data:
            def value_changed(self, a: int, b: int, c: int) -> None:
                pass

        def assert_raises(cls: type[MyAgg | Data]) -> None:
            obj = cls()
            with self.assertRaises(TypeError) as cm:
                obj.value_changed()  # type: ignore[call-arg]

            name = get_method_name(cls.value_changed)

            self.assertEqual(
                f"{name}() missing 3 required positional arguments: 'a', 'b', and 'c'",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_method_missing_1_required_keyword_only_argument(self) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, a: int, *, b: int) -> None:
                pass

        class Data:
            def value_changed(self, a: int, *, b: int) -> None:
                pass

        def assert_raises(cls: type[MyAgg | Data]) -> None:
            obj = cls()

            with self.assertRaises(TypeError) as cm:
                obj.value_changed(1)  # type: ignore[call-arg]

            name = get_method_name(cls.value_changed)
            self.assertEqual(
                f"{name}() missing 1 required keyword-only argument: 'b'",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_method_missing_2_required_keyword_only_arguments(self) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, a: int, *, b: int, c: int) -> None:
                pass

        class Data:
            def value_changed(self, a: int, *, b: int, c: int) -> None:
                pass

        def assert_raises(cls: type[MyAgg | Data]) -> None:
            obj = cls()

            with self.assertRaises(TypeError) as cm:
                obj.value_changed(1)  # type: ignore[call-arg]

            name = get_method_name(cls.value_changed)
            self.assertEqual(
                f"{name}() missing 2 required keyword-only arguments: 'b' and 'c'",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_method_missing_3_required_keyword_only_arguments(self) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, a: int, *, b: int, c: int, d: int) -> None:
                pass

        class Data:
            def value_changed(self, a: int, *, b: int, c: int, d: int) -> None:
                pass

        def assert_raises(cls: type[MyAgg | Data]) -> None:
            obj = cls()

            with self.assertRaises(TypeError) as cm:
                obj.value_changed(1)  # type: ignore[call-arg]

            name = get_method_name(cls.value_changed)
            self.assertEqual(
                f"{name}() missing 3 required keyword-only arguments: "
                "'b', 'c', and 'd'",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_missing_positional_and_required_keyword_only_arguments(
        self,
    ) -> None:
        class MyAgg(Aggregate):
            @event
            def value_changed(self, a: int, *, b: int, c: int, d: int) -> None:
                pass

        class Data:
            def value_changed(self, a: int, *, b: int, c: int, d: int) -> None:
                pass

        def assert_raises(cls: type[MyAgg | Data]) -> None:
            obj = cls()

            with self.assertRaises(TypeError) as cm:
                obj.value_changed()  # type: ignore[call-arg]

            name = get_method_name(cls.value_changed)
            self.assertEqual(
                f"{name}() missing 1 required positional argument: 'a'",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_method_gets_unexpected_keyword_argument(self) -> None:
        class Data:
            def value_changed(self, a: int) -> None:
                pass

        class MyAgg(Aggregate):
            @event
            def value_changed(self, a: int) -> None:
                pass

        def assert_raises(cls: type[Data | MyAgg]) -> None:
            obj = cls()

            with self.assertRaises(TypeError) as cm:
                obj.value_changed(b=1)  # type: ignore[call-arg]

            name = get_method_name(cls.value_changed)
            self.assertEqual(
                f"{name}() got an unexpected keyword argument 'b'",
                cm.exception.args[0],
            )

        assert_raises(MyAgg)
        assert_raises(Data)

    def test_raises_when_method_is_staticmethod(self) -> None:
        with self.assertRaises(TypeError) as cm:

            class _(Aggregate):  # noqa: N801
                @event
                @staticmethod
                def value_changed() -> None:
                    pass

        self.assertIn(
            "is not a str, function, property, or subclass of CanMutateAggregate",
            cm.exception.args[0],
        )

        with self.assertRaises(TypeError) as cm:

            class MyAgg(Aggregate):
                @event("ValueChanged")
                @staticmethod
                def value_changed() -> None:
                    pass

        self.assertTrue(
            cm.exception.args[0].endswith(
                " is not a function or property",
            ),
            cm.exception.args[0],
        )

    def test_raises_when_method_is_classmethod(self) -> None:
        with self.assertRaises(TypeError) as cm:

            class _(Aggregate):  # noqa: N801
                @event
                @classmethod
                def value_changed(cls) -> None:
                    pass

        self.assertIn(
            "is not a str, function, property, or subclass of CanMutateAggregate",
            cm.exception.args[0],
        )

        with self.assertRaises(TypeError) as cm:

            class MyAgg(Aggregate):
                @event("ValueChanged")
                @classmethod
                def value_changed(cls) -> None:
                    pass

        self.assertTrue(
            cm.exception.args[0].endswith(
                " is not a function or property",
            ),
            cm.exception.args[0],
        )

    def test_method_called_with_positional_defined_with_keyword_params(self) -> None:
        class MyAgg(Aggregate):
            @event
            def values_changed(
                self, a: int | None = None, b: int | None = None
            ) -> None:
                self.a = a
                self.b = b

        a = MyAgg()
        a.values_changed(1, 2)

    def test_method_called_with_keyword_defined_with_positional_params(self) -> None:
        class MyAgg(Aggregate):
            @event
            def values_changed(self, a: int, b: int) -> None:
                self.a = a
                self.b = b

        a = MyAgg()
        a.values_changed(a=1, b=2)

    # @skipIf(sys.version_info[0:2] < (3, 8), "Positional only params not supported")
    # def test_method_called_with_keyword_defined_with_positional_only(self) -> None:
    #     @aggregate
    #     class MyAgg:
    #         @event
    #         def values_changed(self, a, b, /):
    #             self.a = a
    #             self.b = b
    #
    #     a = MyAgg()
    #     a.values_changed(1, 2)

    # def test_raises_when_method_has_positional_only_params(self) -> None:
    #     @aggregate
    #     class MyAgg:
    #         @event
    #         def values_changed(self, a, b, /):
    #             self.a = a
    #             self.b = b
    #
    #     with self.assertRaises(TypeError) as cm:
    #
    #         a = MyAgg()
    #         a.values_changed(1, 2)
    #
    #     self.assertTrue(
    #         cm.exception.args[0].startswith(
    #             # "values_changed() got some positional-only arguments"
    #             "Can't construct event"
    #         ),
    #         cm.exception.args[0],
    #     )

    def test_raises_when_decorated_method_called_directly_without_instance_arg(
        self,
    ) -> None:
        class MyAgg(Aggregate):
            @event
            def method(self) -> None:
                pass

        with self.assertRaises(TypeError) as cm:
            MyAgg.method()  # type: ignore[call-arg]
        self.assertEqual(
            cm.exception.args[0],
            "Expected aggregate as first argument",
        )

    def test_decorated_method_called_directly_on_class(self) -> None:
        class MyAgg(Aggregate):
            @event
            def method(self) -> None:
                pass

        a = MyAgg()
        self.assertEqual(a.version, 1)
        MyAgg.method(a)
        self.assertEqual(a.version, 2)

    def test_event_name_set_in_decorator(self) -> None:
        class MyAgg(Aggregate):
            @event("ValueChanged")
            def set_value(self, value: int) -> None:
                self.value = value

        a = MyAgg()
        a.set_value(value=1)
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_event_called_to_redefine_method_with_explicit_name(self) -> None:
        class MyAgg(Aggregate):
            def set_value(self, value: int) -> None:
                self.value = value

            set_value = event("ValueChanged")(set_value)

        a = MyAgg()
        a.set_value(value=1)
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_event_called_to_redefine_method_with_implied_name(self) -> None:
        class MyAgg(Aggregate):
            def value_changed(self, value: int) -> None:
                self.value = value

            set_value = event(value_changed)

        a = MyAgg()
        a.set_value(value=1)
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_event_name_set_in_decorator_cannot_be_empty_string(self) -> None:
        with self.assertRaises(ValueError) as cm:

            class MyAgg(Aggregate):
                @event("")
                def set_value(self, value: int) -> None:
                    self.value = value

        self.assertEqual(
            cm.exception.args[0], "Can't use empty string as name of event class"
        )

    def test_event_with_name_decorates_property(self) -> None:
        class MyAgg(Aggregate):
            def __init__(self, value: int) -> None:
                self._value = value

            @property
            def value(self) -> int:
                return self._value

            @event("ValueChanged")  # type: ignore[misc]
            @value.setter
            def value(self, x: int) -> None:
                self._value = x

        a = MyAgg(0)
        self.assertEqual(a.value, 0)
        a.value = 1  # type: ignore[misc]
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_property_decorates_event_with_name(self) -> None:
        class MyAgg(Aggregate):
            @property
            def value(self) -> int:
                return self._value

            @value.setter
            @event("ValueChanged")
            def value(self, x: int) -> None:
                self._value = x

        a = MyAgg()
        a.value = 1
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_property_called_with_decorated_set_method_with_name_given(self) -> None:
        class MyAgg(Aggregate):
            def get_value(self) -> int:
                return self._value

            @event("ValueChanged")
            def set_value(self, x: int) -> None:
                self._value = x

            value = property(get_value, set_value)

        a = MyAgg()
        a.value = 1
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_property_called_with_decorated_set_method_with_name_inferred(self) -> None:
        class MyAgg(Aggregate):
            def get_value(self) -> int:
                return self._value

            @event
            def value_changed(self, x: int) -> None:
                self._value = x

            value = property(get_value, value_changed)

        a = MyAgg()
        a.value = 1
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_property_called_with_wrapped_set_method_with_name_given(self) -> None:
        class MyAgg(Aggregate):
            def get_value(self) -> int:
                return self._value

            def set_value(self, x: int) -> None:
                self._value = x

            value = property(get_value, event("ValueChanged")(set_value))

        a = MyAgg()
        a.value = 1
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_property_called_with_wrapped_set_method_with_name_inferred(self) -> None:
        class MyAgg(Aggregate):
            def get_value(self) -> int:
                return self._value

            def value_changed(self, x: int) -> None:
                self._value = x

            value = property(get_value, event(value_changed))

        a = MyAgg()
        a.value = 1
        self.assertEqual(a.value, 1)
        self.assertIsInstance(a, Aggregate)
        self.assertEqual(len(a.pending_events), 2)
        self.assertIsInstance(a.pending_events[1], MyAgg.ValueChanged)  # type: ignore[attr-defined]

    def test_raises_when_event_decorates_property_getter(self) -> None:
        with self.assertRaises(TypeError) as cm:

            class MyAgg(Aggregate):
                @event("ValueChanged")  # type: ignore[prop-decorator]
                @property
                def value(self) -> None:
                    return None

        self.assertEqual(
            cm.exception.args[0], "@event can't decorate value() property getter"
        )

        with self.assertRaises(TypeError) as cm:

            @aggregate
            class _:  # noqa: N801
                @event("ValueChanged")  # type: ignore[prop-decorator]
                @property
                def value(self) -> None:
                    return None

        self.assertEqual(
            cm.exception.args[0], "@event can't decorate value() property getter"
        )

    def test_raises_when_event_without_name_decorates_property(self) -> None:
        with self.assertRaises(TypeError) as cm:

            class MyAgg(Aggregate):
                def __init__(self, _: Any) -> None:
                    pass

                @property
                def value(self) -> None:
                    return None

                @event  # type: ignore[misc]
                @value.setter
                def value(self, x: int) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0],
            "@event on value() setter requires event name or class",
        )

    def test_raises_when_property_decorates_event_without_name(self) -> None:
        with self.assertRaises(TypeError) as cm:

            class MyAgg(Aggregate):
                def __init__(self, _: Any) -> None:
                    pass

                @property
                def value(self) -> None:
                    return None

                @value.setter
                @event
                def value(self, _: Any) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0],
            "@event under value() property setter requires event class name",
        )

    def test_raises_when_event_decorator_used_with_wrong_args(self) -> None:
        with self.assertRaises(TypeError) as cm:
            event(1)  # type: ignore[call-overload]
        self.assertEqual(
            "1 is not a str, function, property, or subclass of CanMutateAggregate",
            cm.exception.args[0],
        )

        with self.assertRaises(TypeError) as cm:
            event("EventName")(1)  # type: ignore[type-var]
        self.assertEqual(
            "1 is not a function or property",
            cm.exception.args[0],
        )

    def test_raises_when_decorated_method_has_variable_args(self) -> None:
        with self.assertRaises(TypeError) as cm:

            class _1(Aggregate):  # noqa: N801
                @event  # no event name
                def method(self, *args: Any) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0], "*args not supported by decorator on method()"
        )

        with self.assertRaises(TypeError) as cm:

            class _2(Aggregate):  # noqa: N801
                @event("EventName")  # has event name
                def method(self, *args: Any) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0], "*args not supported by decorator on method()"
        )

    def test_raises_when_decorated_method_has_variable_kwargs(self) -> None:
        with self.assertRaises(TypeError) as cm:

            class _1(Aggregate):  # noqa: N801
                @event  # no event name
                def method(self, **kwargs: Any) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0], "**kwargs not supported by decorator on method()"
        )

        with self.assertRaises(TypeError) as cm:

            class _2(Aggregate):  # noqa: N801
                @event("EventName")  # has event name
                def method(self, **kwargs: Any) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0], "**kwargs not supported by decorator on method()"
        )

        # With property.
        with self.assertRaises(TypeError) as cm:

            class _3(Aggregate):  # noqa: N801
                @property
                def name(self) -> None:
                    return None

                # before setter
                @event("EventName")  # type: ignore[misc]
                @name.setter
                def name(self, **kwargs: Any) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0], "**kwargs not supported by decorator on name()"
        )

        with self.assertRaises(TypeError) as cm:

            class _4(Aggregate):  # noqa: N801
                @property
                def name(self) -> None:
                    return None

                @name.setter
                @event("EventName")  # after setter (same as without property)
                def name(self, **kwargs: Any) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0], "**kwargs not supported by decorator on name()"
        )

    # TODO: Somehow deal with custom decorators?
    # def test_custom_decorators(self) -> None:
    #
    #     def mydecorator(f):
    #         def g(*args, **kwargs):
    #             f(*args, **kwargs)
    #         return g
    #
    #     @aggregate
    #     class MyAgg:
    #         @event
    #         @mydecorator
    #         def method(self) -> None:
    #             raise Exception("Shou")
    #
    #     a = MyAgg()
    #     a.method()
    #

    def test_event_decorator_uses_explicit_event_classes(self) -> None:
        # Here we just use the @event decorator to trigger events
        # that are applied using the decorated method.
        @aggregate
        class Order:
            class Confirmed(AggregateEvent):
                at: datetime

            @triggers(Confirmed)
            def confirm(self, at: datetime) -> None:
                self.confirmed_at = at

        order = Order()

        order.confirm(AggregateEvent.create_timestamp())
        self.assertIsInstance(order.confirmed_at, datetime)

        app: Application = Application()
        app.save(order)  # type: ignore[arg-type]

        copy: Order = app.repository.get(order.id)  # type: ignore[attr-defined]

        self.assertEqual(copy.confirmed_at, order.confirmed_at)

        self.assertIsInstance(order, Aggregate)
        self.assertIsInstance(order, Order)
        self.assertIsInstance(copy, Aggregate)
        self.assertIsInstance(copy, Order)

    # def test_raises_when_event_class_has_apply_method(self) -> None:
    #     # Check raises when defining an apply method on an
    #     # event used in a decorator when aggregate inherits
    #     # from Aggregate class.
    #     with self.assertRaises(TypeError) as cm:
    #
    #         class _(Aggregate):
    #             class Confirmed(AggregateEvent):
    #                 def apply(self, aggregate):
    #                     pass
    #
    #             @triggers(Confirmed)
    #             def confirm(self) -> None:
    #                 pass
    #
    #     self.assertEqual(
    #         cm.exception.args[0], "event class has unexpected apply() method"
    #     )

    def test_raises_when_event_class_already_defined(self) -> None:
        # Here we just use the @event decorator to trigger events
        # that are applied using the decorated method.
        with self.assertRaises(TypeError) as cm:

            @aggregate
            class Order(Aggregate):
                class Confirmed(AggregateEvent):
                    at: datetime

                @triggers("Confirmed")
                def confirm(self, at: datetime) -> None:
                    self.confirmed_at = at

        self.assertEqual(
            cm.exception.args[0], "Confirmed event already defined on Order"
        )

    def test_raises_when_event_class_name_used_twice(self) -> None:
        # Here we make sure the same event class name can't be
        # declared on two decorators.
        with self.assertRaises(TypeError) as cm:

            @aggregate
            class Order(Aggregate):
                @triggers("Confirmed")
                def confirm1(self, at: datetime) -> None:
                    self.confirmed_at = at

                @triggers("Confirmed")
                def confirm2(self, at: datetime) -> None:
                    self.confirmed_at = at

        self.assertEqual(
            cm.exception.args[0], "Confirmed event already defined on Order"
        )

    def test_raises_when_event_class_used_twice(self) -> None:
        # Here we make sure the same event class can't be
        # mentioned on two decorators.
        with self.assertRaises(TypeError) as cm:

            @aggregate
            class Order(Aggregate):
                class Confirmed(AggregateEvent):
                    at: datetime

                @triggers(Confirmed)
                def confirm1(self, at: datetime) -> None:
                    self.confirmed_at = at

                @triggers(Confirmed)
                def confirm2(self, at: datetime) -> None:
                    self.confirmed_at = at

        self.assertEqual(
            cm.exception.args[0],
            "Confirmed event class used in more than one decorator",
        )

    def test_dirty_style_isnt_so_dirty_after_all(self) -> None:
        class Order(Aggregate):
            def __init__(self, name: str) -> None:
                self.name = name
                self.confirmed_at: datetime | None = None
                self.pickedup_at: datetime | None = None

            @event("Confirmed")
            def confirm(self, at: datetime) -> None:
                self.confirmed_at = at

            @event("PickedUp")
            def pickup(self, at: datetime) -> None:
                if self.confirmed_at is None:
                    msg = "Order is not confirmed"
                    raise RuntimeError(msg)
                self.pickedup_at = at

        order = Order("name")
        self.assertEqual(len(order.pending_events), 1)
        with contextlib.suppress(RuntimeError):
            order.pickup(AggregateEvent.create_timestamp())
        self.assertEqual(len(order.pending_events), 1)

    def test_aggregate_has_a_created_event_name_defined_with_event_decorator(
        self,
    ) -> None:
        class MyAggregate(Aggregate):
            @event("Started")
            def __init__(self) -> None:
                pass

        a = MyAggregate()
        created_event = a.pending_events[0]
        created_event_cls = type(created_event)
        self.assertEqual(created_event_cls.__name__, "Started")

        self.assertTrue(created_event_cls.__qualname__.endswith("MyAggregate.Started"))
        self.assertTrue(issubclass(created_event_cls, AggregateCreated))
        self.assertEqual(created_event_cls, MyAggregate.Started)  # type: ignore[attr-defined]

    def test_one_of_many_created_events_selected_by_init_method_decorator(self) -> None:
        class MyAggregate(Aggregate):
            class Started(AggregateCreated):
                pass

            class Opened(AggregateCreated):
                pass

            @event(Started)
            def __init__(self) -> None:
                pass

        a = MyAggregate()
        created_event = a.pending_events[0]
        created_event_cls = type(created_event)
        self.assertEqual(created_event_cls.__name__, "Started")
        self.assertTrue(created_event_cls.__qualname__.endswith("MyAggregate.Started"))
        self.assertTrue(issubclass(created_event_cls, AggregateCreated))
        self.assertEqual(created_event_cls, MyAggregate.Started)

    def test_aggregate_has_incompatible_created_event_class_in_event_decorator(
        self,
    ) -> None:
        # Event mentions 'a' but constructor doesn't.
        class MyAggregate1(Aggregate):
            class Started(AggregateCreated):
                a: int

            @event(Started)
            def __init__(self) -> None:
                pass

        with self.assertRaises(TypeError) as cm:
            MyAggregate1()

        # Check error message.
        errmsg = cm.exception.args[0]
        self.assertTrue(
            errmsg.startswith(
                f"Unable to construct '{MyAggregate1.Started.__qualname__}' event:"
            ),
            errmsg,
        )
        self.assertTrue(
            errmsg.endswith("__init__() missing 1 required positional argument: 'a'"),
            errmsg,
        )

        with self.assertRaises(TypeError) as cm:
            MyAggregate1(a=1)  # type: ignore[call-arg]

        method_name = get_method_name(MyAggregate1.__init__)
        self.assertEqual(
            f"{method_name}() got an unexpected keyword argument 'a'",
            cm.exception.args[0],
        )

        # Constructor mentions 'a' but event doesn't.
        class MyAggregate2(Aggregate):
            class Started(AggregateCreated):
                pass

            @event(Started)
            def __init__(self, a: int):
                self.a = a

        with self.assertRaises(TypeError) as cm:
            MyAggregate2()  # type: ignore[call-arg]

        # Check error message.
        method_name = get_method_name(MyAggregate2.__init__)
        self.assertEqual(
            f"{method_name}() missing 1 required positional argument: 'a'",
            cm.exception.args[0],
        )

        with self.assertRaises(TypeError) as cm:
            MyAggregate2(a=1)
        errmsg = cm.exception.args[0]
        self.assertTrue(
            errmsg.startswith(
                f"Unable to construct '{MyAggregate2.Started.__qualname__}' event:"
            ),
            errmsg,
        )
        self.assertTrue(
            errmsg.endswith("__init__() got an unexpected keyword argument 'a'"),
            errmsg,
        )

    def test_raises_when_using_created_event_name_and_init_event_decorator(
        self,
    ) -> None:
        # Different name.
        with self.assertRaises(TypeError) as cm:

            class _1(Aggregate, created_event_name="Opened"):  # noqa: N801
                class Started(AggregateCreated):
                    a: int

                class Opened(AggregateCreated):
                    a: int

                @event("Started")
                def __init__(self) -> None:
                    pass

        self.assertEqual(
            "Can't use both 'created_event_name' and decorator on __init__",
            cm.exception.args[0],
        )

        # Same name.
        with self.assertRaises(TypeError) as cm:

            class _2(Aggregate, created_event_name="Opened"):  # noqa: N801
                class Started(AggregateCreated):
                    a: int

                class Opened(AggregateCreated):
                    a: int

                @event("Opened")
                def __init__(self) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0],
            "Can't use both 'created_event_name' and decorator on __init__",
        )

        # Different class.
        with self.assertRaises(TypeError) as cm:

            class _3(Aggregate, created_event_name="Opened"):  # noqa: N801
                class Started(AggregateCreated):
                    a: int

                class Opened(AggregateCreated):
                    a: int

                @event(Started)
                def __init__(self) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0],
            "Can't use both 'created_event_name' and decorator on __init__",
        )

        # Same class.
        with self.assertRaises(TypeError) as cm:

            class _4(Aggregate, created_event_name="Opened"):  # noqa: N801
                class Started(AggregateCreated):
                    a: int

                class Opened(AggregateCreated):
                    a: int

                @event(Opened)
                def __init__(self) -> None:
                    pass

        self.assertEqual(
            cm.exception.args[0],
            "Can't use both 'created_event_name' and decorator on __init__",
        )

    def test_raises_when_using_init_event_decorator_without_args(self) -> None:
        # Different name.
        with self.assertRaises(TypeError) as cm:

            class _(Aggregate):  # noqa: N801
                @event
                def __init__(self) -> None:
                    pass

        self.assertEqual(
            "Decorator on __init__ has neither event name nor class",
            cm.exception.args[0],
        )

    def test_raises_type_error_if_given_event_class_cannot_init_aggregate(self) -> None:
        with self.assertRaises(TypeError) as cm:

            class MyAggregate(Aggregate):
                @event(Aggregate.Event)
                def __init__(self) -> None:
                    pass

        self.assertIn("not subclass of CanInit", cm.exception.args[0])

    def test_raises_if_given_event_class_on_command_method_can_init_aggregate(
        self,
    ) -> None:
        with self.assertRaises(TypeError) as cm:

            class MyAggregate(Aggregate):
                @event(Aggregate.Created)
                def do_something(self) -> None:
                    pass

        self.assertIn("is subclass of CanInit", cm.exception.args[0])

    def test_raises_if_given_event_class_on_command_method_is_not_aggregate_event(
        self,
    ) -> None:
        with self.assertRaises(TypeError) as cm:

            class X:
                pass

            class MyAggregate(Aggregate):
                @event(X)  # type: ignore[call-arg, type-var]
                def do_something(self) -> None:
                    pass

        self.assertIn(
            "is not a str, function, property, or subclass of CanMutateAggregate",
            cm.exception.args[0],
        )

    def test_decorated_method_has_original_docstring(self) -> None:
        class MyAggregate(Aggregate):
            def method0(self) -> None:
                """Method 0"""

            @event
            def method1(self) -> None:
                """Method 1"""

        self.assertEqual(MyAggregate.method0.__doc__, "Method 0")
        self.assertEqual(MyAggregate().method0.__doc__, "Method 0")
        self.assertEqual(MyAggregate.method1.__doc__, "Method 1")
        self.assertEqual(MyAggregate().method1.__doc__, "Method 1")

    def test_decorated_method_has_original_annotations(self) -> None:
        class MyAggregate(Aggregate):
            def method0(self, a: int) -> None:
                """Method 0"""

            @event
            def method1(self, a: int) -> None:
                """Method 1"""

        expected_annotations = {"a": "int", "return": "None"}
        self.assertEqual(MyAggregate.method0.__annotations__, expected_annotations)
        self.assertEqual(MyAggregate().method0.__annotations__, expected_annotations)
        self.assertEqual(MyAggregate.method1.__annotations__, expected_annotations)
        self.assertEqual(MyAggregate().method1.__annotations__, expected_annotations)

    def test_decorated_method_has_original_module(self) -> None:
        class MyAggregate(Aggregate):
            def method0(self, a: int) -> None:
                """Method 0"""

            @event
            def method1(self, a: int) -> None:
                """Method 1"""

        expected_module = __name__
        self.assertEqual(MyAggregate.method0.__module__, expected_module)
        self.assertEqual(MyAggregate().method0.__module__, expected_module)
        self.assertEqual(MyAggregate.method1.__module__, expected_module)
        self.assertEqual(MyAggregate().method1.__module__, expected_module)

    def test_decorated_method_has_original_name(self) -> None:
        class MyAggregate(Aggregate):
            def method0(self, a: int) -> None:
                """Method 0"""

            @event
            def method1(self, a: int) -> None:
                """Method 1"""

        self.assertEqual(MyAggregate.method0.__name__, "method0")
        self.assertEqual(MyAggregate().method0.__name__, "method0")
        self.assertEqual(MyAggregate.method1.__name__, "method1")
        self.assertEqual(MyAggregate().method1.__name__, "method1")

    # def test_raises_when_apply_method_returns_value(self) -> None:
    #     # Different name.
    #     class MyAgg(Aggregate):
    #         @event("EventName")
    #         def name(self) -> None:
    #             return 1
    #
    #     a = MyAgg()
    #
    #     with self.assertRaises(TypeError) as cm:
    #         a.name()
    #     msg = str(cm.exception.args[0])
    #     self.assertTrue(msg.startswith("Unexpected value returned from "), msg)
    #     self.assertTrue(
    #         msg.endswith(
    #             "MyAgg.name(). Values returned from 'apply' methods are discarded."
    #         ),
    #         msg,
    #     )
    def test_can_include_timestamp_in_command_method_signature(self) -> None:
        class Order(Aggregate):
            def __init__(self, name: str, timestamp: datetime | None = None) -> None:
                self.name = name
                self.confirmed_at: datetime | None = None
                self.pickedup_at: datetime | None = None

            class Started(AggregateCreated):
                name: str

            @event("Confirmed")
            def confirm(self, timestamp: datetime | None = None) -> None:
                self.confirmed_at = timestamp

            class PickedUp(Aggregate.Event):
                pass

            @event(PickedUp)
            def picked_up(self, timestamp: datetime | None = None) -> None:
                self.pickedup_at = timestamp

        order1 = Order("order1")
        self.assertIsInstance(order1.created_on, datetime)
        order1.confirm()
        self.assertIsInstance(order1.modified_on, datetime)
        self.assertGreater(order1.modified_on, order1.created_on)

        order2 = Order(
            "order2", timestamp=datetime(year=2000, month=1, day=1, tzinfo=timezone.utc)
        )
        self.assertIsInstance(order2.created_on, datetime)
        self.assertEqual(order2.created_on.year, 2000)
        self.assertEqual(order2.created_on.month, 1)
        self.assertEqual(order2.created_on.day, 1)

        order2.confirm(
            timestamp=datetime(year=2000, month=1, day=2, tzinfo=timezone.utc)
        )
        self.assertIsInstance(order2.created_on, datetime)
        self.assertEqual(order2.modified_on.year, 2000)
        self.assertEqual(order2.modified_on.month, 1)
        self.assertEqual(order2.modified_on.day, 2)
        self.assertEqual(order2.confirmed_at, order2.modified_on)

        order2.picked_up(
            timestamp=datetime(year=2000, month=1, day=3, tzinfo=timezone.utc)
        )
        self.assertIsInstance(order2.created_on, datetime)
        self.assertEqual(order2.modified_on.year, 2000)
        self.assertEqual(order2.modified_on.month, 1)
        self.assertEqual(order2.modified_on.day, 3)
        self.assertEqual(order2.pickedup_at, order2.modified_on)


class TestOrder(TestCase):
    def test(self) -> None:
        class Order(Aggregate):
            def __init__(self, name: str) -> None:
                self.name = name
                self.confirmed_at: datetime | None = None
                self.pickedup_at: datetime | None = None

            class Started(AggregateCreated):
                name: str

            @event("Confirmed")
            def confirm(self, at: datetime) -> None:
                self.confirmed_at = at

            def pickup(self, at: datetime) -> None:
                if self.confirmed_at:
                    self._pickup(at)
                else:
                    msg = "Order is not confirmed"
                    raise Exception(msg)

            @event("Pickedup")
            def _pickup(self, at: datetime) -> None:
                self.pickedup_at = at

        order = Order("my order")
        self.assertEqual(order.name, "my order")

        with self.assertRaises(Exception) as cm:
            order.pickup(AggregateEvent.create_timestamp())
        self.assertEqual(cm.exception.args[0], "Order is not confirmed")

        self.assertEqual(order.confirmed_at, None)
        self.assertEqual(order.pickedup_at, None)

        order.confirm(AggregateEvent.create_timestamp())
        self.assertIsInstance(order.confirmed_at, datetime)
        self.assertEqual(order.pickedup_at, None)

        order.pickup(AggregateEvent.create_timestamp())
        self.assertIsInstance(order.confirmed_at, datetime)
        self.assertIsInstance(order.pickedup_at, datetime)

        # Check the events determine the state correctly.
        pending_events = order.collect_events()
        copy = None
        for e in pending_events:
            copy = e.mutate(copy)

        assert isinstance(copy, Order)
        self.assertEqual(copy.name, order.name)
        self.assertEqual(copy.created_on, order.created_on)
        self.assertEqual(copy.modified_on, order.modified_on)
        self.assertEqual(copy.confirmed_at, order.confirmed_at)
        self.assertEqual(copy.pickedup_at, order.pickedup_at)
