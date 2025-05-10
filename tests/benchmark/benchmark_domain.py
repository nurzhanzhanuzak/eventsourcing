from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from eventsourcing.domain import Aggregate, event

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


def test_construct_aggregate_base_class(benchmark: BenchmarkFixture) -> None:
    benchmark(Aggregate)


def test_define_aggregate(benchmark: BenchmarkFixture) -> None:
    def define_aggregate() -> None:
        class A(Aggregate):
            a: int
            b: int

            @event("Continued")
            def subsequent(self, a: int, b: int) -> None:
                self.a = a
                self.b = b

    benchmark(define_aggregate)


def test_construct_aggregate_subclass(benchmark: BenchmarkFixture) -> None:
    @dataclass
    class A(Aggregate):
        a: int
        b: int

        @event("Commanded")
        def command(self, a: int, b: int) -> None:
            self.a = a
            self.b = b

    def construct_custom() -> None:
        A(a=1, b=2)

    benchmark(construct_custom)


def test_trigger_aggregate_event(benchmark: BenchmarkFixture) -> None:
    @dataclass
    class A(Aggregate):
        a: int
        b: int

        class Commanded(Aggregate.Event):
            a: int
            b: int

            def apply(self, aggregate: A) -> None:
                aggregate.a = self.a
                aggregate.b = self.b

    a = A(a=1, b=2)

    def func() -> None:
        a.trigger_event(A.Commanded, a=3, b=4)

    benchmark(func)


def test_call_decorated_command_method(benchmark: BenchmarkFixture) -> None:
    @dataclass
    class A(Aggregate):
        a: int
        b: int

        @event("Commanded")
        def command(self, a: int, b: int) -> None:
            self.a = a
            self.b = b

    a = A(a=1, b=2)

    def func() -> None:
        a.command(a=3, b=4)

    benchmark(func)
