from __future__ import annotations

from typing import TYPE_CHECKING

from eventsourcing.application import Application

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


def test_construct_application(benchmark: BenchmarkFixture) -> None:
    benchmark(Application)
