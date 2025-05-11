from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from eventsourcing.application import Application

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture


@pytest.mark.benchmark(group="construct-application")
def test_construct_application(benchmark: BenchmarkFixture) -> None:
    benchmark(Application)
