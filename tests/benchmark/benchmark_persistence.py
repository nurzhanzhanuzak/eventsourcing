from dataclasses import dataclass
from uuid import uuid4

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from eventsourcing.application import Application
from eventsourcing.domain import Aggregate, event
from eventsourcing.persistence import InfrastructureFactory, StoredEvent
from eventsourcing.utils import Environment, clear_topic_cache

envs = {
    "popo": {
        "PERSISTENCE_MODULE": "eventsourcing.popo",
    },
    "sqlite": {
        "PERSISTENCE_MODULE": "eventsourcing.sqlite",
        "SQLITE_DBNAME": ":memory:",
    },
    "postgres": {
        "PERSISTENCE_MODULE": "eventsourcing.postgres",
        "POSTGRES_DBNAME": "eventsourcing",
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": "eventsourcing",
        "POSTGRES_PASSWORD": "eventsourcing",
    },
}

param_arg_names = "env,num_events"
param_arg_values = [
    ("popo", 1),
    # ("popo", 10),
    ("popo", 100),
    ("sqlite", 1),
    # ("sqlite", 10),
    ("sqlite", 100),
    ("postgres", 1),
    # ("postgres", 10),
    ("postgres", 100),
]


@pytest.mark.parametrize("num_events", [1, 100])
def test_stored_event(num_events: int, benchmark: BenchmarkFixture) -> None:
    originator_id = uuid4()

    def func() -> None:
        _ = [
            StoredEvent(
                originator_id=originator_id,
                originator_version=i + 1,
                topic="topic1",
                state=b"state1",
            )
            for i in range(num_events)
        ]

    benchmark(func)


@pytest.mark.parametrize("num_events", [1, 100])
def test_uuid4(num_events: int, benchmark: BenchmarkFixture) -> None:
    def func() -> None:
        _ = [uuid4() for i in range(num_events)]

    benchmark(func)


@pytest.mark.parametrize(param_arg_names, param_arg_values)
def test_recorder_insert_events(
    env: str, num_events: int, benchmark: BenchmarkFixture
) -> None:
    recorder = InfrastructureFactory.construct(
        env=Environment(name="benchmark", env=envs[env])
    ).application_recorder()

    def func() -> None:
        originator_id = uuid4()
        events = [
            StoredEvent(
                originator_id=originator_id,
                originator_version=i + 1,
                topic="topic1",
                state=b"state1",
            )
            for i in range(num_events)
        ]
        recorder.insert_events(events)

    benchmark(func)


@pytest.mark.parametrize(param_arg_names, param_arg_values)
def test_recorder_select_events(
    env: str, num_events: int, benchmark: BenchmarkFixture
) -> None:
    recorder = InfrastructureFactory.construct(
        env=Environment(name="benchmark", env=envs[env])
    ).application_recorder()

    originator_id = uuid4()
    events = [
        StoredEvent(
            originator_id=originator_id,
            originator_version=i + 1,
            topic="topic1",
            state=b"state1",
        )
        for i in range(num_events)
    ]

    recorder.insert_events(events)

    def func() -> None:
        recorder.select_events(originator_id)

    benchmark(func)


@pytest.mark.parametrize(param_arg_names, param_arg_values)
def test_app_save(env: str, num_events: int, benchmark: BenchmarkFixture) -> None:

    clear_topic_cache()

    @dataclass
    class A(Aggregate):
        a: int

        @event("Continued")
        def subsequent(self, a: int) -> None:
            self.a = a

    app = Application(env=envs[env])
    agg = A(a=0)
    for i in range(num_events - 1):
        agg.subsequent(a=i + 1)

    def func() -> None:
        app.save(agg)

    benchmark(func)


@pytest.mark.parametrize(param_arg_names, param_arg_values)
def test_repository_get(env: str, num_events: int, benchmark: BenchmarkFixture) -> None:

    clear_topic_cache()

    @dataclass
    class A(Aggregate):
        a: int

        @event("Continued")
        def subsequent(self, a: int) -> None:
            self.a = a

    app = Application(env=envs[env])
    agg = A(a=0)
    for i in range(num_events - 1):
        agg.subsequent(a=i + 1)
    app.save(agg)

    def func() -> None:
        app.repository.get(agg.id)

    benchmark(func)
