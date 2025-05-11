from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from eventsourcing.application import Application
from eventsourcing.domain import Aggregate, event
from eventsourcing.persistence import InfrastructureFactory, StoredEvent
from eventsourcing.postgres import PostgresApplicationRecorder
from eventsourcing.tests.postgres_utils import drop_postgres_table
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
@pytest.mark.benchmark(group="construct-stored-event")
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
@pytest.mark.benchmark(group="new-uuid4")
def test_uuid4(num_events: int, benchmark: BenchmarkFixture) -> None:
    def func() -> None:
        _ = [uuid4() for i in range(num_events)]

    benchmark(func)


@pytest.mark.parametrize(param_arg_names, param_arg_values)
@pytest.mark.benchmark(group="recorder-insert-events")
def test_recorder_insert_events(
    env: str, num_events: int, benchmark: BenchmarkFixture
) -> None:
    recorder = InfrastructureFactory.construct(
        env=Environment(name="benchmark", env=envs[env])
    ).application_recorder()

    def setup() -> Any:
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
        return (events,), {}

    def func(events: Sequence[StoredEvent]) -> None:
        recorder.insert_events(events)

    rounds = {
        "popo": 5000,
        "sqlite": 3000,
        "postgres": 300,
    }

    try:
        benchmark.pedantic(func, setup=setup, rounds=rounds[env])
    finally:
        if isinstance(recorder, PostgresApplicationRecorder):
            drop_postgres_table(recorder.datastore, recorder.events_table_name)


@pytest.mark.parametrize(param_arg_names, param_arg_values)
@pytest.mark.benchmark(group="recorder-select-events")
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

    try:
        benchmark(func)
    finally:
        if isinstance(recorder, PostgresApplicationRecorder):
            drop_postgres_table(recorder.datastore, recorder.events_table_name)


@pytest.mark.parametrize(param_arg_names, param_arg_values)
@pytest.mark.benchmark(group="call-app-save")
def test_app_save(env: str, num_events: int, benchmark: BenchmarkFixture) -> None:

    app = Application(env=envs[env])

    clear_topic_cache()

    @dataclass
    class A(Aggregate):
        a: int

        @event("Continued")
        def subsequent(self, a: int) -> None:
            self.a = a

    def setup() -> Any:
        agg = A(a=0)
        for i in range(num_events - 1):
            agg.subsequent(a=i + 1)
        return (app, agg), {}

    def func(app: Application, agg: Aggregate) -> None:
        app.save(agg)

    rounds = {
        "popo": 5000,
        "sqlite": 3000,
        "postgres": 300,
    }

    try:
        benchmark.pedantic(func, setup=setup, rounds=rounds[env])
    finally:
        if isinstance(app.recorder, PostgresApplicationRecorder):
            drop_postgres_table(app.recorder.datastore, app.recorder.events_table_name)


@pytest.mark.parametrize(param_arg_names, param_arg_values)
@pytest.mark.benchmark(group="call-app-command")
def test_app_command(env: str, num_events: int, benchmark: BenchmarkFixture) -> None:

    @dataclass
    class A(Aggregate):
        a: int

        @event("Continued")
        def subsequent(self, a: int) -> None:
            self.a = a

    class MyApplication(Application):
        def command(self) -> None:
            agg = A(a=0)
            for i in range(num_events - 1):
                agg.subsequent(a=i + 1)
            self.save(agg)

    app = MyApplication(env=envs[env])

    clear_topic_cache()

    try:
        benchmark(app.command)
    finally:
        if isinstance(app.recorder, PostgresApplicationRecorder):
            drop_postgres_table(app.recorder.datastore, app.recorder.events_table_name)


@pytest.mark.parametrize(param_arg_names, param_arg_values)
@pytest.mark.benchmark(group="call-repository-get")
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

    try:
        benchmark(func)
    finally:
        if isinstance(app.recorder, PostgresApplicationRecorder):
            drop_postgres_table(app.recorder.datastore, app.recorder.events_table_name)
