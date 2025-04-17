from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar
from unittest import TestCase

from eventsourcing.application import Application
from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.domain import Aggregate, DomainEventProtocol
from eventsourcing.persistence import InfrastructureFactory, Tracking, TrackingRecorder
from eventsourcing.popo import POPOTrackingRecorder
from eventsourcing.postgres import PostgresDatastore, PostgresTrackingRecorder
from eventsourcing.projection import Projection, ProjectionRunner
from eventsourcing.tests.postgres_utils import drop_postgres_table
from eventsourcing.utils import Environment

if TYPE_CHECKING:
    from collections.abc import Mapping

    from psycopg import Connection
    from psycopg.rows import DictRow


class CountRecorderInterface(TrackingRecorder):
    @abstractmethod
    def incr_created_events_counter(self, tracking: Tracking) -> None:
        pass

    @abstractmethod
    def incr_subsequent_events_counter(self, tracking: Tracking) -> None:
        pass

    @abstractmethod
    def get_created_events_counter(self) -> int:
        pass

    @abstractmethod
    def get_subsequent_events_counter(self) -> int:
        pass


class POPOCountRecorder(POPOTrackingRecorder, CountRecorderInterface):
    def __init__(self) -> None:
        super().__init__()
        self._created_events_counter = 0
        self._subsequent_events_counter = 0

    def incr_created_events_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._assert_tracking_uniqueness(tracking)
            self._insert_tracking(tracking)
            self._created_events_counter += 1

    def incr_subsequent_events_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._assert_tracking_uniqueness(tracking)
            self._insert_tracking(tracking)
            self._subsequent_events_counter += 1

    def get_created_events_counter(self) -> int:
        return self._created_events_counter

    def get_subsequent_events_counter(self) -> int:
        return self._subsequent_events_counter


class PostgresCountRecorder(PostgresTrackingRecorder, CountRecorderInterface):
    def __init__(
        self,
        datastore: PostgresDatastore,
        **kwargs: Any,
    ):
        super().__init__(datastore, **kwargs)
        self.counter_table_name = "countprojection"
        self.check_table_name_length(self.counter_table_name)
        self.create_table_statements.append(
            "CREATE TABLE IF NOT EXISTS "
            f"{self.counter_table_name} ("
            "counter_name text, "
            "counter bigint, "
            "PRIMARY KEY "
            "(counter_name))"
        )
        self.select_counter_statement = (
            f"SELECT counter FROM {self.counter_table_name} WHERE counter_name=%s"
        )

        self.incr_counter_statement = (
            f"INSERT INTO {self.counter_table_name} VALUES (%s, 1) "
            f"ON CONFLICT (counter_name) DO UPDATE "
            f"SET counter = {self.counter_table_name}.counter + 1"
        )

    def incr_created_events_counter(self, tracking: Tracking) -> None:
        self._incr_counter("CREATED_EVENTS", tracking)

    def incr_subsequent_events_counter(self, tracking: Tracking) -> None:
        self._incr_counter("SUBSEQUENT_EVENTS", tracking)

    def _incr_counter(self, name: str, tracking: Tracking) -> None:
        conn: Connection[DictRow]
        with (
            self.datastore.get_connection() as conn,
            conn.transaction(),
            conn.cursor() as curs,
        ):
            self._insert_tracking(curs, tracking)
            curs.execute(
                query=self.incr_counter_statement,
                params=(name,),
                prepare=True,
            )

    def get_created_events_counter(self) -> int:
        return self._select_counter("CREATED_EVENTS")

    def get_subsequent_events_counter(self) -> int:
        return self._select_counter("SUBSEQUENT_EVENTS")

    def _select_counter(self, name: str) -> int:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            curs.execute(
                query=self.select_counter_statement,
                params=(name,),
                prepare=True,
            )
            fetchone = curs.fetchone()
            return fetchone["counter"] if fetchone else 0


class SpannerThrown(Aggregate.Event):
    pass


class SpannerThrownError(Exception):
    pass


class CountProjection(Projection[CountRecorderInterface]):
    def __init__(
        self,
        view: CountRecorderInterface,
    ):
        assert isinstance(view, CountRecorderInterface), type(view)
        super().__init__(view)

    @singledispatchmethod
    def process_event(self, _: DomainEventProtocol, tracking: Tracking) -> None:
        pass

    @process_event.register
    def aggregate_created(self, _: Aggregate.Created, tracking: Tracking) -> None:
        self.view.incr_created_events_counter(tracking)

    @process_event.register
    def aggregate_event(self, _: Aggregate.Event, tracking: Tracking) -> None:
        self.view.incr_subsequent_events_counter(tracking)

    @process_event.register
    def spanner_thrown(self, _: SpannerThrown, __: Tracking) -> None:
        msg = "This is a deliberate bug"
        raise SpannerThrownError(msg)


class TestCountProjection(TestCase, ABC):
    env: ClassVar[Mapping[str, str]] = {}
    view_class: type[CountRecorderInterface] = POPOCountRecorder

    def test_runner_with_count_projection(self) -> None:
        # Construct runner with application, projection, and recorder.
        runner = ProjectionRunner(
            application_class=Application,
            view_class=self.view_class,
            projection_class=CountProjection,
            env=self.env,
        )

        # Get "read" and "write" model instances from the runner.
        write_model = runner.app
        read_model = runner.projection.view

        # Write some events.
        aggregate = Aggregate()
        aggregate.trigger_event(event_class=Aggregate.Event)
        aggregate.trigger_event(event_class=Aggregate.Event)
        recordings = write_model.save(aggregate)

        # Wait for the events to be processed.
        read_model.wait(write_model.name, recordings[-1].notification.id)

        # Query the read model.
        self.assertEqual(read_model.get_created_events_counter(), 1)
        self.assertEqual(read_model.get_subsequent_events_counter(), 2)

        # Write some more events.
        aggregate = Aggregate()
        aggregate.trigger_event(event_class=Aggregate.Event)
        aggregate.trigger_event(event_class=Aggregate.Event)
        recordings = write_model.save(aggregate)

        # Wait for the events to be processed.
        read_model.wait(write_model.name, recordings[-1].notification.id)

        # Query the read model.
        self.assertEqual(read_model.get_created_events_counter(), 2)
        self.assertEqual(read_model.get_subsequent_events_counter(), 4)

    def test_run_forever_raises_projection_error(self) -> None:
        # Construct runner with application, projection, and recorder.
        runner = ProjectionRunner(
            application_class=Application,
            view_class=self.view_class,
            projection_class=CountProjection,
            env=self.env,
        )
        write_model = runner.app
        read_model = runner.projection.view

        # Write some events.
        aggregate = Aggregate()
        aggregate.trigger_event(event_class=SpannerThrown)
        recordings = write_model.save(aggregate)

        # Projection runner terminates with projection error.
        with self.assertRaises(SpannerThrownError):
            runner.run_forever()

        # Wait times out (event has not been processed).
        with self.assertRaises(TimeoutError):
            read_model.wait(write_model.name, recordings[-1].notification.id)


class TestCountProjectionWithPostgres(TestCountProjection):
    env: ClassVar[dict[str, str]] = {
        "PERSISTENCE_MODULE": "eventsourcing.postgres",
        "POSTGRES_DBNAME": "eventsourcing",
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": "eventsourcing",
        "POSTGRES_PASSWORD": "eventsourcing",
    }
    view_class = PostgresCountRecorder

    def test_runner_with_count_projection(self) -> None:
        super().test_runner_with_count_projection()
        # Resume....
        _ = ProjectionRunner(
            application_class=Application,
            view_class=self.view_class,
            projection_class=CountProjection,
            env=self.env,
        )

        # Construct separate instance of "write model".
        write_model = Application(self.env)

        # Construct separate instance of "read model".
        read_model = (
            InfrastructureFactory[CountRecorderInterface]
            .construct(env=Environment(name=CountProjection.__name__, env=self.env))
            .tracking_recorder(self.view_class)
        )

        # Write some events.
        aggregate = Aggregate()
        aggregate.trigger_event(event_class=Aggregate.Event)
        aggregate.trigger_event(event_class=Aggregate.Event)
        recordings = write_model.save(aggregate)

        # Wait for events to be processed.
        read_model.wait(write_model.name, recordings[-1].notification.id)

        # Query the read model.
        self.assertEqual(read_model.get_created_events_counter(), 3)
        self.assertEqual(read_model.get_subsequent_events_counter(), 6)

        # Write some more events.
        aggregate = Aggregate()
        aggregate.trigger_event(event_class=Aggregate.Event)
        aggregate.trigger_event(event_class=Aggregate.Event)
        recordings = write_model.save(aggregate)

        # Wait for events to be processed.
        read_model.wait(write_model.name, recordings[-1].notification.id)

        # Query the read model.
        self.assertEqual(read_model.get_created_events_counter(), 4)
        self.assertEqual(read_model.get_subsequent_events_counter(), 8)

    def test_run_forever_raises_projection_error(self) -> None:
        super().test_run_forever_raises_projection_error()
        # Resume...
        runner = ProjectionRunner(
            application_class=Application,
            view_class=self.view_class,
            projection_class=CountProjection,
            env=self.env,
        )

        # Construct separate instance of "write model".
        write_model = Application(self.env)

        # Construct separate instance of "read model".
        read_model = InfrastructureFactory.construct(
            env=Environment(name=CountProjection.__name__, env=self.env)
        ).tracking_recorder(self.view_class)

        # Still terminates with projection error.
        with self.assertRaises(SpannerThrownError):
            runner.run_forever()

        # Wait times out (event has not been processed).
        with self.assertRaises(TimeoutError):
            read_model.wait(
                write_model.name, write_model.recorder.max_notification_id()
            )

    def setUp(self) -> None:
        super().setUp()
        self.drop_tables()

    def tearDown(self) -> None:
        super().tearDown()
        self.drop_tables()

    def drop_tables(self) -> None:
        datastore = PostgresDatastore(
            "eventsourcing",
            "127.0.0.1",
            "5432",
            "eventsourcing",
            "eventsourcing",
        )
        drop_postgres_table(datastore, "application_events")
        drop_postgres_table(datastore, "countprojection_tracking")
        drop_postgres_table(datastore, "countprojection")
