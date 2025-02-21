from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Dict, Mapping, Type
from unittest import TestCase

from eventsourcing.application import Application
from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.domain import Aggregate, DomainEventProtocol
from eventsourcing.persistence import Tracking, TrackingRecorder
from eventsourcing.popo import POPOTrackingRecorder
from eventsourcing.postgres import PostgresDatastore, PostgresTrackingRecorder
from eventsourcing.projection import Projection, ProjectionRunner
from eventsourcing.tests.postgres_utils import drop_postgres_table

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import DictRow


class CountRecorder(TrackingRecorder):
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

    @abstractmethod
    def get_all_events_counter(self) -> int:
        pass


class POPOCountRecorder(POPOTrackingRecorder, CountRecorder):
    def __init__(self):
        super().__init__()
        self._created_events_counter = 0
        self._subsequent_events_counter = 0

    def incr_created_events_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._insert_tracking(tracking)
            self._created_events_counter += 1

    def incr_subsequent_events_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._insert_tracking(tracking)
            self._subsequent_events_counter += 1

    def get_created_events_counter(self) -> int:
        return self._created_events_counter

    def get_subsequent_events_counter(self) -> int:
        return self._subsequent_events_counter

    def get_all_events_counter(self) -> int:
        return self._created_events_counter + self._subsequent_events_counter


class PostgresCountRecorder(PostgresTrackingRecorder, CountRecorder):
    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        tracking_table_name: str = "OVERWRITTEN",
        **kwargs,
    ):
        _ = tracking_table_name
        tracking_table_name = "countprojection_tracking"
        events_counter_table_name: str = "countprojection"
        super().__init__(datastore, tracking_table_name=tracking_table_name, **kwargs)
        self.check_table_name_length(events_counter_table_name, datastore.schema)
        self.counter_table_name = events_counter_table_name
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
        c: Connection[DictRow]
        with self.datastore.get_connection() as c, c.transaction(), c.cursor() as curs:
            self._insert_tracking(c, tracking)
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

    def get_all_events_counter(self) -> int:
        return self._select_counter("CREATED_EVENTS") + self._select_counter(
            "SUBSEQUENT_EVENTS"
        )


class SpannerThrown(Aggregate.Event):
    pass


class SpannerThrownError(Exception):
    pass


class CountProjection(Projection[CountRecorder]):
    def __init__(
        self,
        tracking_recorder: CountRecorder,
    ):
        assert isinstance(tracking_recorder, CountRecorder), type(tracking_recorder)
        super().__init__(tracking_recorder)

    @singledispatchmethod
    def process_event(self, _: DomainEventProtocol, tracking: Tracking) -> None:
        self.tracking_recorder.incr_subsequent_events_counter(tracking)

    @process_event.register
    def aggregate_created(self, _: Aggregate.Created, tracking: Tracking) -> None:
        self.tracking_recorder.incr_created_events_counter(tracking)

    @process_event.register
    def spanner_thrown(self, _: SpannerThrown, __: Tracking) -> None:
        msg = "This is a deliberate bug"
        raise SpannerThrownError(msg)


class TestCountProjection(TestCase, ABC):
    env: ClassVar[Mapping[str, str]] = {}
    tracking_recorder_class: Type[TrackingRecorder] = POPOCountRecorder

    def test_runner_with_count_projection(self):
        # Construct runner with application, projection, and recorder.
        runner = ProjectionRunner(
            application_class=Application,
            projection_class=CountProjection,
            tracking_recorder_class=self.tracking_recorder_class,
            env=self.env,
        )

        # Get "read" and "write" model instances from the runner.
        write_model = runner.app
        read_model = runner.projection.tracking_recorder

        # Write some events.
        aggregate = Aggregate()
        aggregate.trigger_event(event_class=Aggregate.Event)
        aggregate.trigger_event(event_class=Aggregate.Event)
        recordings = write_model.save(aggregate)

        # Wait for the events to be processed.
        read_model.wait(write_model.name, recordings[-1].notification.id)

        # Query the read model.
        self.assertEqual(read_model.get_all_events_counter(), 3)
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
        self.assertEqual(read_model.get_all_events_counter(), 6)
        self.assertEqual(read_model.get_created_events_counter(), 2)
        self.assertEqual(read_model.get_subsequent_events_counter(), 4)

    def test_run_forever_raises_projection_error(self):
        # Construct runner with application, projection, and recorder.
        runner = ProjectionRunner(
            application_class=Application,
            projection_class=CountProjection,
            tracking_recorder_class=self.tracking_recorder_class,
            env=self.env,
        )
        write_model = runner.app
        read_model = runner.projection.tracking_recorder

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
    env: ClassVar[Dict[str, str]] = {
        "PERSISTENCE_MODULE": "eventsourcing.postgres",
        "POSTGRES_DBNAME": "eventsourcing",
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": "eventsourcing",
        "POSTGRES_PASSWORD": "eventsourcing",
    }
    tracking_recorder_class = PostgresCountRecorder

    def test_runner_with_count_projection(self):
        super().test_runner_with_count_projection()
        # Resume....
        _ = ProjectionRunner(
            application_class=Application,
            projection_class=CountProjection,
            tracking_recorder_class=self.tracking_recorder_class,
            env=self.env,
        )

        # Construct separate instance of application.
        write_model = Application(self.env)

        # Construct separate instance of recorder.
        read_model = PostgresCountRecorder(
            datastore=PostgresDatastore(
                "eventsourcing",
                "127.0.0.1",
                "5432",
                "eventsourcing",
                "eventsourcing",
            ),
        )

        # Write some events.
        aggregate = Aggregate()
        aggregate.trigger_event(event_class=Aggregate.Event)
        aggregate.trigger_event(event_class=Aggregate.Event)
        recordings = write_model.save(aggregate)

        # Wait for events to be processed.
        read_model.wait(write_model.name, recordings[-1].notification.id)

        # Query the read model.
        self.assertEqual(read_model.get_all_events_counter(), 9)
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
        self.assertEqual(read_model.get_all_events_counter(), 12)
        self.assertEqual(read_model.get_created_events_counter(), 4)
        self.assertEqual(read_model.get_subsequent_events_counter(), 8)

    def test_throw_spanner(self):
        super().test_run_forever_raises_projection_error()
        # Resume...
        runner = ProjectionRunner(
            application_class=Application,
            projection_class=CountProjection,
            tracking_recorder_class=self.tracking_recorder_class,
            env=self.env,
        )

        # Construct separate instance of application.
        write_model = Application(self.env)

        # Construct separate instance of recorder.
        read_model = PostgresCountRecorder(
            datastore=PostgresDatastore(
                "eventsourcing",
                "127.0.0.1",
                "5432",
                "eventsourcing",
                "eventsourcing",
            ),
        )

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

    def drop_tables(self):
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
