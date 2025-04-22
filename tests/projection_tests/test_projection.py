from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, cast
from unittest import TestCase

from eventsourcing.application import Application
from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.domain import Aggregate, DomainEventProtocol
from eventsourcing.persistence import (
    InfrastructureFactory,
    IntegrityError,
    Tracking,
    TrackingRecorder,
)
from eventsourcing.popo import POPOTrackingRecorder
from eventsourcing.postgres import (
    PostgresDatastore,
    PostgresFactory,
    PostgresTrackingRecorder,
)
from eventsourcing.projection import Projection, ProjectionRunner
from eventsourcing.tests.postgres_utils import drop_postgres_table
from eventsourcing.utils import Environment, get_topic

if TYPE_CHECKING:
    from collections.abc import Mapping


class EventCountersInterface(TrackingRecorder):
    @abstractmethod
    def get_created_event_counter(self) -> int:
        pass

    @abstractmethod
    def get_subsequent_event_counter(self) -> int:
        pass

    @abstractmethod
    def incr_created_event_counter(self, tracking: Tracking) -> None:
        pass

    @abstractmethod
    def incr_subsequent_event_counter(self, tracking: Tracking) -> None:
        pass


class EventCountersViewTestCase(TestCase):
    def construct_event_counters_view(self) -> EventCountersInterface:
        raise NotImplementedError

    def test(self) -> None:
        # Construct materialised view object.
        view = self.construct_event_counters_view()

        # Check the view object is a tracking recorder.
        self.assertIsInstance(view, TrackingRecorder)

        # Check the view has processed no events.
        self.assertIsNone(view.max_tracking_id("upstream"))

        # Check the event counters are zero.
        self.assertEqual(view.get_created_event_counter(), 0)
        self.assertEqual(view.get_subsequent_event_counter(), 0)

        # Increment the "created" event counter.
        view.incr_created_event_counter(Tracking("upstream", 1))
        self.assertEqual(view.get_created_event_counter(), 1)
        self.assertEqual(view.get_subsequent_event_counter(), 0)

        # Increment the subsequent event counter.
        view.incr_subsequent_event_counter(Tracking("upstream", 2))
        self.assertEqual(view.get_created_event_counter(), 1)
        self.assertEqual(view.get_subsequent_event_counter(), 1)

        # Increment the subsequent event counter again.
        view.incr_subsequent_event_counter(Tracking("upstream", 3))
        self.assertEqual(view.get_created_event_counter(), 1)
        self.assertEqual(view.get_subsequent_event_counter(), 2)

        # Check the tracking objects have been recorded.
        self.assertEqual(view.max_tracking_id("upstream"), 3)

        # Check the tracking objects are recorded uniquely and atomically.
        with self.assertRaises(IntegrityError):
            view.incr_created_event_counter(Tracking("upstream", 3))
        self.assertEqual(view.get_created_event_counter(), 1)

        with self.assertRaises(IntegrityError):
            view.incr_subsequent_event_counter(Tracking("upstream", 3))
        self.assertEqual(view.get_subsequent_event_counter(), 2)


class TestPOPOEventCounters(EventCountersViewTestCase):
    def construct_event_counters_view(self) -> EventCountersInterface:
        return POPOEventCounters()


class POPOEventCounters(POPOTrackingRecorder, EventCountersInterface):
    def __init__(self) -> None:
        super().__init__()
        self._created_event_counter = 0
        self._subsequent_event_counter = 0

    def get_created_event_counter(self) -> int:
        return self._created_event_counter

    def get_subsequent_event_counter(self) -> int:
        return self._subsequent_event_counter

    def incr_created_event_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._assert_tracking_uniqueness(tracking)
            self._insert_tracking(tracking)
            self._created_event_counter += 1

    def incr_subsequent_event_counter(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._assert_tracking_uniqueness(tracking)
            self._insert_tracking(tracking)
            self._subsequent_event_counter += 1


class TestPostgresEventCounters(EventCountersViewTestCase):
    def setUp(self) -> None:
        self.factory = cast(
            PostgresFactory,
            InfrastructureFactory.construct(
                env=Environment(
                    name="eventcounters",
                    env={
                        "PERSISTENCE_MODULE": "eventsourcing.postgres",
                        "POSTGRES_DBNAME": "eventsourcing",
                        "POSTGRES_HOST": "127.0.0.1",
                        "POSTGRES_PORT": "5432",
                        "POSTGRES_USER": "eventsourcing",
                        "POSTGRES_PASSWORD": "eventsourcing",
                    },
                )
            ),
        )

    def construct_event_counters_view(self) -> EventCountersInterface:
        return self.factory.tracking_recorder(PostgresEventCounters)

    def tearDown(self) -> None:
        drop_postgres_table(self.factory.datastore, self.factory.env.name + "_tracking")
        drop_postgres_table(self.factory.datastore, self.factory.env.name)


class PostgresEventCounters(PostgresTrackingRecorder, EventCountersInterface):
    _created_event_counter_name = "CREATED_EVENTS"
    _subsequent_event_counter_name = "SUBSEQUENT_EVENTS"

    def __init__(
        self,
        datastore: PostgresDatastore,
        **kwargs: Any,
    ):
        super().__init__(datastore, **kwargs)
        assert self.tracking_table_name.endswith("_tracking")  # Because we replace it.
        self.counters_table_name = self.tracking_table_name.replace("_tracking", "")
        self.check_table_name_length(self.counters_table_name)
        self.create_table_statements.append(
            "CREATE TABLE IF NOT EXISTS "
            f"{self.counters_table_name} ("
            "counter_name text, "
            "counter bigint, "
            "PRIMARY KEY "
            "(counter_name))"
        )
        self.select_counter_statement = (
            f"SELECT counter FROM {self.counters_table_name} WHERE counter_name=%s"
        )

        self.incr_counter_statement = (
            f"INSERT INTO {self.counters_table_name} VALUES (%s, 1) "
            f"ON CONFLICT (counter_name) DO UPDATE "
            f"SET counter = {self.counters_table_name}.counter + 1"
        )

    def get_created_event_counter(self) -> int:
        return self._select_counter(self._created_event_counter_name)

    def get_subsequent_event_counter(self) -> int:
        return self._select_counter(self._subsequent_event_counter_name)

    def incr_created_event_counter(self, tracking: Tracking) -> None:
        self._incr_counter(self._created_event_counter_name, tracking)

    def incr_subsequent_event_counter(self, tracking: Tracking) -> None:
        self._incr_counter(self._subsequent_event_counter_name, tracking)

    def _select_counter(self, name: str) -> int:
        with self.datastore.transaction(commit=False) as curs:
            curs.execute(
                query=self.select_counter_statement,
                params=(name,),
                prepare=True,
            )
            fetchone = curs.fetchone()
            return fetchone["counter"] if fetchone else 0

    def _incr_counter(self, name: str, tracking: Tracking) -> None:
        with self.datastore.transaction(commit=True) as curs:
            self._insert_tracking(curs, tracking)
            curs.execute(
                query=self.incr_counter_statement,
                params=(name,),
                prepare=True,
            )


class SpannerThrown(Aggregate.Event):
    pass


class SpannerThrownError(Exception):
    pass


class EventCountersProjection(Projection[EventCountersInterface]):
    name = "eventcounters"
    topics = [
        get_topic(Aggregate.Created),
        get_topic(Aggregate.Event),
        get_topic(SpannerThrown),
    ]

    @singledispatchmethod
    def process_event(self, _: DomainEventProtocol, tracking: Tracking) -> None:
        pass

    @process_event.register
    def aggregate_created(self, _: Aggregate.Created, tracking: Tracking) -> None:
        self.view.incr_created_event_counter(tracking)

    @process_event.register
    def aggregate_event(self, _: Aggregate.Event, tracking: Tracking) -> None:
        self.view.incr_subsequent_event_counter(tracking)

    @process_event.register
    def spanner_thrown(self, _: SpannerThrown, __: Tracking) -> None:
        msg = "This is a deliberate bug"
        raise SpannerThrownError(msg)


class TestEventCountersProjection(TestCase, ABC):
    view_class: type[EventCountersInterface] = POPOEventCounters
    env: ClassVar[Mapping[str, str]] = {}

    def test_event_counters_projection(self) -> None:
        # Construct runner with application, projection, and recorder.
        with ProjectionRunner(
            application_class=Application,
            projection_class=EventCountersProjection,
            view_class=self.view_class,
            env=self.env,
        ) as runner:

            # Get "read" and "write" model instances from the runner.
            write_model = runner.app
            read_model = runner.projection.view

            # Write some events.
            aggregate = Aggregate()
            aggregate.trigger_event(event_class=Aggregate.Event)
            aggregate.trigger_event(event_class=Aggregate.Event)
            recordings = write_model.save(aggregate)

            # Wait for the events to be processed.
            read_model.wait(
                application_name=write_model.name,
                notification_id=recordings[-1].notification.id,
                timeout=5,
            )

            # Query the read model.
            self.assertEqual(read_model.get_created_event_counter(), 1)
            self.assertEqual(read_model.get_subsequent_event_counter(), 2)

            # Write some more events.
            aggregate = Aggregate()
            aggregate.trigger_event(event_class=Aggregate.Event)
            aggregate.trigger_event(event_class=Aggregate.Event)
            recordings = write_model.save(aggregate)

            # Wait for the events to be processed.
            read_model.wait(
                application_name=write_model.name,
                notification_id=recordings[-1].notification.id,
                timeout=5,
            )

            # Query the read model.
            self.assertEqual(read_model.get_created_event_counter(), 2)
            self.assertEqual(read_model.get_subsequent_event_counter(), 4)

    def test_run_forever_raises_projection_error(self) -> None:
        # Construct runner with application, projection, and recorder.
        with ProjectionRunner(
            application_class=Application,
            projection_class=EventCountersProjection,
            view_class=self.view_class,
            env=self.env,
        ) as runner:
            write_model = runner.app
            read_model = runner.projection.view

            # Write some events.
            aggregate = Aggregate()
            aggregate.trigger_event(event_class=SpannerThrown)
            recordings = write_model.save(aggregate)

            # Projection runner terminates with projection error.
            with self.assertRaises(SpannerThrownError):
                runner.run_forever(timeout=5)

            # Wait times out (event has not been processed).
            with self.assertRaises(TimeoutError):
                read_model.wait(
                    application_name=write_model.name,
                    notification_id=recordings[-1].notification.id,
                    timeout=1,
                )


class TestEventCountersProjectionWithPostgres(TestEventCountersProjection):
    view_class = PostgresEventCounters
    env: ClassVar[dict[str, str]] = {
        "APPLICATION_PERSISTENCE_MODULE": "eventsourcing.postgres",
        "APPLICATION_POSTGRES_DBNAME": "eventsourcing",
        "APPLICATION_POSTGRES_HOST": "127.0.0.1",
        "APPLICATION_POSTGRES_PORT": "5432",
        "APPLICATION_POSTGRES_USER": "eventsourcing",
        "APPLICATION_POSTGRES_PASSWORD": "eventsourcing",
        "EVENTCOUNTERS_PERSISTENCE_MODULE": "eventsourcing.postgres",
        "EVENTCOUNTERS_POSTGRES_DBNAME": "eventsourcing",
        "EVENTCOUNTERS_POSTGRES_HOST": "127.0.0.1",
        "EVENTCOUNTERS_POSTGRES_PORT": "5432",
        "EVENTCOUNTERS_POSTGRES_USER": "eventsourcing",
        "EVENTCOUNTERS_POSTGRES_PASSWORD": "eventsourcing",
    }

    def test_event_counters_projection(self) -> None:
        super().test_event_counters_projection()

        # Resume....
        with ProjectionRunner(
            application_class=Application,
            projection_class=EventCountersProjection,
            view_class=self.view_class,
            env=self.env,
        ):

            # Construct separate instance of "write model".
            write_model = Application(self.env)

            # Construct separate instance of "read model".
            read_model = (
                InfrastructureFactory[EventCountersInterface]
                .construct(
                    env=Environment(name=EventCountersProjection.name, env=self.env)
                )
                .tracking_recorder(self.view_class)
            )

            # Write some events.
            aggregate = Aggregate()
            aggregate.trigger_event(event_class=Aggregate.Event)
            aggregate.trigger_event(event_class=Aggregate.Event)
            recordings = write_model.save(aggregate)

            # Wait for events to be processed.
            read_model.wait(
                application_name=write_model.name,
                notification_id=recordings[-1].notification.id,
                timeout=5,
            )

            # Query the read model.
            self.assertEqual(read_model.get_created_event_counter(), 3)
            self.assertEqual(read_model.get_subsequent_event_counter(), 6)

            # Write some more events.
            aggregate = Aggregate()
            aggregate.trigger_event(event_class=Aggregate.Event)
            aggregate.trigger_event(event_class=Aggregate.Event)
            recordings = write_model.save(aggregate)

            # Wait for events to be processed.
            read_model.wait(
                application_name=write_model.name,
                notification_id=recordings[-1].notification.id,
                timeout=5,
            )

            # Query the read model.
            self.assertEqual(read_model.get_created_event_counter(), 4)
            self.assertEqual(read_model.get_subsequent_event_counter(), 8)

    def test_run_forever_raises_projection_error(self) -> None:
        super().test_run_forever_raises_projection_error()

        # Resume...
        with ProjectionRunner(
            application_class=Application,
            projection_class=EventCountersProjection,
            view_class=self.view_class,
            env=self.env,
        ) as runner:

            # Construct separate instance of "write model".
            write_model = Application(self.env)

            # Construct separate instance of "read model".
            read_model = InfrastructureFactory.construct(
                env=Environment(name=EventCountersProjection.name, env=self.env)
            ).tracking_recorder(self.view_class)

            # Still terminates with projection error.
            with self.assertRaises(SpannerThrownError):
                runner.run_forever(timeout=5)

            # Wait times out (event has not been processed).
            with self.assertRaises(TimeoutError):
                read_model.wait(
                    application_name=write_model.name,
                    notification_id=write_model.recorder.max_notification_id(),
                    timeout=1,
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
        drop_postgres_table(datastore, "eventcounters_tracking")
        drop_postgres_table(datastore, "eventcounters")


del EventCountersViewTestCase
