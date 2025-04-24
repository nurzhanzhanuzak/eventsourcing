from __future__ import annotations

import sys
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from threading import Event, Thread
from time import sleep
from typing import TYPE_CHECKING
from unittest import TestCase, skipIf
from unittest.mock import Mock
from uuid import uuid4

import psycopg
from psycopg import Connection
from psycopg.sql import SQL
from psycopg_pool import ConnectionPool

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    PersistenceError,
    ProcessRecorder,
    ProgrammingError,
    StoredEvent,
    Tracking,
    TrackingRecorder,
)
from eventsourcing.postgres import (
    PostgresAggregateRecorder,
    PostgresApplicationRecorder,
    PostgresDatastore,
    PostgresFactory,
    PostgresProcessRecorder,
    PostgresSubscription,
    PostgresTrackingRecorder,
)
from eventsourcing.tests.persistence import (
    AggregateRecorderTestCase,
    ApplicationRecorderTestCase,
    InfrastructureFactoryTestCase,
    ProcessRecorderTestCase,
    TrackingRecorderTestCase,
)
from eventsourcing.tests.postgres_utils import (
    drop_postgres_table,
    pg_close_all_connections,
)
from eventsourcing.utils import Environment, get_topic
from tests.persistence_tests.test_connection_pool import TestConnectionPool

if TYPE_CHECKING:
    from collections.abc import Iterator


class TestPostgresDatastore(TestCase):
    def test_is_pipeline_supported(self) -> None:
        self.assertTrue(psycopg.Pipeline.is_supported())

    def test_has_connection_pool(self) -> None:
        with PostgresDatastore(
            dbname="eventsourcing",
            host="127.0.0.1",
            port="5432",
            user="eventsourcing",
            password="eventsourcing",  # noqa: S106
        ) as datastore:
            self.assertIsInstance(datastore.pool, ConnectionPool)

    def test_get_connection(self) -> None:
        with (
            PostgresDatastore(
                dbname="eventsourcing",
                host="127.0.0.1",
                port="5432",
                user="eventsourcing",
                password="eventsourcing",  # noqa: S106
            ) as datastore,
            datastore.get_connection() as conn,
        ):
            self.assertIsInstance(conn, Connection)

    def test_context_manager_converts_exceptions_and_conditionally_calls_close(
        self,
    ) -> None:
        cases: list[tuple[type[Exception], Exception | type[Exception], bool]] = [
            (InterfaceError, psycopg.InterfaceError(), True),
            (DataError, psycopg.DataError(), False),
            (OperationalError, psycopg.OperationalError(), True),
            (IntegrityError, psycopg.IntegrityError(), False),
            (InternalError, psycopg.InternalError(), False),
            (ProgrammingError, psycopg.ProgrammingError(), False),
            (NotSupportedError, psycopg.NotSupportedError(), False),
            (DatabaseError, psycopg.DatabaseError(), False),
            (PersistenceError, psycopg.Error(), True),
            (TypeError, TypeError(), True),
            (TypeError, TypeError, True),
        ]
        with PostgresDatastore(
            dbname="eventsourcing",
            host="127.0.0.1",
            port="5432",
            user="eventsourcing",
            password="eventsourcing",  # noqa: S106
        ) as datastore:
            for expected_exc_type, raised_exc, expect_conn_closed in cases:
                with self.assertRaises(expected_exc_type):
                    with datastore.get_connection() as conn:
                        self.assertFalse(conn.closed)
                        raise raised_exc
                    self.assertTrue(conn.closed is expect_conn_closed, raised_exc)

    def test_transaction_from_datastore(self) -> None:
        with (
            PostgresDatastore(
                dbname="eventsourcing",
                host="127.0.0.1",
                port="5432",
                user="eventsourcing",
                password="eventsourcing",  # noqa: S106
            ) as datastore,
            datastore.transaction(commit=False) as curs,
        ):
            # As a convenience, we can use the transaction() method.
            curs.execute("SELECT 1")
            self.assertEqual(curs.fetchall(), [{"?column?": 1}])

    def test_connect_failure_raises_operational_error(self) -> None:
        datastore = PostgresDatastore(
            dbname="eventsourcing",
            host="127.0.0.1",
            port="4321",  # wrong port
            user="eventsourcing",
            password="eventsourcing",  # noqa: S106
            pool_open_timeout=2,
        )
        with self.assertRaises(OperationalError), datastore.get_connection():
            pass

        with (
            PostgresDatastore(
                dbname="eventsourcing",
                host="127.0.0.1",
                port="987654321",  # bad value
                user="eventsourcing",
                password="eventsourcing",  # noqa: S106
                pool_open_timeout=2,
            ) as datastore,
            self.assertRaises(OperationalError),
            datastore.get_connection(),
        ):
            pass

    @skipIf(
        sys.version_info[:2] < (3, 8),
        "The 'check' argument and the check_connection() method aren't supported.",
    )
    def test_pre_ping(self) -> None:
        # Define method to open and close a connection, and then execute a statement.
        def open_close_execute(*, pre_ping: bool) -> None:
            with PostgresDatastore(
                dbname="eventsourcing",
                host="127.0.0.1",
                port="5432",
                user="eventsourcing",
                password="eventsourcing",  # noqa: S106
                pool_size=1,
                pre_ping=pre_ping,
            ) as datastore:

                # Create a connection.
                with datastore.get_connection() as conn, conn.cursor() as curs:
                    curs.execute("SELECT 1")
                    self.assertEqual(curs.fetchall(), [{"?column?": 1}])

                # Close all connections via separate connection.
                pg_close_all_connections()

                # Check the connection doesn't think it's closed.
                self.assertTrue(datastore.pool._pool)
                self.assertFalse(datastore.pool._pool[0].closed)

                # Get a closed connection.
                with datastore.get_connection() as conn:
                    self.assertFalse(conn.closed)

                    with conn.cursor() as curs:
                        curs.execute("SELECT 1")

        # Check using the closed connection gives an error.
        with self.assertRaises(OperationalError):
            open_close_execute(pre_ping=False)

        # Now try that again with pre-ping enabled.
        open_close_execute(pre_ping=True)

    def test_idle_in_transaction_session_timeout(self) -> None:
        with PostgresDatastore(
            dbname="eventsourcing",
            host="127.0.0.1",
            port="5432",
            user="eventsourcing",
            password="eventsourcing",  # noqa: S106
            idle_in_transaction_session_timeout=1,
        ) as datastore:

            # Error on commit is raised.
            with self.assertRaises(InternalError), datastore.get_connection() as curs:
                curs.execute("BEGIN")
                curs.execute("SELECT 1")
                self.assertFalse(curs.closed)
                sleep(2)

            # Error on commit is raised.
            with (
                self.assertRaises(InternalError),
                datastore.transaction(commit=True) as curs,
            ):
                # curs.execute("BEGIN")
                curs.execute("SELECT 1")
                self.assertFalse(curs.closed)
                sleep(2)

            # Force rollback. Error is ignored.
            with datastore.transaction(commit=False) as curs:
                # curs.execute("BEGIN")
                curs.execute("SELECT 1")
                self.assertFalse(curs.closed)
                sleep(2)

            # Autocommit mode - transaction is commited in time.
            with datastore.get_connection() as curs:
                curs.execute("SELECT 1")
                self.assertFalse(curs.closed)
                sleep(2)

    def test_get_password_func(self) -> None:
        # Check correct password is required, wrong password causes operational error.
        with (
            PostgresDatastore(
                dbname="eventsourcing",
                host="127.0.0.1",
                port="5432",
                user="eventsourcing",
                password="wrong",  # noqa: S106
                pool_size=1,
                connect_timeout=3,
            ) as datastore,
            self.assertRaises(OperationalError),
            datastore.get_connection() as conn,
            conn.cursor() as curs,
        ):
            curs.execute("SELECT 1")

        # Define a "get password" function, with a generator that returns
        # wrong password a few times first.
        def password_token_generator() -> Iterator[str]:
            yield "wrong"
            yield "wrong"
            yield "eventsourcing"

        password_generator = password_token_generator()

        def get_password_func() -> str:
            return next(password_generator)

        # Construct datastore with "get password" function.
        with (
            PostgresDatastore(
                dbname="eventsourcing",
                host="127.0.0.1",
                port="5432",
                user="eventsourcing",
                password="",
                pool_size=1,
                get_password_func=get_password_func,
                connect_timeout=3,
            ) as datastore,
            datastore.get_connection() as conn,
            conn.cursor() as curs,
        ):
            # Create a connection, and check it works (this test depends on psycopg
            # retrying attempt to connect, should call "get password" twice).
            curs.execute("SELECT 1")
            self.assertEqual(curs.fetchall(), [{"?column?": 1}])


MAX_IDENTIFIER_LEN = 63


def _check_identifier_is_max_len(identifier: str) -> None:
    if len(identifier) != MAX_IDENTIFIER_LEN:
        msg = "Expected length of name string to be max identifier length"
        raise ValueError(msg)


# Use maximally long identifier for table name.
EVENTS_TABLE_NAME = "s" * 50 + "stored_events"
_check_identifier_is_max_len(EVENTS_TABLE_NAME)


class SetupPostgresDatastore(TestCase):
    schema = "public"
    pool_size = 1
    max_overflow = 0
    max_waiting = 0

    def setUp(self) -> None:
        super().setUp()
        self.datastore = PostgresDatastore(
            "eventsourcing",
            "127.0.0.1",
            "5432",
            "eventsourcing",
            "eventsourcing",
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            max_waiting=self.max_waiting,
            schema=self.schema,
        )
        self.drop_tables()

    def tearDown(self) -> None:
        super().tearDown()
        self.drop_tables()
        self.datastore.close()

    def drop_tables(self) -> None:
        drop_postgres_table(self.datastore, EVENTS_TABLE_NAME)


class WithSchema(SetupPostgresDatastore):
    schema = "myschema"

    def test_datastore_has_schema(self) -> None:
        self.assertEqual(self.datastore.schema, self.schema)


class TestPostgresAggregateRecorder(SetupPostgresDatastore, AggregateRecorderTestCase):
    def create_recorder(self, table_name: str = EVENTS_TABLE_NAME) -> AggregateRecorder:
        recorder = PostgresAggregateRecorder(
            datastore=self.datastore, events_table_name=table_name
        )
        recorder.create_table()
        return recorder

    def drop_tables(self) -> None:
        super().drop_tables()
        drop_postgres_table(self.datastore, "stored_events")

    def test_create_table(self) -> None:
        recorder = PostgresAggregateRecorder(
            datastore=self.datastore, events_table_name="stored_events"
        )
        recorder.create_table()

    def test_insert_and_select(self) -> None:
        super().test_insert_and_select()

    def test_performance(self) -> None:
        super().test_performance()

    def test_retry_insert_events_after_closing_connection(self) -> None:
        # This checks connection is recreated after connections are closed.
        self.datastore.pool.resize(1, 1)

        # Construct the recorder.
        recorder = self.create_recorder()

        # Check we have open connections.
        self.assertTrue(self.datastore.pool._pool)

        # Close connections.
        pg_close_all_connections()
        self.assertFalse(self.datastore.pool._pool[0].closed)

        # Write a stored event.
        stored_event1 = StoredEvent(
            originator_id=uuid4(),
            originator_version=0,
            topic="topic1",
            state=b"state1",
        )
        recorder.insert_events([stored_event1])


class TestPostgresAggregateRecorderWithSchema(
    WithSchema, TestPostgresAggregateRecorder
):
    pass


class TestPostgresAggregateRecorderErrors(SetupPostgresDatastore, TestCase):
    def create_recorder(
        self, table_name: str = EVENTS_TABLE_NAME
    ) -> PostgresAggregateRecorder:
        return PostgresAggregateRecorder(
            datastore=self.datastore, events_table_name=table_name
        )

    def test_excessively_long_table_name_raises_error(self) -> None:
        # Add one more character to the table name.
        long_table_name = "s" + EVENTS_TABLE_NAME
        self.assertEqual(len(long_table_name), 64)
        with self.assertRaises(ProgrammingError):
            self.create_recorder(long_table_name)

    def test_create_table_raises_programming_error_when_sql_is_broken(self) -> None:
        recorder = self.create_recorder()

        # Mess up the statement.
        recorder.create_table_statements = [SQL("BLAH").format()]
        with self.assertRaises(ProgrammingError):
            recorder.create_table()

    def test_insert_events_raises_programming_error_when_table_not_created(
        self,
    ) -> None:
        # Construct the recorder.
        recorder = self.create_recorder()

        # Write a stored event without creating the table.
        stored_event1 = StoredEvent(
            originator_id=uuid4(),
            originator_version=0,
            topic="topic1",
            state=b"state1",
        )
        with self.assertRaises(ProgrammingError):
            recorder.insert_events([stored_event1])

    def test_insert_events_raises_programming_error_when_sql_is_broken(self) -> None:
        # Construct the recorder.
        recorder = self.create_recorder()

        # Create the table.
        recorder.create_table()

        # Write a stored event with broken statement.
        recorder.insert_events_statement = SQL("BLAH").format()
        stored_event1 = StoredEvent(
            originator_id=uuid4(),
            originator_version=0,
            topic="topic1",
            state=b"state1",
        )
        with self.assertRaises(ProgrammingError):
            recorder.insert_events([stored_event1])

    def test_select_events_raises_programming_error_when_table_not_created(
        self,
    ) -> None:
        # Construct the recorder.
        recorder = self.create_recorder()

        # Select events without creating the table.
        originator_id = uuid4()
        with self.assertRaises(ProgrammingError):
            recorder.select_events(originator_id=originator_id)

    def test_select_events_raises_programming_error_when_sql_is_broken(self) -> None:
        # Construct the recorder.
        recorder = self.create_recorder()

        # Create the table.
        recorder.create_table()

        # Select events with broken statement.
        recorder.select_events_statement = SQL("BLAH").format()
        originator_id = uuid4()
        with self.assertRaises(ProgrammingError):
            recorder.select_events(originator_id=originator_id)


class TestPostgresSubscription(TestCase):
    def test_listen_catches_error(self) -> None:
        mock_recorder = Mock(spec=PostgresApplicationRecorder)

        subscription = PostgresSubscription(mock_recorder, 0)

        subscription._thread_error = None

        subscription._listen()
        self.assertIsInstance(subscription._thread_error, AttributeError)

        # Check _listen_for_notifications() preserves first error.
        subscription = PostgresSubscription(mock_recorder, 0)
        subscription._thread_error = ValueError()
        subscription._listen()
        self.assertIsInstance(subscription._thread_error, ValueError)


class TestPostgresApplicationRecorder(
    SetupPostgresDatastore, ApplicationRecorderTestCase[PostgresApplicationRecorder]
):
    def create_recorder(
        self, table_name: str = EVENTS_TABLE_NAME
    ) -> PostgresApplicationRecorder:
        recorder = PostgresApplicationRecorder(
            self.datastore, events_table_name=table_name
        )
        recorder.create_table()
        return recorder

    def test_insert_select(self) -> None:
        super().test_insert_select()

    def test_insert_subscribe(self) -> None:
        self.datastore.pool.resize(2, 2)
        super().optional_test_insert_subscribe()

    def test_subscribe_concurrent_reading_and_writing(self) -> None:
        self.datastore.pool.resize(2, 2)
        recorder = self.create_recorder()

        num_batches = 20
        batch_size = 100
        num_events = num_batches * batch_size

        def read(last_notification_id: int) -> None:
            start = datetime.now()
            with recorder.subscribe(last_notification_id) as subscription:
                for i, notification in enumerate(subscription):
                    # print("Read", i+1, "notifications")
                    last_notification_id = notification.id
                    if i + 1 == num_events:
                        break
            duration = datetime.now() - start
            print(
                "Finished reading",
                num_events,
                "events in",
                duration.total_seconds(),
                "seconds",
            )

        def write() -> None:
            start = datetime.now()
            for _ in range(num_batches):
                events = []
                for _ in range(batch_size):
                    stored_event = StoredEvent(
                        originator_id=uuid4(),
                        originator_version=self.INITIAL_VERSION,
                        topic="topic1",
                        state=b"state1",
                    )
                    events.append(stored_event)
                recorder.insert_events(events)
                # print("Wrote", i + 1, "notifications")
            duration = datetime.now() - start
            print(
                "Finished writing",
                num_events,
                "events in",
                duration.total_seconds(),
                "seconds",
            )

        thread_pool = ThreadPoolExecutor(max_workers=2)

        print("Concurrent...")
        # Get the max notification ID (for the subscription).
        last_notification_id = recorder.max_notification_id()
        write_job = thread_pool.submit(write)
        read_job = thread_pool.submit(read, last_notification_id)
        write_job.result()
        read_job.result()

        print("Sequential...")
        last_notification_id = recorder.max_notification_id()
        write_job = thread_pool.submit(write)
        write_job.result()
        read_job = thread_pool.submit(read, last_notification_id)
        read_job.result()

        thread_pool.shutdown()

    def test_concurrent_no_conflicts(self) -> None:
        self.datastore.pool.resize(12, 12)
        super().test_concurrent_no_conflicts()

    def test_concurrent_throughput(self) -> None:
        self.datastore.pool.resize(10, 10)
        super().test_concurrent_throughput()

    def test_retry_select_notifications_after_closing_connection(self) -> None:
        # This checks connection is recreated after InterfaceError.

        # Construct the recorder.
        recorder = self.create_recorder()

        self.assertEqual(len(self.datastore.pool._pool), 1)
        self.assertFalse(self.datastore.pool._pool[0].closed)

        # Write a stored event.
        originator_id = uuid4()
        stored_event1 = StoredEvent(
            originator_id=originator_id,
            originator_version=0,
            topic="topic1",
            state=b"state1",
        )
        recorder.insert_events([stored_event1])

        # Close connections.
        pg_close_all_connections()

        self.assertEqual(len(self.datastore.pool._pool), 1)
        self.assertFalse(self.datastore.pool._pool[0].closed)
        conn_id_before = id(self.datastore.pool._pool[0])

        # Select events.
        recorder.select_notifications(start=1, limit=1)

        self.assertEqual(len(self.datastore.pool._pool), 1)
        self.assertFalse(self.datastore.pool._pool[0].closed)
        conn_id_after = id(self.datastore.pool._pool[0])
        self.assertNotEqual(conn_id_before, conn_id_after)

    def test_retry_max_notification_id_after_closing_connection(self) -> None:
        # This checks connection is recreated after InterfaceError.

        # Construct the recorder.
        recorder = self.create_recorder()

        self.assertEqual(len(self.datastore.pool._pool), 1)
        self.assertFalse(self.datastore.pool._pool[0].closed)
        conn_id_before = id(self.datastore.pool._pool[0])

        # Write a stored event.
        originator_id = uuid4()
        stored_event1 = StoredEvent(
            originator_id=originator_id,
            originator_version=0,
            topic="topic1",
            state=b"state1",
        )
        recorder.insert_events([stored_event1])

        # Close connections.
        pg_close_all_connections()

        # Get max notification ID.
        recorder.max_notification_id()

        self.assertEqual(len(self.datastore.pool._pool), 1)
        self.assertFalse(self.datastore.pool._pool[0].closed)
        conn_id_after = id(self.datastore.pool._pool[0])
        self.assertNotEqual(conn_id_before, conn_id_after)

    def test_insert_lock_timeout_actually_works(self) -> None:
        self.datastore.lock_timeout = 1
        self.datastore.pool.resize(2, 2)
        recorder = self.create_recorder()

        stored_event1 = StoredEvent(
            originator_id=uuid4(),
            originator_version=1,
            topic="topic1",
            state=b"state1",
        )
        stored_event2 = StoredEvent(
            originator_id=uuid4(),
            originator_version=1,
            topic="topic1",
            state=b"state1",
        )

        table_lock_acquired = Event()
        test_ended = Event()
        table_lock_timed_out = Event()

        def insert1() -> None:
            with self.datastore.transaction() as curs:
                # Lock table.
                recorder._insert_stored_events(curs, [stored_event1])
                table_lock_acquired.set()
                # Wait for other thread to timeout.
                test_ended.wait(timeout=5)  # keep the lock

        def insert2() -> None:
            try:
                with self.datastore.get_connection() as conn:
                    # Wait for other thread to lock table.
                    table_lock_acquired.wait(timeout=5)
                    # Expect to timeout.
                    with conn.transaction(), conn.cursor() as curs:
                        recorder._insert_stored_events(curs, [stored_event2])
            except OperationalError as e:
                if "lock timeout" in e.args[0]:
                    table_lock_timed_out.set()

        thread1 = Thread(target=insert1, daemon=True)
        thread1.start()
        thread2 = Thread(target=insert2, daemon=True)
        thread2.start()

        table_lock_timed_out.wait(timeout=4)
        test_ended.set()

        thread1.join(timeout=10)
        thread2.join(timeout=10)

        self.assertTrue(table_lock_timed_out.is_set())


class TestPostgresApplicationRecorderWithSchema(
    WithSchema, TestPostgresApplicationRecorder
):
    pass


class TestPostgresApplicationRecorderErrors(SetupPostgresDatastore, TestCase):
    def create_recorder(
        self, table_name: str = EVENTS_TABLE_NAME
    ) -> ApplicationRecorder:
        return PostgresApplicationRecorder(self.datastore, events_table_name=table_name)

    def test_excessively_long_table_name_raises_error(self) -> None:
        # Add one more character to the table name.
        long_table_name = "s" + EVENTS_TABLE_NAME
        self.assertEqual(len(long_table_name), 64)
        with self.assertRaises(ProgrammingError):
            self.create_recorder(long_table_name)

    def test_select_notification_raises_programming_error_when_table_not_created(
        self,
    ) -> None:
        # Construct the recorder.
        recorder = self.create_recorder()

        # Select notifications without creating table.
        with self.assertRaises(ProgrammingError):
            recorder.select_notifications(start=1, limit=1)

    def test_max_notification_id_raises_programming_error_when_table_not_created(
        self,
    ) -> None:
        # Construct the recorder.
        recorder = PostgresApplicationRecorder(
            datastore=self.datastore, events_table_name=EVENTS_TABLE_NAME
        )

        # Select notifications without creating table.
        with self.assertRaises(ProgrammingError):
            recorder.max_notification_id()

    def test_fetch_ids_after_insert_events(self) -> None:
        def make_events() -> list[StoredEvent]:
            return [
                StoredEvent(
                    originator_id=uuid4(),
                    originator_version=1,
                    state=b"",
                    topic="",
                )
            ]

        # Check it actually works.
        recorder = PostgresApplicationRecorder(
            datastore=self.datastore, events_table_name=EVENTS_TABLE_NAME
        )
        recorder.create_table()
        notification_ids = recorder.insert_events(make_events())
        self.assertEqual(len(notification_ids), 1)
        self.assertEqual(1, notification_ids[0])

        # Insert statement has no RETURNING clause.
        with self.assertRaises(ProgrammingError):
            recorder = PostgresApplicationRecorder(
                datastore=self.datastore, events_table_name=EVENTS_TABLE_NAME
            )
            original = recorder.insert_events_statement
            without_returning = original.as_string().partition("RETURNING")[0]
            recorder.insert_events_statement = SQL(
                without_returning  # pyright: ignore
            ).format()
            recorder.create_table()
            recorder.insert_events(make_events())


TRACKING_TABLE_NAME = "n" * 42 + "notification_tracking"
_check_identifier_is_max_len(TRACKING_TABLE_NAME)


class TestPostgresTrackingRecorder(SetupPostgresDatastore, TrackingRecorderTestCase):
    def drop_tables(self) -> None:
        super().drop_tables()
        drop_postgres_table(self.datastore, TRACKING_TABLE_NAME)

    def create_recorder(self) -> TrackingRecorder:
        tracking_table_name = TRACKING_TABLE_NAME
        recorder = PostgresTrackingRecorder(
            datastore=self.datastore,
            tracking_table_name=tracking_table_name,
        )
        recorder.create_table()
        return recorder

    def test_insert_tracking(self) -> None:
        super().test_insert_tracking()

    def test_excessively_long_table_names_raise_error(self) -> None:
        with self.assertRaises(ProgrammingError):
            PostgresProcessRecorder(
                datastore=self.datastore,
                events_table_name=EVENTS_TABLE_NAME,
                tracking_table_name="n" + TRACKING_TABLE_NAME,
            )


class TestPostgresProcessRecorder(SetupPostgresDatastore, ProcessRecorderTestCase):
    def drop_tables(self) -> None:
        super().drop_tables()
        drop_postgres_table(self.datastore, TRACKING_TABLE_NAME)

    def create_recorder(self) -> ProcessRecorder:
        events_table_name = EVENTS_TABLE_NAME
        tracking_table_name = TRACKING_TABLE_NAME
        recorder = PostgresProcessRecorder(
            datastore=self.datastore,
            events_table_name=events_table_name,
            tracking_table_name=tracking_table_name,
        )
        recorder.create_table()
        return recorder

    def test_performance(self) -> None:
        super().test_performance()

    def test_excessively_long_table_names_raise_error(self) -> None:
        with self.assertRaises(ProgrammingError):
            PostgresProcessRecorder(
                self.datastore,
                events_table_name="e" + EVENTS_TABLE_NAME,
                tracking_table_name=TRACKING_TABLE_NAME,
            )

        with self.assertRaises(ProgrammingError):
            PostgresProcessRecorder(
                datastore=self.datastore,
                events_table_name=EVENTS_TABLE_NAME,
                tracking_table_name="n" + TRACKING_TABLE_NAME,
            )

    def test_retry_max_tracking_id_after_closing_connection(self) -> None:
        # This checks connection is recreated after InterfaceError.

        # Construct the recorder.
        recorder = self.create_recorder()

        self.assertEqual(len(self.datastore.pool._pool), 1)
        self.assertFalse(self.datastore.pool._pool[0].closed)

        # Write a tracking record.
        originator_id = uuid4()
        stored_event1 = StoredEvent(
            originator_id=originator_id,
            originator_version=0,
            topic="topic1",
            state=b"state1",
        )
        recorder.insert_events([stored_event1], tracking=Tracking("upstream", 1))

        # Close connections.
        pg_close_all_connections()

        self.assertEqual(len(self.datastore.pool._pool), 1)
        self.assertFalse(self.datastore.pool._pool[0].closed)

        conn_id_before = id(self.datastore.pool._pool[0])

        # Get max tracking ID.
        notification_id = recorder.max_tracking_id("upstream")
        self.assertEqual(notification_id, 1)

        # Check the connection has been replaced.
        conn_id_after = id(self.datastore.pool._pool[0])
        self.assertNotEqual(conn_id_before, conn_id_after)


class TestPostgresProcessRecorderWithSchema(WithSchema, TestPostgresProcessRecorder):
    pass


class TestPostgresProcessRecorderErrors(SetupPostgresDatastore, TestCase):
    def drop_tables(self) -> None:
        super().drop_tables()
        drop_postgres_table(self.datastore, TRACKING_TABLE_NAME)

    def create_recorder(self) -> PostgresProcessRecorder:
        return PostgresProcessRecorder(
            datastore=self.datastore,
            events_table_name=EVENTS_TABLE_NAME,
            tracking_table_name=TRACKING_TABLE_NAME,
        )

    def test_max_tracking_id_raises_programming_error_when_table_not_created(
        self,
    ) -> None:
        # Construct the recorder.
        recorder = self.create_recorder()

        # Get max tracking ID without creating table.
        with self.assertRaises(ProgrammingError):
            recorder.max_tracking_id("upstream")


class TestPostgresInfrastructureFactory(InfrastructureFactoryTestCase[PostgresFactory]):
    def test_create_application_recorder(self) -> None:
        super().test_create_application_recorder()

    def expected_factory_class(self) -> type[PostgresFactory]:
        return PostgresFactory

    def expected_aggregate_recorder_class(self) -> type[AggregateRecorder]:
        return PostgresAggregateRecorder

    def expected_application_recorder_class(self) -> type[ApplicationRecorder]:
        return PostgresApplicationRecorder

    def expected_tracking_recorder_class(self) -> type[TrackingRecorder]:
        return PostgresTrackingRecorder

    class PostgresTrackingRecorderSubclass(PostgresTrackingRecorder):
        pass

    def tracking_recorder_subclass(self) -> type[TrackingRecorder]:
        return self.PostgresTrackingRecorderSubclass

    def test_create_tracking_recorder(self) -> None:
        super().test_create_tracking_recorder()
        self.factory.datastore.schema = "myschema"
        recorder = self.factory.tracking_recorder()
        self.assertIn('"myschema".', recorder.create_table_statements[0].as_string())

    def expected_process_recorder_class(self) -> type[ProcessRecorder]:
        return PostgresProcessRecorder

    def setUp(self) -> None:
        self.env = Environment("TestCase")
        self.env[PostgresFactory.PERSISTENCE_MODULE] = PostgresFactory.__module__
        self.env[PostgresFactory.POSTGRES_DBNAME] = "eventsourcing"
        self.env[PostgresFactory.POSTGRES_HOST] = "127.0.0.1"
        self.env[PostgresFactory.POSTGRES_PORT] = "5432"
        self.env[PostgresFactory.POSTGRES_USER] = "eventsourcing"
        self.env[PostgresFactory.POSTGRES_PASSWORD] = "eventsourcing"
        self.drop_tables()
        super().setUp()

    def tearDown(self) -> None:
        self.drop_tables()
        super().tearDown()

    def drop_tables(self) -> None:
        with PostgresDatastore(
            "eventsourcing",
            "127.0.0.1",
            "5432",
            "eventsourcing",
            "eventsourcing",
        ) as datastore:
            drop_postgres_table(datastore, "testcase_events")
            drop_postgres_table(datastore, "testcase_snapshots")
            drop_postgres_table(datastore, "testcase_tracking")
            datastore.schema = "myschema"
            drop_postgres_table(datastore, "testcase_events")
            drop_postgres_table(datastore, "testcase_snapshots")
            drop_postgres_table(datastore, "testcase_tracking")

    def test_close(self) -> None:
        factory = PostgresFactory(self.env)
        with factory.datastore.get_connection() as conn:
            conn.execute("SELECT 1")
        self.assertFalse(factory.datastore.pool.closed)
        factory.close()
        self.assertTrue(factory.datastore.pool.closed)

    def test_conn_max_age_is_set_to_float(self) -> None:
        self.env[PostgresFactory.POSTGRES_CONN_MAX_AGE] = ""
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.max_lifetime, 60 * 60.0)

    def test_conn_max_age_is_set_to_number(self) -> None:
        self.env[PostgresFactory.POSTGRES_CONN_MAX_AGE] = "0"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.max_lifetime, 0)

    def test_pool_size_is_five_by_default(self) -> None:
        self.assertTrue(PostgresFactory.POSTGRES_POOL_SIZE not in self.env)
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.min_size, 5)

        self.env[PostgresFactory.POSTGRES_POOL_SIZE] = ""
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.min_size, 5)

    def test_max_overflow_is_ten_by_default(self) -> None:
        self.assertTrue(PostgresFactory.POSTGRES_MAX_OVERFLOW not in self.env)
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.max_size, 15)

        self.env[PostgresFactory.POSTGRES_MAX_OVERFLOW] = ""
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.max_size, 15)

    def test_max_overflow_is_set(self) -> None:
        self.env[PostgresFactory.POSTGRES_MAX_OVERFLOW] = "7"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.max_size, 12)

    def test_pool_size_is_set(self) -> None:
        self.env[PostgresFactory.POSTGRES_POOL_SIZE] = "6"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.min_size, 6)

    def test_connect_timeout_is_thirty_by_default(self) -> None:
        self.assertTrue(PostgresFactory.POSTGRES_CONNECT_TIMEOUT not in self.env)
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.timeout, 30)

        self.env[PostgresFactory.POSTGRES_CONNECT_TIMEOUT] = ""
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.timeout, 30)

    def test_connect_timeout_is_set(self) -> None:
        self.env[PostgresFactory.POSTGRES_CONNECT_TIMEOUT] = "8"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.timeout, 8)

    def test_max_waiting_is_0_by_default(self) -> None:
        self.assertTrue(PostgresFactory.POSTGRES_MAX_WAITING not in self.env)
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.max_waiting, 0)

        self.env[PostgresFactory.POSTGRES_MAX_WAITING] = ""
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.max_waiting, 0)

    def test_max_waiting_is_set(self) -> None:
        self.env[PostgresFactory.POSTGRES_MAX_WAITING] = "8"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.max_waiting, 8)

    def test_lock_timeout_is_zero_by_default(self) -> None:
        self.assertTrue(PostgresFactory.POSTGRES_LOCK_TIMEOUT not in self.env)
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.lock_timeout, 0)

        self.env[PostgresFactory.POSTGRES_LOCK_TIMEOUT] = ""
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.lock_timeout, 0)

    def test_lock_timeout_is_set(self) -> None:
        self.env[PostgresFactory.POSTGRES_LOCK_TIMEOUT] = "1"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.lock_timeout, 1)

    def test_idle_in_transaction_session_timeout_is_5_by_default(self) -> None:
        self.assertTrue(
            PostgresFactory.POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT not in self.env
        )
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.idle_in_transaction_session_timeout, 5)
        factory.close()

        self.env[PostgresFactory.POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT] = ""
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.idle_in_transaction_session_timeout, 5)

    def test_idle_in_transaction_session_timeout_is_set(self) -> None:
        self.env[PostgresFactory.POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT] = "10"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.idle_in_transaction_session_timeout, 10)

    def test_pre_ping_off_by_default(self) -> None:
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pre_ping, False)

    def test_pre_ping_off(self) -> None:
        self.env[PostgresFactory.POSTGRES_PRE_PING] = "off"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pre_ping, False)

    def test_pre_ping_on(self) -> None:
        self.env[PostgresFactory.POSTGRES_PRE_PING] = "on"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pre_ping, True)

    def test_get_password_topic_not_set(self) -> None:
        factory = PostgresFactory(self.env)
        self.assertIsNone(factory.datastore.pool.get_password_func, None)

    def test_get_password_topic_set(self) -> None:
        def get_password_func() -> str:
            return "eventsourcing"

        self.env[PostgresFactory.POSTGRES_GET_PASSWORD_TOPIC] = get_topic(
            get_password_func
        )
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.pool.get_password_func, get_password_func)

    def test_environment_error_raised_when_conn_max_age_not_a_float(self) -> None:
        self.env[PostgresFactory.POSTGRES_CONN_MAX_AGE] = "abc"
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres environment value for key 'POSTGRES_CONN_MAX_AGE' "
            "is invalid. If set, a float or empty string is expected: 'abc'",
        )

    def test_environment_error_raised_when_connect_timeout_not_an_integer(self) -> None:
        self.env[PostgresFactory.POSTGRES_CONNECT_TIMEOUT] = "abc"
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres environment value for key 'POSTGRES_CONNECT_TIMEOUT' "
            "is invalid. If set, an integer or empty string is expected: 'abc'",
        )

    def test_environment_error_raised_when_max_waiting_not_an_integer(self) -> None:
        self.env[PostgresFactory.POSTGRES_MAX_WAITING] = "abc"
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres environment value for key 'POSTGRES_MAX_WAITING' "
            "is invalid. If set, an integer or empty string is expected: 'abc'",
        )

    def test_environment_error_raised_when_lock_timeout_not_an_integer(self) -> None:
        self.env[PostgresFactory.POSTGRES_LOCK_TIMEOUT] = "abc"
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres environment value for key 'POSTGRES_LOCK_TIMEOUT' "
            "is invalid. If set, an integer or empty string is expected: 'abc'",
        )

    def test_environment_error_raised_when_min_conn_not_an_integer(self) -> None:
        self.env[PostgresFactory.POSTGRES_POOL_SIZE] = "abc"
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres environment value for key 'POSTGRES_POOL_SIZE' "
            "is invalid. If set, an integer or empty string is expected: 'abc'",
        )

    def test_environment_error_raised_when_max_conn_not_an_integer(self) -> None:
        self.env[PostgresFactory.POSTGRES_MAX_OVERFLOW] = "abc"
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres environment value for key 'POSTGRES_MAX_OVERFLOW' "
            "is invalid. If set, an integer or empty string is expected: 'abc'",
        )

    def test_environment_error_raised_when_idle_in_transaction_session_timeout_not_int(
        self,
    ) -> None:
        self.env[PostgresFactory.POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT] = "abc"
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres environment value for key "
            "'POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT' "
            "is invalid. If set, an integer or empty string is expected: 'abc'",
        )

    def test_environment_error_raised_when_dbname_missing(self) -> None:
        del self.env[PostgresFactory.POSTGRES_DBNAME]
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory.construct(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres database name not found in environment "
            "with key 'POSTGRES_DBNAME'",
        )

    def test_environment_error_raised_when_dbhost_missing(self) -> None:
        del self.env[PostgresFactory.POSTGRES_HOST]
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory.construct(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres host not found in environment with key 'POSTGRES_HOST'",
        )

    def test_environment_error_raised_when_user_missing(self) -> None:
        del self.env[PostgresFactory.POSTGRES_USER]
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory.construct(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres user not found in environment with key 'POSTGRES_USER'",
        )

    def test_environment_error_raised_when_password_missing(self) -> None:
        del self.env[PostgresFactory.POSTGRES_PASSWORD]
        with self.assertRaises(EnvironmentError) as cm:
            PostgresFactory.construct(self.env)
        self.assertEqual(
            cm.exception.args[0],
            "Postgres password not found in environment with key 'POSTGRES_PASSWORD'",
        )

    def test_schema_set_to_empty_string(self) -> None:
        self.env[PostgresFactory.POSTGRES_SCHEMA] = ""
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.schema, "public")

    def test_schema_set_to_whitespace(self) -> None:
        self.env[PostgresFactory.POSTGRES_SCHEMA] = " "
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.schema, "public")

    def test_scheme_adjusts_table_names_on_aggregate_recorder(self) -> None:
        factory = PostgresFactory(self.env)

        # Check by default the table name is not qualified.
        recorder = factory.aggregate_recorder("events")
        assert isinstance(recorder, PostgresAggregateRecorder)
        self.assertEqual(recorder.events_table_name, "testcase_events")
        self.assertIn(
            '"public"."testcase_events"',
            recorder.create_table_statements[0].as_string(),
        )

        # Check by default the table name is not qualified.
        recorder = factory.aggregate_recorder("snapshots")
        assert isinstance(recorder, PostgresAggregateRecorder)
        self.assertEqual(recorder.events_table_name, "testcase_snapshots")
        self.assertIn(
            '"public"."testcase_snapshots"',
            recorder.create_table_statements[0].as_string(),
        )

        # Set schema in environment.
        self.env[PostgresFactory.POSTGRES_SCHEMA] = "myschema"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.schema, "myschema")

        # Check by default the table name is qualified.
        recorder = factory.aggregate_recorder("events")
        assert isinstance(recorder, PostgresAggregateRecorder)
        self.assertEqual(recorder.events_table_name, "testcase_events")
        self.assertIn(
            '"myschema"."testcase_events"',
            recorder.create_table_statements[0].as_string(),
        )

        # Check by default the table name is qualified.
        recorder = factory.aggregate_recorder("snapshots")
        assert isinstance(recorder, PostgresAggregateRecorder)
        self.assertEqual(recorder.events_table_name, "testcase_snapshots")
        self.assertIn(
            '"myschema"."testcase_snapshots"',
            recorder.create_table_statements[0].as_string(),
        )

    def test_scheme_adjusts_table_name_on_application_recorder(self) -> None:
        factory = PostgresFactory(self.env)

        # Check by default the table name is not qualified.
        recorder = factory.application_recorder()
        assert isinstance(recorder, PostgresApplicationRecorder)
        self.assertEqual(factory.datastore.schema, "public")
        self.assertEqual(recorder.events_table_name, "testcase_events")
        self.assertIn(
            '"public"."testcase_events"',
            recorder.create_table_statements[0].as_string(),
        )
        self.assertIn(
            '"public"."testcase_events"',
            recorder.create_table_statements[1].as_string(),
        )

        # Set schema in environment.
        self.env[PostgresFactory.POSTGRES_SCHEMA] = "myschema"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.schema, "myschema")

        # Check by default the table name is qualified.
        recorder = factory.application_recorder()
        assert isinstance(recorder, PostgresApplicationRecorder)
        self.assertIn(
            '"myschema"."testcase_events"',
            recorder.create_table_statements[0].as_string(),
        )
        self.assertIn(
            '"myschema"."testcase_events"',
            recorder.create_table_statements[1].as_string(),
        )

    def test_scheme_adjusts_table_names_on_process_recorder(self) -> None:
        factory = PostgresFactory(self.env)

        # Check by default the table name is not qualified.
        recorder = factory.process_recorder()
        assert isinstance(recorder, PostgresProcessRecorder)
        self.assertEqual(recorder.events_table_name, "testcase_events")
        self.assertEqual(recorder.tracking_table_name, "testcase_tracking")
        self.assertIn(
            '"public"."testcase_events"',
            recorder.create_table_statements[0].as_string(),
        )
        self.assertIn(
            '"public"."testcase_events"',
            recorder.create_table_statements[1].as_string(),
        )
        self.assertIn(
            '"public"."testcase_tracking"',
            recorder.create_table_statements[2].as_string(),
        )

        # Set schema in environment.
        self.env[PostgresFactory.POSTGRES_SCHEMA] = "myschema"
        factory = PostgresFactory(self.env)
        self.assertEqual(factory.datastore.schema, "myschema")

        # Check by default the table name is qualified.
        recorder = factory.process_recorder()
        assert isinstance(recorder, PostgresProcessRecorder)
        self.assertEqual(recorder.events_table_name, "testcase_events")
        self.assertEqual(recorder.tracking_table_name, "testcase_tracking")
        self.assertIn(
            '"myschema"."testcase_events"',
            recorder.create_table_statements[0].as_string(),
        )
        self.assertIn(
            '"myschema"."testcase_events"',
            recorder.create_table_statements[1].as_string(),
        )
        self.assertIn(
            '"myschema"."testcase_tracking"',
            recorder.create_table_statements[2].as_string(),
        )


del AggregateRecorderTestCase
del ApplicationRecorderTestCase
del TrackingRecorderTestCase
del ProcessRecorderTestCase
del InfrastructureFactoryTestCase
# del SetupPostgresDatastore
del WithSchema
del TestConnectionPool
