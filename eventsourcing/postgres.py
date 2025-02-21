from __future__ import annotations

import logging
from asyncio import CancelledError
from contextlib import contextmanager
from threading import Thread
from typing import TYPE_CHECKING, Any, Callable, Iterator, List, Sequence, Type

import psycopg
import psycopg.errors
import psycopg_pool
from psycopg import Connection, Cursor, Error
from psycopg.generators import notifies
from psycopg.rows import DictRow, dict_row

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    DatabaseError,
    DataError,
    InfrastructureFactory,
    IntegrityError,
    InterfaceError,
    InternalError,
    ListenNotifySubscription,
    Notification,
    NotSupportedError,
    OperationalError,
    PersistenceError,
    ProcessRecorder,
    ProgrammingError,
    StoredEvent,
    Tracking,
    TrackingRecorder,
)
from eventsourcing.utils import Environment, resolve_topic, retry, strtobool

if TYPE_CHECKING:  # pragma: no cover
    from uuid import UUID

    from typing_extensions import Self

logging.getLogger("psycopg.pool").setLevel(logging.CRITICAL)
logging.getLogger("psycopg").setLevel(logging.CRITICAL)

# Copy of "private" psycopg.errors._NO_TRACEBACK (in case it changes)
# From psycopg: "Don't show a complete traceback upon raising these exception.
# Usually the traceback starts from internal functions (for instance in the
# server communication callbacks) but, for the end user, it's more important
# to get the high level information about where the exception was raised, for
# instance in a certain `Cursor.execute()`."
NO_TRACEBACK = (Error, KeyboardInterrupt, CancelledError)


class ConnectionPool(psycopg_pool.ConnectionPool[Any]):
    def __init__(
        self,
        *args: Any,
        get_password_func: Callable[[], str] | None = None,
        **kwargs: Any,
    ) -> None:
        self.get_password_func = get_password_func
        super().__init__(*args, **kwargs)

    def _connect(self, timeout: float | None = None) -> Connection[Any]:
        if self.get_password_func:
            self.kwargs["password"] = self.get_password_func()
        return super()._connect(timeout=timeout)


class PostgresDatastore:
    def __init__(
        self,
        dbname: str,
        host: str,
        port: str | int,
        user: str,
        password: str,
        *,
        connect_timeout: int = 30,
        idle_in_transaction_session_timeout: int = 0,
        pool_size: int = 2,
        max_overflow: int = 2,
        max_waiting: int = 0,
        conn_max_age: float = 60 * 60.0,
        pre_ping: bool = False,
        lock_timeout: int = 0,
        schema: str = "",
        pool_open_timeout: int | None = None,
        get_password_func: Callable[[], str] | None = None,
    ):
        self.idle_in_transaction_session_timeout = idle_in_transaction_session_timeout
        self.pre_ping = pre_ping
        self.pool_open_timeout = pool_open_timeout

        check = ConnectionPool.check_connection if pre_ping else None
        self.pool = ConnectionPool(
            get_password_func=get_password_func,
            connection_class=Connection[DictRow],
            kwargs={
                "dbname": dbname,
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "row_factory": dict_row,
            },
            min_size=pool_size,
            max_size=pool_size + max_overflow,
            open=False,
            configure=self.after_connect_func(),
            timeout=connect_timeout,
            max_waiting=max_waiting,
            max_lifetime=conn_max_age,
            check=check,
        )
        self.lock_timeout = lock_timeout
        self.schema = schema.strip()

    def after_connect_func(self) -> Callable[[Connection[Any]], None]:
        statement = (
            "SET idle_in_transaction_session_timeout = "
            f"'{self.idle_in_transaction_session_timeout}s'"
        )

        def after_connect(conn: Connection[DictRow]) -> None:
            conn.autocommit = True
            conn.cursor().execute(statement)

        return after_connect

    @contextmanager
    def get_connection(self) -> Iterator[Connection[DictRow]]:
        try:
            wait = self.pool_open_timeout is not None
            timeout = self.pool_open_timeout or 30.0
            self.pool.open(wait, timeout)

            with self.pool.connection() as conn:
                yield conn
        except psycopg.InterfaceError as e:
            # conn.close()
            raise InterfaceError(str(e)) from e
        except psycopg.OperationalError as e:
            # conn.close()
            raise OperationalError(str(e)) from e
        except psycopg.DataError as e:
            raise DataError(str(e)) from e
        except psycopg.IntegrityError as e:
            raise IntegrityError(str(e)) from e
        except psycopg.InternalError as e:
            raise InternalError(str(e)) from e
        except psycopg.ProgrammingError as e:
            raise ProgrammingError(str(e)) from e
        except psycopg.NotSupportedError as e:
            raise NotSupportedError(str(e)) from e
        except psycopg.DatabaseError as e:
            raise DatabaseError(str(e)) from e
        except psycopg.Error as e:
            # conn.close()
            raise PersistenceError(str(e)) from e
        except Exception:
            # conn.close()
            raise

    @contextmanager
    def transaction(self, *, commit: bool = False) -> Iterator[Cursor[DictRow]]:
        conn: Connection[DictRow]
        with self.get_connection() as conn, conn.transaction(force_rollback=not commit):
            yield conn.cursor()

    def close(self) -> None:
        self.pool.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        self.close()


class PostgresRecorder:
    """Base class for recorders that use PostgreSQL."""

    def __init__(
        self,
        datastore: PostgresDatastore,
    ):
        self.datastore = datastore
        self.create_table_statements = self.construct_create_table_statements()

    def construct_create_table_statements(self) -> List[str]:
        return []

    @staticmethod
    def check_table_name_length(table_name: str, schema_name: str) -> None:
        schema_prefix = schema_name + "."
        if table_name.startswith(schema_prefix):
            unqualified_table_name = table_name[len(schema_prefix) :]
        else:
            unqualified_table_name = table_name
        if len(unqualified_table_name) > 63:
            msg = f"Table name too long: {unqualified_table_name}"
            raise ProgrammingError(msg)

    def create_table(self) -> None:
        with self.datastore.transaction(commit=True) as curs:
            for statement in self.create_table_statements:
                curs.execute(statement, prepare=False)


class PostgresAggregateRecorder(PostgresRecorder, AggregateRecorder):
    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        events_table_name: str = "stored_events",
    ):
        super().__init__(datastore)
        self.check_table_name_length(events_table_name, datastore.schema)
        self.events_table_name = events_table_name
        # Index names can't be qualified names, but
        # are created in the same schema as the table.
        if "." in self.events_table_name:
            unqualified_table_name = self.events_table_name.split(".")[-1]
        else:
            unqualified_table_name = self.events_table_name
        self.notification_id_index_name = (
            f"{unqualified_table_name}_notification_id_idx "
        )
        self.create_table_statements.append(
            "CREATE TABLE IF NOT EXISTS "
            f"{self.events_table_name} ("
            "originator_id uuid NOT NULL, "
            "originator_version bigint NOT NULL, "
            "topic text, "
            "state bytea, "
            "PRIMARY KEY "
            "(originator_id, originator_version)) "
            "WITH (autovacuum_enabled=false)"
        )

        self.insert_events_statement = (
            f"INSERT INTO {self.events_table_name} VALUES (%s, %s, %s, %s)"
        )
        self.select_events_statement = (
            f"SELECT * FROM {self.events_table_name} WHERE originator_id = %s"
        )
        self.lock_table_statements: List[str] = []

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def insert_events(
        self, stored_events: List[StoredEvent], **kwargs: Any
    ) -> Sequence[int] | None:
        conn: Connection[DictRow]
        exc: Exception | None = None
        notification_ids: Sequence[int] | None = None
        with self.datastore.get_connection() as conn:
            with conn.pipeline() as pipeline, conn.transaction():
                # Do other things first, so they can be pipelined too.
                with conn.cursor() as curs:
                    self._insert_events(curs, stored_events, **kwargs)
                # Then use a different cursor for the executemany() call.
                with conn.cursor() as curs:
                    try:
                        self._insert_stored_events(curs, stored_events, **kwargs)
                        # Sync now, so any uniqueness constraint violation causes an
                        # IntegrityError to be raised here, rather an InternalError
                        # being raised sometime later e.g. when commit() is called.
                        pipeline.sync()
                        notification_ids = self._fetch_ids_after_insert_events(
                            curs, stored_events, **kwargs
                        )
                    except Exception as e:
                        # Avoid psycopg emitting a pipeline warning.
                        exc = e
            if exc:
                # Reraise exception after pipeline context manager has exited.
                raise exc
        return notification_ids

    def _insert_events(
        self,
        c: Cursor[DictRow],
        stored_events: List[StoredEvent],
        **_: Any,
    ) -> None:
        pass

    def _insert_stored_events(
        self,
        c: Cursor[DictRow],
        stored_events: List[StoredEvent],
        **_: Any,
    ) -> None:
        # Only do something if there is something to do.
        if len(stored_events) > 0:
            self._lock_table(c)

            self._notify_channel(c)

            # Insert events.
            c.executemany(
                query=self.insert_events_statement,
                params_seq=[
                    (
                        stored_event.originator_id,
                        stored_event.originator_version,
                        stored_event.topic,
                        stored_event.state,
                    )
                    for stored_event in stored_events
                ],
                returning="RETURNING" in self.insert_events_statement,
            )

    def _lock_table(self, c: Cursor[DictRow]) -> None:
        pass

    def _notify_channel(self, c: Cursor[DictRow]) -> None:
        pass

    def _fetch_ids_after_insert_events(
        self,
        c: Cursor[DictRow],
        stored_events: List[StoredEvent],
        **kwargs: Any,
    ) -> Sequence[int] | None:
        return None

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def select_events(
        self,
        originator_id: UUID,
        *,
        gt: int | None = None,
        lte: int | None = None,
        desc: bool = False,
        limit: int | None = None,
    ) -> List[StoredEvent]:
        statement = self.select_events_statement
        params: List[Any] = [originator_id]
        if gt is not None:
            params.append(gt)
            statement += " AND originator_version > %s"
        if lte is not None:
            params.append(lte)
            statement += " AND originator_version <= %s"
        statement += " ORDER BY originator_version"
        if desc is False:
            statement += " ASC"
        else:
            statement += " DESC"
        if limit is not None:
            params.append(limit)
            statement += " LIMIT %s"

        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            curs.execute(statement, params, prepare=True)
            return [
                StoredEvent(
                    originator_id=row["originator_id"],
                    originator_version=row["originator_version"],
                    topic=row["topic"],
                    state=bytes(row["state"]),
                )
                for row in curs.fetchall()
            ]


class PostgresApplicationRecorder(PostgresAggregateRecorder, ApplicationRecorder):
    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        events_table_name: str = "stored_events",
    ):
        super().__init__(datastore, events_table_name=events_table_name)
        self.create_table_statements[-1] = (
            "CREATE TABLE IF NOT EXISTS "
            f"{self.events_table_name} ("
            "originator_id uuid NOT NULL, "
            "originator_version bigint NOT NULL, "
            "topic text, "
            "state bytea, "
            "notification_id bigserial, "
            "PRIMARY KEY "
            "(originator_id, originator_version)) "
            "WITH (autovacuum_enabled=false)"
        )
        self.create_table_statements.append(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            f"{self.notification_id_index_name}"
            f"ON {self.events_table_name} (notification_id ASC);"
        )
        self.channel_name = self.events_table_name.replace(".", "_")
        self.insert_events_statement += " RETURNING notification_id"
        self.max_notification_id_statement = (
            f"SELECT MAX(notification_id) FROM {self.events_table_name}"
        )
        self.lock_table_statements = [
            f"SET LOCAL lock_timeout = '{self.datastore.lock_timeout}s'",
            f"LOCK TABLE {self.events_table_name} IN EXCLUSIVE MODE",
        ]

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def select_notifications(
        self,
        start: int | None,
        limit: int,
        stop: int | None = None,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> List[Notification]:
        """
        Returns a list of event notifications
        from 'start', limited by 'limit'.
        """

        params: List[int | str | Sequence[str]] = []
        statement = f"SELECT * FROM {self.events_table_name}"
        has_where = False
        if start is not None:
            statement += " WHERE"
            has_where = True
            params.append(start)
            if inclusive_of_start:
                statement += " notification_id>=%s"
            else:
                statement += " notification_id>%s"

        if stop is not None:
            if not has_where:
                has_where = True
                statement += " WHERE"
            else:
                statement += " AND"

            params.append(stop)
            statement += " notification_id <= %s"

        if topics:
            if not has_where:
                statement += " WHERE"
            else:
                statement += " AND"
            params.append(topics)
            statement += " topic = ANY(%s)"

        params.append(limit)
        statement += " ORDER BY notification_id LIMIT %s"

        connection = self.datastore.get_connection()
        with connection as conn, conn.cursor() as curs:
            curs.execute(statement, params, prepare=True)
            return [
                Notification(
                    id=row["notification_id"],
                    originator_id=row["originator_id"],
                    originator_version=row["originator_version"],
                    topic=row["topic"],
                    state=bytes(row["state"]),
                )
                for row in curs.fetchall()
            ]

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def max_notification_id(self) -> int | None:
        """
        Returns the maximum notification ID.
        """
        conn: Connection[DictRow]
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            curs.execute(self.max_notification_id_statement)
            fetchone = curs.fetchone()
            assert fetchone is not None
            return fetchone["max"]

    def _lock_table(self, c: Cursor[DictRow]) -> None:
        # Acquire "EXCLUSIVE" table lock, to serialize transactions that insert
        # stored events, so that readers don't pass over gaps that are filled in
        # later. We want each transaction that will be issued with notifications
        # IDs by the notification ID sequence to receive all its notification IDs
        # and then commit, before another transaction is issued with any notification
        # IDs. In other words, we want the insert order to be the same as the commit
        # order. We can accomplish this by locking the table for writes. The
        # EXCLUSIVE lock mode does not block SELECT statements, which acquire an
        # ACCESS SHARE lock, so the stored events table can be read concurrently
        # with writes and other reads. However, INSERT statements normally just
        # acquires ROW EXCLUSIVE locks, which risks the interleaving (within the
        # recorded sequence of notification IDs) of stored events from one transaction
        # with those of another transaction. And since one transaction will always
        # commit before another, the possibility arises when using ROW EXCLUSIVE locks
        # for readers that are tailing a notification log to miss items inserted later
        # but issued with lower notification IDs.
        # https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-TABLES
        # https://www.postgresql.org/docs/9.1/sql-lock.html
        # https://stackoverflow.com/questions/45866187/guarantee-monotonicity-of
        # -postgresql-serial-column-values-by-commit-order
        for lock_statement in self.lock_table_statements:
            c.execute(lock_statement, prepare=True)

    def _notify_channel(self, c: Cursor[DictRow]) -> None:
        c.execute("NOTIFY " + self.channel_name)

    def _fetch_ids_after_insert_events(
        self,
        c: Cursor[DictRow],
        stored_events: List[StoredEvent],
        **kwargs: Any,
    ) -> Sequence[int] | None:
        notification_ids: List[int] = []
        len_events = len(stored_events)
        if len_events:
            while c.nextset() and len(notification_ids) != len_events:
                if c.statusmessage and c.statusmessage.startswith("INSERT"):
                    row = c.fetchone()
                    assert row is not None
                    notification_ids.append(row["notification_id"])
            if len(notification_ids) != len(stored_events):
                msg = "Couldn't get all notification IDs "
                msg += f"(got {len(notification_ids)}, expected {len(stored_events)}"
                raise ProgrammingError(msg)
        return notification_ids

    def subscribe(self, gt: int | None = None) -> PostgresSubscription:
        return PostgresSubscription(self, gt)


class PostgresSubscription(ListenNotifySubscription[PostgresApplicationRecorder]):
    def __init__(
        self, recorder: PostgresApplicationRecorder, gt: int | None = None
    ) -> None:
        assert isinstance(recorder, PostgresApplicationRecorder)
        super().__init__(recorder=recorder, gt=gt)
        self._listen_thread = Thread(target=self._listen)
        self._listen_thread.start()

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        super().__exit__(*args, **kwargs)
        self._listen_thread.join()

    def _listen(self) -> None:
        try:
            with self._recorder.datastore.get_connection() as conn:
                conn.execute("LISTEN " + self._recorder.channel_name)
                while not self._has_been_stopped and not self._thread_error:
                    # This block simplifies psycopg's conn.notifies(), because
                    # we aren't interested in the actual notify messages, and
                    # also we want to stop consuming notify messages when the
                    # subscription has an error or is otherwise stopped.
                    with conn.lock:
                        try:
                            if conn.wait(notifies(conn.pgconn), interval=0.1):
                                self._has_been_notified.set()
                        except NO_TRACEBACK as ex:  # pragma: no cover
                            raise ex.with_traceback(None) from None

        except BaseException as e:
            if self._thread_error is None:
                self._thread_error = e
            self.stop()


class PostgresTrackingRecorder(PostgresRecorder, TrackingRecorder):
    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        tracking_table_name: str = "notification_tracking",
        **kwargs: Any,
    ):
        super().__init__(datastore, **kwargs)
        self.check_table_name_length(tracking_table_name, datastore.schema)
        self.tracking_table_name = tracking_table_name
        self.create_table_statements.append(
            "CREATE TABLE IF NOT EXISTS "
            f"{self.tracking_table_name} ("
            "application_name text, "
            "notification_id bigint, "
            "PRIMARY KEY "
            "(application_name, notification_id))"
        )
        self.insert_tracking_statement = (
            f"INSERT INTO {self.tracking_table_name} VALUES (%s, %s)"
        )
        self.max_tracking_id_statement = (
            "SELECT MAX(notification_id) "
            f"FROM {self.tracking_table_name} "
            "WHERE application_name=%s"
        )
        self.count_tracking_id_statement = (
            "SELECT COUNT(*) "
            f"FROM {self.tracking_table_name} "
            "WHERE application_name=%s AND notification_id=%s"
        )

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def insert_tracking(self, tracking: Tracking) -> None:
        c: Connection[DictRow]
        with self.datastore.get_connection() as c, c.transaction(), c.cursor() as curs:
            self._insert_tracking(curs, tracking)

    def _insert_tracking(
        self,
        c: Cursor[DictRow],
        tracking: Tracking,
    ) -> None:
        c.execute(
            query=self.insert_tracking_statement,
            params=(
                tracking.application_name,
                tracking.notification_id,
            ),
            prepare=True,
        )

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def max_tracking_id(self, application_name: str) -> int | None:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            curs.execute(
                query=self.max_tracking_id_statement,
                params=(application_name,),
                prepare=True,
            )
            fetchone = curs.fetchone()
            assert fetchone is not None
            return fetchone["max"]

    @retry((InterfaceError, OperationalError), max_attempts=10, wait=0.2)
    def has_tracking_id(self, application_name: str, notification_id: int) -> bool:
        conn: Connection[DictRow]
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            curs.execute(
                query=self.count_tracking_id_statement,
                params=(application_name, notification_id),
                prepare=True,
            )
            fetchone = curs.fetchone()
            assert fetchone is not None
            return bool(fetchone["count"])


class PostgresProcessRecorder(
    PostgresTrackingRecorder, PostgresApplicationRecorder, ProcessRecorder
):
    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        events_table_name: str = "stored_events",
        tracking_table_name: str = "notification_tracking",
    ):
        super().__init__(
            datastore,
            tracking_table_name=tracking_table_name,
            events_table_name=events_table_name,
        )

    def _insert_events(
        self,
        c: Cursor[DictRow],
        stored_events: List[StoredEvent],
        **kwargs: Any,
    ) -> None:
        tracking: Tracking | None = kwargs.get("tracking", None)
        if tracking is not None:
            self._insert_tracking(c, tracking=tracking)
        super()._insert_events(c, stored_events, **kwargs)


class PostgresFactory(InfrastructureFactory[PostgresTrackingRecorder]):
    POSTGRES_DBNAME = "POSTGRES_DBNAME"
    POSTGRES_HOST = "POSTGRES_HOST"
    POSTGRES_PORT = "POSTGRES_PORT"
    POSTGRES_USER = "POSTGRES_USER"
    POSTGRES_PASSWORD = "POSTGRES_PASSWORD"  # noqa: S105
    POSTGRES_GET_PASSWORD_TOPIC = "POSTGRES_GET_PASSWORD_TOPIC"  # noqa: S105
    POSTGRES_CONNECT_TIMEOUT = "POSTGRES_CONNECT_TIMEOUT"
    POSTGRES_CONN_MAX_AGE = "POSTGRES_CONN_MAX_AGE"
    POSTGRES_PRE_PING = "POSTGRES_PRE_PING"
    POSTGRES_MAX_WAITING = "POSTGRES_MAX_WAITING"
    POSTGRES_LOCK_TIMEOUT = "POSTGRES_LOCK_TIMEOUT"
    POSTGRES_POOL_SIZE = "POSTGRES_POOL_SIZE"
    POSTGRES_MAX_OVERFLOW = "POSTGRES_MAX_OVERFLOW"
    POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT = (
        "POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT"
    )
    POSTGRES_SCHEMA = "POSTGRES_SCHEMA"
    CREATE_TABLE = "CREATE_TABLE"

    aggregate_recorder_class = PostgresAggregateRecorder
    application_recorder_class = PostgresApplicationRecorder
    tracking_recorder_class = PostgresTrackingRecorder
    process_recorder_class = PostgresProcessRecorder

    def __init__(self, env: Environment):
        super().__init__(env)
        dbname = self.env.get(self.POSTGRES_DBNAME)
        if dbname is None:
            msg = (
                "Postgres database name not found "
                "in environment with key "
                f"'{self.POSTGRES_DBNAME}'"
            )
            raise OSError(msg)

        host = self.env.get(self.POSTGRES_HOST)
        if host is None:
            msg = (
                "Postgres host not found "
                "in environment with key "
                f"'{self.POSTGRES_HOST}'"
            )
            raise OSError(msg)

        port = self.env.get(self.POSTGRES_PORT) or "5432"

        user = self.env.get(self.POSTGRES_USER)
        if user is None:
            msg = (
                "Postgres user not found "
                "in environment with key "
                f"'{self.POSTGRES_USER}'"
            )
            raise OSError(msg)

        get_password_func = None
        get_password_topic = self.env.get(self.POSTGRES_GET_PASSWORD_TOPIC)
        if not get_password_topic:
            password = self.env.get(self.POSTGRES_PASSWORD)
            if password is None:
                msg = (
                    "Postgres password not found "
                    "in environment with key "
                    f"'{self.POSTGRES_PASSWORD}'"
                )
                raise OSError(msg)
        else:
            get_password_func = resolve_topic(get_password_topic)
            password = ""

        connect_timeout = 30
        connect_timeout_str = self.env.get(self.POSTGRES_CONNECT_TIMEOUT)
        if connect_timeout_str:
            try:
                connect_timeout = int(connect_timeout_str)
            except ValueError:
                msg = (
                    "Postgres environment value for key "
                    f"'{self.POSTGRES_CONNECT_TIMEOUT}' is invalid. "
                    "If set, an integer or empty string is expected: "
                    f"'{connect_timeout_str}'"
                )
                raise OSError(msg) from None

        idle_in_transaction_session_timeout_str = (
            self.env.get(self.POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT) or "5"
        )

        try:
            idle_in_transaction_session_timeout = int(
                idle_in_transaction_session_timeout_str
            )
        except ValueError:
            msg = (
                "Postgres environment value for key "
                f"'{self.POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT}' is invalid. "
                "If set, an integer or empty string is expected: "
                f"'{idle_in_transaction_session_timeout_str}'"
            )
            raise OSError(msg) from None

        pool_size = 5
        pool_size_str = self.env.get(self.POSTGRES_POOL_SIZE)
        if pool_size_str:
            try:
                pool_size = int(pool_size_str)
            except ValueError:
                msg = (
                    "Postgres environment value for key "
                    f"'{self.POSTGRES_POOL_SIZE}' is invalid. "
                    "If set, an integer or empty string is expected: "
                    f"'{pool_size_str}'"
                )
                raise OSError(msg) from None

        pool_max_overflow = 10
        pool_max_overflow_str = self.env.get(self.POSTGRES_MAX_OVERFLOW)
        if pool_max_overflow_str:
            try:
                pool_max_overflow = int(pool_max_overflow_str)
            except ValueError:
                msg = (
                    "Postgres environment value for key "
                    f"'{self.POSTGRES_MAX_OVERFLOW}' is invalid. "
                    "If set, an integer or empty string is expected: "
                    f"'{pool_max_overflow_str}'"
                )
                raise OSError(msg) from None

        max_waiting = 0
        max_waiting_str = self.env.get(self.POSTGRES_MAX_WAITING)
        if max_waiting_str:
            try:
                max_waiting = int(max_waiting_str)
            except ValueError:
                msg = (
                    "Postgres environment value for key "
                    f"'{self.POSTGRES_MAX_WAITING}' is invalid. "
                    "If set, an integer or empty string is expected: "
                    f"'{max_waiting_str}'"
                )
                raise OSError(msg) from None

        conn_max_age = 60 * 60.0
        conn_max_age_str = self.env.get(self.POSTGRES_CONN_MAX_AGE)
        if conn_max_age_str:
            try:
                conn_max_age = float(conn_max_age_str)
            except ValueError:
                msg = (
                    "Postgres environment value for key "
                    f"'{self.POSTGRES_CONN_MAX_AGE}' is invalid. "
                    "If set, a float or empty string is expected: "
                    f"'{conn_max_age_str}'"
                )
                raise OSError(msg) from None

        pre_ping = strtobool(self.env.get(self.POSTGRES_PRE_PING) or "no")

        lock_timeout_str = self.env.get(self.POSTGRES_LOCK_TIMEOUT) or "0"

        try:
            lock_timeout = int(lock_timeout_str)
        except ValueError:
            msg = (
                "Postgres environment value for key "
                f"'{self.POSTGRES_LOCK_TIMEOUT}' is invalid. "
                "If set, an integer or empty string is expected: "
                f"'{lock_timeout_str}'"
            )
            raise OSError(msg) from None

        schema = self.env.get(self.POSTGRES_SCHEMA) or ""

        self.datastore = PostgresDatastore(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password,
            get_password_func=get_password_func,
            connect_timeout=connect_timeout,
            idle_in_transaction_session_timeout=idle_in_transaction_session_timeout,
            pool_size=pool_size,
            max_overflow=pool_max_overflow,
            max_waiting=max_waiting,
            conn_max_age=conn_max_age,
            pre_ping=pre_ping,
            lock_timeout=lock_timeout,
            schema=schema,
        )

    def env_create_table(self) -> bool:
        return strtobool(self.env.get(self.CREATE_TABLE) or "yes")

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_" + purpose
        if self.datastore.schema:
            events_table_name = f"{self.datastore.schema}.{events_table_name}"
        recorder = type(self).aggregate_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_events"
        if self.datastore.schema:
            events_table_name = f"{self.datastore.schema}.{events_table_name}"

        application_recorder_topic = self.env.get(self.APPLICATION_RECORDER_TOPIC)
        if application_recorder_topic:
            application_recorder_class: Type[PostgresApplicationRecorder] = (
                resolve_topic(application_recorder_topic)
            )
            assert issubclass(application_recorder_class, PostgresApplicationRecorder)
        else:
            application_recorder_class = type(self).application_recorder_class

        recorder = application_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def tracking_recorder(
        self, tracking_recorder_class: Type[PostgresTrackingRecorder] | None = None
    ) -> PostgresTrackingRecorder:
        prefix = self.env.name.lower() or "notification"
        tracking_table_name = prefix + "_tracking"
        if self.datastore.schema:
            tracking_table_name = f"{self.datastore.schema}.{tracking_table_name}"
        if tracking_recorder_class is None:
            tracking_recorder_topic = self.env.get(self.TRACKING_RECORDER_TOPIC)
            if tracking_recorder_topic:
                tracking_recorder_class = resolve_topic(tracking_recorder_topic)
            else:
                tracking_recorder_class = type(self).tracking_recorder_class
        assert tracking_recorder_class is not None
        assert issubclass(tracking_recorder_class, PostgresTrackingRecorder)
        recorder = tracking_recorder_class(
            datastore=self.datastore,
            tracking_table_name=tracking_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def process_recorder(self) -> ProcessRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_events"
        prefix = self.env.name.lower() or "notification"
        tracking_table_name = prefix + "_tracking"
        if self.datastore.schema:
            events_table_name = f"{self.datastore.schema}.{events_table_name}"
            tracking_table_name = f"{self.datastore.schema}.{tracking_table_name}"

        process_recorder_topic = self.env.get(self.PROCESS_RECORDER_TOPIC)
        if process_recorder_topic:
            process_recorder_class: Type[PostgresTrackingRecorder] = resolve_topic(
                process_recorder_topic
            )
            assert issubclass(process_recorder_class, PostgresProcessRecorder)
        else:
            process_recorder_class = type(self).process_recorder_class

        recorder = process_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
            tracking_table_name=tracking_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def close(self) -> None:
        if hasattr(self, "datastore"):
            self.datastore.close()

    def __del__(self) -> None:
        self.close()


Factory = PostgresFactory
