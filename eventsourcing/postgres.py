import threading
from distutils.util import strtobool
from threading import Event, Timer
from types import TracebackType
from typing import Any, Dict, List, Mapping, Optional, Type
from uuid import UUID

import psycopg2
import psycopg2.errors
import psycopg2.extras
from psycopg2.extensions import connection, cursor

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    DatabaseError,
    DataError,
    InfrastructureFactory,
    IntegrityError,
    InterfaceError,
    InternalError,
    Notification,
    NotSupportedError,
    OperationalError,
    PersistenceError,
    ProcessRecorder,
    ProgrammingError,
    StoredEvent,
    Tracking,
)
from eventsourcing.utils import retry

psycopg2.extras.register_uuid()


class Connection:
    def __init__(self, c: connection, max_age: Optional[float]):
        self.c = c
        self.max_age = max_age
        self.is_idle = Event()
        self.is_closing = Event()
        self.timer: Optional[Timer]
        if max_age is not None:
            self.timer = Timer(interval=max_age, function=self.close_on_timer)
            self.timer.setDaemon(True)
            self.timer.start()
        else:
            self.timer = None

    def cursor(self) -> cursor:
        return self.c.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def rollback(self) -> None:
        self.c.rollback()

    def commit(self) -> None:
        self.c.commit()

    def close_on_timer(self) -> None:
        self.close()

    def close(self, timeout: Optional[float] = None) -> None:
        if self.timer is not None:
            self.timer.cancel()
        self.is_closing.set()
        self.is_idle.wait(timeout=timeout)
        self.c.close()

    @property
    def is_closed(self) -> bool:
        return self.c.closed


class Transaction:
    # noinspection PyShadowingNames
    def __init__(self, c: Connection, commit: bool):
        self.c = c
        self.commit = commit
        self.has_entered = False

    def __enter__(self) -> cursor:
        self.has_entered = True
        return self.c.cursor()

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        try:
            if exc_val:
                self.c.rollback()
                raise exc_val
            elif not self.commit:
                self.c.rollback()
            else:
                self.c.commit()
        except psycopg2.InterfaceError as e:
            self.c.close(timeout=0)
            raise InterfaceError(e)
        except psycopg2.DataError as e:
            raise DataError(e)
        except psycopg2.OperationalError as e:
            raise OperationalError(e)
        except psycopg2.IntegrityError as e:
            raise IntegrityError(e)
        except psycopg2.InternalError as e:
            raise InternalError(e)
        except psycopg2.ProgrammingError as e:
            raise ProgrammingError(e)
        except psycopg2.NotSupportedError as e:
            raise NotSupportedError(e)
        except psycopg2.DatabaseError as e:
            raise DatabaseError(e)
        except psycopg2.Error as e:
            raise PersistenceError(e)
        finally:
            self.c.is_idle.set()

    def __del__(self) -> None:
        if not self.has_entered:
            self.c.is_idle.set()
            raise RuntimeWarning(f"Transaction {self} was not used as context manager")


class PostgresDatastore:
    def __init__(
        self,
        dbname: str,
        host: str,
        port: str,
        user: str,
        password: str,
        conn_max_age: Optional[float] = None,
        pre_ping: bool = False,
        lock_timeout: float = 0,
    ):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.conn_max_age = conn_max_age
        self.pre_ping = pre_ping
        self.lock_timeout = lock_timeout
        self._connections: Dict[int, Connection] = {}

    def transaction(self, commit: bool) -> Transaction:
        thread_id = threading.get_ident()
        try:
            c = self._connections[thread_id]
        except KeyError:
            c = self._create_connection(thread_id)
        else:
            c.is_idle.clear()
            if c.is_closing.is_set() or c.is_closed:
                c = self._create_connection(thread_id)
            elif self.pre_ping:
                try:
                    c.cursor().execute("SELECT 1")
                except psycopg2.Error:
                    c = self._create_connection(thread_id)
        return Transaction(c, commit=commit)

    def _create_connection(self, thread_id: int) -> Connection:
        # Make a connection to a Postgres database.
        try:
            psycopg_c = psycopg2.connect(
                dbname=self.dbname,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
        except psycopg2.Error as e:
            raise InterfaceError(e)
        else:
            c = Connection(
                psycopg_c,
                max_age=self.conn_max_age,
            )
            self._connections[thread_id] = c
            return c

    def close_connection(self) -> None:
        thread_id = threading.get_ident()
        try:
            c = self._connections.pop(thread_id)
        except KeyError:
            pass
        else:
            c.close()

    def close_all_connections(self, timeout: Optional[float] = None) -> None:
        for c in self._connections.values():
            c.close(timeout=timeout)
        self._connections.clear()

    def __del__(self) -> None:
        self.close_all_connections(timeout=1)


# noinspection SqlResolve
class PostgresAggregateRecorder(AggregateRecorder):
    def __init__(self, datastore: PostgresDatastore, events_table_name: str):
        self.datastore = datastore
        self.events_table_name = events_table_name
        self.create_table_statements = self.construct_create_table_statements()
        self.insert_events_statement = (
            f"INSERT INTO {self.events_table_name} VALUES (%s, %s, %s, %s)"
        )
        self.select_events_statement = (
            f"SELECT * FROM {self.events_table_name} WHERE originator_id = %s "
        )

    def construct_create_table_statements(self) -> List[str]:
        statement = (
            "CREATE TABLE IF NOT EXISTS "
            f"{self.events_table_name} ("
            "originator_id uuid NOT NULL, "
            "originator_version integer NOT NULL, "
            "topic text, "
            "state bytea, "
            "PRIMARY KEY "
            "(originator_id, originator_version))"
        )
        return [statement]

    def create_table(self) -> None:
        with self.datastore.transaction(commit=True) as c:
            for statement in self.create_table_statements:
                c.execute(statement)

    @retry(InterfaceError, max_attempts=10, wait=0.2)
    def insert_events(self, stored_events: List[StoredEvent], **kwargs: Any) -> None:
        with self.datastore.transaction(commit=True) as c:
            self._lock_table(c)
            self._insert_events(c, stored_events, **kwargs)

    def _lock_table(self, c: cursor) -> None:
        # Acquire "EXCLUSIVE" table lock, to serialize inserts so that
        # insertion of notification IDs is monotonic for notification log
        # readers. We want concurrent transactions to commit inserted
        # SERIAL values in order, and by locking the table for writes,
        # it can be guaranteed. The EXCLUSIVE lock mode does not block
        # the ACCESS SHARE lock which is acquired during SELECT statements,
        # so the table can be read concurrently. However INSERT normally
        # just acquires ROW EXCLUSIVE locks, which risks interleaving of
        # many inserts in one transaction with many insert in another
        # transaction. Since one transaction will commit before another,
        # the possibility arises for readers that are tailing a notification
        # log to miss items inserted later but with lower notification IDs.
        # https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-TABLES
        # https://www.postgresql.org/docs/9.1/sql-lock.html
        # https://stackoverflow.com/questions/45866187/guarantee-monotonicity-of
        # -postgresql-serial-column-values-by-commit-order
        c.execute(
            f"SET LOCAL lock_timeout = '{self.datastore.lock_timeout}s'; "
            f"LOCK TABLE {self.events_table_name} IN EXCLUSIVE MODE"
        )

    def _insert_events(
        self,
        c: cursor,
        stored_events: List[StoredEvent],
        **kwargs: Any,
    ) -> None:
        params = []
        for stored_event in stored_events:
            params.append(
                (
                    stored_event.originator_id,
                    stored_event.originator_version,
                    stored_event.topic,
                    stored_event.state,
                )
            )
        c.executemany(self.insert_events_statement, params)

    @retry(InterfaceError, max_attempts=10, wait=0.2)
    def select_events(
        self,
        originator_id: UUID,
        gt: Optional[int] = None,
        lte: Optional[int] = None,
        desc: bool = False,
        limit: Optional[int] = None,
    ) -> List[StoredEvent]:
        statement = self.select_events_statement
        params: List[Any] = [originator_id]
        if gt is not None:
            statement += "AND originator_version > %s "
            params.append(gt)
        if lte is not None:
            statement += "AND originator_version <= %s "
            params.append(lte)
        statement += "ORDER BY originator_version "
        if desc is False:
            statement += "ASC "
        else:
            statement += "DESC "
        if limit is not None:
            statement += "LIMIT %s "
            params.append(limit)
        # statement += ";"
        stored_events = []
        with self.datastore.transaction(commit=False) as c:
            c.execute(statement, params)
            for row in c.fetchall():
                stored_events.append(
                    StoredEvent(
                        originator_id=row["originator_id"],
                        originator_version=row["originator_version"],
                        topic=row["topic"],
                        state=bytes(row["state"]),
                    )
                )
        return stored_events


# noinspection SqlResolve
class PostgresApplicationRecorder(
    PostgresAggregateRecorder,
    ApplicationRecorder,
):
    def __init__(
        self,
        datastore: PostgresDatastore,
        events_table_name: str = "stored_events",
    ):
        super().__init__(datastore, events_table_name)
        self.select_notifications_statement = (
            "SELECT * "
            f"FROM {self.events_table_name} "
            "WHERE notification_id>=%s "
            "ORDER BY notification_id "
            "LIMIT %s"
        )
        self.max_notification_id_statement = (
            f"SELECT MAX(notification_id) FROM {self.events_table_name}"
        )

    def construct_create_table_statements(self) -> List[str]:
        statements = [
            "CREATE TABLE IF NOT EXISTS "
            f"{self.events_table_name} ("
            "originator_id uuid NOT NULL, "
            "originator_version integer NOT NULL, "
            "topic text, "
            "state bytea, "
            "notification_id SERIAL, "
            "PRIMARY KEY "
            "(originator_id, originator_version))",
            f"CREATE UNIQUE INDEX IF NOT EXISTS "
            f"{self.events_table_name}_notification_id_idx "
            f"ON {self.events_table_name} (notification_id ASC);",
        ]
        return statements

    @retry(InterfaceError, max_attempts=10, wait=0.2)
    def select_notifications(self, start: int, limit: int) -> List[Notification]:
        """
        Returns a list of event notifications
        from 'start', limited by 'limit'.
        """
        notifications = []
        with self.datastore.transaction(commit=False) as c:
            c.execute(self.select_notifications_statement, [start, limit])
            for row in c.fetchall():
                notifications.append(
                    Notification(
                        id=row["notification_id"],
                        originator_id=row["originator_id"],
                        originator_version=row["originator_version"],
                        topic=row["topic"],
                        state=bytes(row["state"]),
                    )
                )
        return notifications

    @retry(InterfaceError, max_attempts=10, wait=0.2)
    def max_notification_id(self) -> int:
        """
        Returns the maximum notification ID.
        """
        with self.datastore.transaction(commit=False) as c:
            c.execute(self.max_notification_id_statement)
            max_id = c.fetchone()[0] or 0
        return max_id


class PostgresProcessRecorder(
    PostgresApplicationRecorder,
    ProcessRecorder,
):
    def __init__(
        self,
        datastore: PostgresDatastore,
        events_table_name: str,
        tracking_table_name: str,
    ):
        self.tracking_table_name = tracking_table_name
        super().__init__(datastore, events_table_name)
        self.insert_tracking_statement = (
            f"INSERT INTO {self.tracking_table_name} " "VALUES (%s, %s)"
        )
        self.max_tracking_id_statement = (
            "SELECT MAX(notification_id) "
            f"FROM {self.tracking_table_name} "
            "WHERE application_name=%s"
        )

    def construct_create_table_statements(self) -> List[str]:
        statements = super().construct_create_table_statements()
        statements.append(
            "CREATE TABLE IF NOT EXISTS "
            f"{self.tracking_table_name} ("
            "application_name text, "
            "notification_id int, "
            "PRIMARY KEY "
            "(application_name, notification_id))"
        )
        return statements

    @retry(InterfaceError, max_attempts=10, wait=0.2)
    def max_tracking_id(self, application_name: str) -> int:
        with self.datastore.transaction(commit=False) as c:
            c.execute(self.max_tracking_id_statement, [application_name])
            max_id = c.fetchone()[0] or 0
        return max_id

    def _insert_events(
        self,
        c: cursor,
        stored_events: List[StoredEvent],
        **kwargs: Any,
    ) -> None:
        super()._insert_events(c, stored_events, **kwargs)
        tracking: Optional[Tracking] = kwargs.get("tracking", None)
        if tracking is not None:
            c.execute(
                self.insert_tracking_statement,
                (
                    tracking.application_name,
                    tracking.notification_id,
                ),
            )


class Factory(InfrastructureFactory):
    POSTGRES_DBNAME = "POSTGRES_DBNAME"
    POSTGRES_HOST = "POSTGRES_HOST"
    POSTGRES_PORT = "POSTGRES_PORT"
    POSTGRES_USER = "POSTGRES_USER"
    POSTGRES_PASSWORD = "POSTGRES_PASSWORD"
    POSTGRES_CONN_MAX_AGE = "POSTGRES_CONN_MAX_AGE"
    CREATE_TABLE = "CREATE_TABLE"
    POSTGRES_PRE_PING = "POSTGRES_PRE_PING"
    POSTGRES_LOCK_TIMEOUT = "POSTGRES_LOCK_TIMEOUT"

    def __init__(self, application_name: str, env: Mapping):
        super().__init__(application_name, env)
        dbname = self.getenv(self.POSTGRES_DBNAME)
        if dbname is None:
            raise EnvironmentError(
                "Postgres database name not found "
                "in environment with key "
                f"'{self.POSTGRES_DBNAME}'"
            )

        host = self.getenv(self.POSTGRES_HOST)
        if host is None:
            raise EnvironmentError(
                "Postgres host not found "
                "in environment with key "
                f"'{self.POSTGRES_HOST}'"
            )

        port = self.getenv(self.POSTGRES_PORT) or "5432"

        user = self.getenv(self.POSTGRES_USER)
        if user is None:
            raise EnvironmentError(
                "Postgres user not found "
                "in environment with key "
                f"'{self.POSTGRES_USER}'"
            )

        password = self.getenv(self.POSTGRES_PASSWORD)
        if password is None:
            raise EnvironmentError(
                "Postgres password not found "
                "in environment with key "
                f"'{self.POSTGRES_PASSWORD}'"
            )

        conn_max_age: Optional[float]
        conn_max_age_str = self.getenv(self.POSTGRES_CONN_MAX_AGE)
        if conn_max_age_str is None:
            conn_max_age = None
        elif conn_max_age_str == "":
            conn_max_age = None
        else:
            try:
                conn_max_age = float(conn_max_age_str)
            except ValueError:
                raise EnvironmentError(
                    f"Postgres environment value for key "
                    f"'{self.POSTGRES_CONN_MAX_AGE}' is invalid. "
                    f"If set, a float or empty string is expected: "
                    f"'{conn_max_age_str}'"
                )

        pre_ping = strtobool(self.getenv(self.POSTGRES_PRE_PING) or "no")

        lock_timeout_str = self.getenv(self.POSTGRES_LOCK_TIMEOUT) or "0"

        try:
            lock_timeout = float(lock_timeout_str)
        except ValueError:
            raise EnvironmentError(
                f"Postgres environment value for key "
                f"'{self.POSTGRES_LOCK_TIMEOUT}' is invalid. "
                f"If set, a float or empty string is expected: "
                f"'{lock_timeout_str}'"
            )

        self.datastore = PostgresDatastore(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password,
            conn_max_age=conn_max_age,
            pre_ping=pre_ping,
            lock_timeout=lock_timeout,
        )

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        prefix = self.application_name.lower() or "stored"
        events_table_name = prefix + "_" + purpose
        recorder = PostgresAggregateRecorder(
            datastore=self.datastore, events_table_name=events_table_name
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        prefix = self.application_name.lower() or "stored"
        events_table_name = prefix + "_events"
        recorder = PostgresApplicationRecorder(
            datastore=self.datastore, events_table_name=events_table_name
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def process_recorder(self) -> ProcessRecorder:
        prefix = self.application_name.lower() or "stored"
        events_table_name = prefix + "_events"
        prefix = self.application_name.lower() or "notification"
        tracking_table_name = prefix + "_tracking"
        recorder = PostgresProcessRecorder(
            datastore=self.datastore,
            events_table_name=events_table_name,
            tracking_table_name=tracking_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def env_create_table(self) -> bool:
        default = "yes"
        return bool(strtobool(self.getenv(self.CREATE_TABLE) or default))
