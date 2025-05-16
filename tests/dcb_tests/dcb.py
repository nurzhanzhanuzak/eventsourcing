from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from threading import RLock
from typing import TYPE_CHECKING, Any, ClassVar

from psycopg.errors import DuplicateObject
from psycopg.sql import SQL, Identifier
from psycopg.types.composite import CompositeInfo, register_composite

from eventsourcing.persistence import IntegrityError, ProgrammingError
from eventsourcing.postgres import PostgresDatastore, PostgresRecorder

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from psycopg import Connection, Cursor
    from psycopg.rows import DictRow


@dataclass
class DCBQueryItem:
    types: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)


@dataclass
class DCBQuery:
    items: Sequence[DCBQueryItem] = field(default_factory=list)


@dataclass
class DCBAppendCondition:
    fail_if_events_match: DCBQuery
    after: int | None = None


@dataclass
class DCBEvent:
    type: str
    data: bytes
    tags: list[str] = field(default_factory=list)


@dataclass
class DCBSequencedEvent:
    event: DCBEvent
    position: int


class DCBEventStore(ABC):
    def get(
        self, query: DCBQuery, after: int | None = None, limit: int | None = None
    ) -> Sequence[DCBSequencedEvent]:
        return list(self.read(query=query, after=after, limit=limit))

    @abstractmethod
    def read(
        self, query: DCBQuery, after: int | None = None, limit: int | None = None
    ) -> Iterator[DCBSequencedEvent]:
        """
        Returns all events, unless 'after' is given then only those with position
        greater than 'after', and unless any query items are given, then only those
        that match at least one query item. An event matches a query item if its type
        is in the item types or there are no item types, and if all the item tags are
        in the event tags.
        """

    @abstractmethod
    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        """
        Appends given events to the event store, unless the condition fails.
        """


class InMemoryDCBEventStore(DCBEventStore):
    def __init__(self) -> None:
        self.events: list[DCBSequencedEvent] = []
        self.position_sequence = self._position_sequence_generator()
        self._lock = RLock()

    def _position_sequence_generator(self) -> Iterator[int]:
        position = 1
        while True:
            yield position
            position += 1

    def read(
        self, query: DCBQuery, after: int | None = None, limit: int | None = None
    ) -> Iterator[DCBSequencedEvent]:
        with self._lock:
            events = (
                event
                for event in self.events
                if (after is None or event.position > after)
                and (
                    not query.items
                    or any(
                        (not item.types or event.event.type in item.types)
                        and (set(event.event.tags) >= set(item.tags))
                        for item in query.items
                    )
                )
            )
            for i, event in enumerate(events):
                if limit is not None and i >= limit:
                    return
                yield event

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        with self._lock:
            if condition is not None:
                try:
                    next(
                        self.read(
                            query=condition.fail_if_events_match,
                            after=condition.after,
                        )
                    )
                    raise IntegrityError
                except StopIteration:
                    pass
            self.events.extend(
                DCBSequencedEvent(
                    position=next(self.position_sequence),
                    event=event,
                )
                for event in events
            )
            return self.events[-1].position


class PostgresDCBEventStore(DCBEventStore, PostgresRecorder):
    dcb_event_type_name = "dcb_event"
    dcb_query_item_type_name = "dcb_query_item"
    dcb_append_condition_type_name = "dcb_append_condition"
    pg_dcb_types: ClassVar[dict[str, Any]] = {}

    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        dcb_table_name: str = "dcb_events",
    ):
        super().__init__(datastore)
        self.check_table_name_length(dcb_table_name)
        self.dcb_events_table_name = dcb_table_name
        self.dcb_channel_name = dcb_table_name.replace(".", "_")

        self.create_table_statements.append(
            SQL(
                "CREATE TABLE IF NOT EXISTS {schema}.{table} ("
                "position BIGSERIAL PRIMARY KEY, "
                "type TEXT NOT NULL, "
                "data bytea, "
                "tags TEXT[] NOT NULL) "
                "WITH (autovacuum_enabled=false)"
            ).format(
                schema=Identifier(self.datastore.schema),
                table=Identifier(self.dcb_events_table_name),
            )
        )

        # Index names can't be qualified names, but
        # are created in the same schema as the table.
        self.create_table_statements.append(
            SQL("CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table}(type)").format(
                index=Identifier(self.dcb_events_table_name + "_type_idx"),
                schema=Identifier(self.datastore.schema),
                table=Identifier(self.dcb_events_table_name),
            )
        )
        self.create_table_statements.append(
            SQL(
                "CREATE INDEX IF NOT EXISTS {index} "
                "ON {schema}.{table} USING GIN (tags)"
            ).format(
                index=Identifier(self.dcb_events_table_name + "_type_idx"),
                schema=Identifier(self.datastore.schema),
                table=Identifier(self.dcb_events_table_name),
            )
        )

        # Actually just get a DuplicateObject error from psycopg:
        # For later: https://stackoverflow.com/questions/7624919/
        # check-if-a-user-defined-type-already-exists-in-postgresql

        # https://www.psycopg.org/psycopg3/docs/basic/pgtypes.html
        # #composite-types-casting
        self.drop_dcb_event_type_statement = SQL(
            "DROP TYPE IF EXISTS {0}.{1} CASCADE"
        ).format(
            Identifier(self.datastore.schema),
            Identifier(self.dcb_event_type_name),
        )
        self.create_dcb_event_type_statement = SQL(
            "CREATE TYPE {0}.{1} AS (type TEXT, data bytea, tags TEXT[]) "
        ).format(
            Identifier(self.datastore.schema),
            Identifier(self.dcb_event_type_name),
        )

        self.drop_dcb_query_item_type_statement = SQL(
            "DROP TYPE IF EXISTS {0}.{1} CASCADE"
        ).format(
            Identifier(self.datastore.schema),
            Identifier(self.dcb_query_item_type_name),
        )

        self.create_dcb_query_item_type_statement = SQL(
            "CREATE TYPE {0}.{1} AS (types TEXT[], tags TEXT[]) "
        ).format(
            Identifier(self.datastore.schema),
            Identifier(self.dcb_query_item_type_name),
        )

        self.drop_dcb_append_condition_type_statement = SQL(
            "DROP TYPE IF EXISTS {0}.{1} CASCADE"
        ).format(
            Identifier(self.datastore.schema),
            Identifier(self.dcb_append_condition_type_name),
        )

        self.create_dcb_append_condition_type_statement = SQL(
            "CREATE TYPE {0}.{1} AS (query_items {2}[], after BIGINT) "
        ).format(
            Identifier(self.datastore.schema),
            Identifier(self.dcb_append_condition_type_name),
            Identifier(self.dcb_query_item_type_name),
        )

        self.dcb_insert_events_function_name = "dcb_insert_events"
        self.dcb_select_events_function_name = "dcb_select_events"
        self.dcb_append_events_procedure_name = "dcb_append_events"

        # Define DCB insert events function.
        self.create_table_statements.append(
            SQL(
                "CREATE OR REPLACE FUNCTION {0} ( events dcb_event[] ) "
                "RETURNS TABLE ( "
                "    posn bigint "
                ") "
                "LANGUAGE plpgsql "
                "AS "
                "$BODY$ "
                "BEGIN "
                "    RETURN QUERY "
                "    INSERT INTO {1}.{2} (type, data, tags) "
                "    SELECT * from unnest(events) "
                "    RETURNING position; "
                "END; "
                "$BODY$; "
            ).format(
                Identifier(self.dcb_insert_events_function_name),
                Identifier(self.datastore.schema),
                Identifier(self.dcb_events_table_name),
            )
        )

        # Define DCB select events function.
        self.create_table_statements.append(
            SQL(
                "CREATE OR REPLACE FUNCTION {0} ("
                "    query_items dcb_query_item[], "
                "    after BIGINT, "
                "    max_results BIGINT DEFAULT NULL, "
                "    fail_fast boolean DEFAULT false "
                ") "
                "RETURNS TABLE ( "
                "    posn BIGINT, "
                "    type TEXT, "
                "    data bytea, "
                "    tags TEXT[] "
                ") "
                "LANGUAGE plpgsql "
                "AS "
                "$BODY$ "
                "DECLARE"
                "    query_item dcb_query_item;"
                "    internal_max integer DEFAULT NULL;"
                "BEGIN"
                "    after = COALESCE(after, 0);"
                "    IF fail_fast THEN "
                "        internal_max = 1;"
                "    END IF;"
                "    IF array_length(query_items, 1) IS NOT NULL THEN "
                "        CREATE TEMP TABLE temp_results ("
                "            tmpposn BIGINT, "
                "            tmptype TEXT, "
                "            tmpdata bytea, "
                "            tmptags TEXT[]"
                "        ) ON COMMIT DROP;"
                "        FOREACH query_item IN ARRAY query_items"
                "        LOOP"
                "            IF array_length(query_item.types, 1) IS NOT NULL THEN "
                "                IF array_length(query_item.tags, 1) IS NOT NULL THEN "
                "                    INSERT INTO temp_results ("
                "                       tmpposn, tmptype, tmpdata, tmptags"
                "                    ) "
                "                    SELECT * from {1}.{2} e "
                "                    WHERE e.tags @> query_item.tags "
                "                    AND e.type = ANY (query_item.types) "
                "                    AND position > after "
                "                    ORDER BY position ASC LIMIT internal_max;"
                "                ELSE"
                "                    INSERT INTO temp_results ("
                "                        tmpposn, tmptype, tmpdata, tmptags"
                "                    ) "
                "                    SELECT * from {1}.{2} e "
                "                    WHERE e.type = ANY(query_item.types) "
                "                    AND position > after "
                "                    ORDER BY position ASC LIMIT internal_max;"
                "                END IF;"
                "            ELSE"
                "                INSERT INTO temp_results ("
                "                    tmpposn, tmptype, tmpdata, tmptags"
                "                ) "
                "                SELECT * from {1}.{2} e "
                "                WHERE e.tags @> query_item.tags "
                "                AND position > after "
                "                ORDER BY position ASC LIMIT internal_max;"
                "            END IF;"
                "            IF fail_fast AND EXISTS (SELECT FROM temp_results) THEN"
                "                EXIT;"
                "            END IF;"
                "        END LOOP;"
                "        RETURN QUERY SELECT tmpposn, tmptype, tmpdata, tmptags"
                "        FROM temp_results ORDER BY tmpposn ASC LIMIT max_results;"
                "    ELSE"
                "        RETURN QUERY SELECT * from {1}.{2} "
                "        WHERE position > after "
                "        ORDER BY position ASC LIMIT max_results;"
                "    END IF;"
                "END; "
                "$BODY$;"
            ).format(
                Identifier(self.dcb_select_events_function_name),
                Identifier(self.datastore.schema),
                Identifier(self.dcb_events_table_name),
            )
        )

        # Define DCB append events procedure.
        # Acquire "EXCLUSIVE" table lock, to serialize transactions that insert
        # stored events, so that readers don't pass over gaps that are filled in
        # later. We want each transaction that will be issued with notification
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
        self.create_table_statements.append(
            SQL(
                "CREATE OR REPLACE PROCEDURE {append_events} ("
                "    in events dcb_event[],"
                "    in query_items dcb_query_item[],"
                "    inout after BIGINT"
                ") "
                "LANGUAGE plpgsql "
                "AS "
                "$BODY$ "
                "BEGIN "
                "    LOCK TABLE {schema}.{table} IN EXCLUSIVE MODE;"
                "    IF (after < 0) "
                "    OR (NOT EXISTS ("
                "        SELECT FROM {select_events}(query_items, after, 1, True)"
                "    )) "
                "    THEN"
                "        SELECT MAX(posn) FROM {insert_events}(events) INTO after;"
                "        NOTIFY {channel};"
                "    else"
                "        after = -1;"
                "    end if;"
                "    return;"
                "END; "
                "$BODY$; "
            ).format(
                append_events=Identifier(self.dcb_append_events_procedure_name),
                select_events=Identifier(self.dcb_select_events_function_name),
                insert_events=Identifier(self.dcb_insert_events_function_name),
                schema=Identifier(self.datastore.schema),
                table=Identifier(self.dcb_events_table_name),
                lock_timeout=self.datastore.lock_timeout,
                channel=Identifier(self.dcb_channel_name),
            )
        )

        self.select_events_statement = SQL(
            "SELECT * FROM dcb_select_events((%s), (%s), (%s))"
        )
        self.insert_events_statement = SQL("SELECT * FROM dcb_insert_events((%s))")
        self.append_events_statement = SQL("CALL dcb_append_events((%s), (%s), (%s))")

        self.lock_table_statements = [
            SQL("SET LOCAL lock_timeout = '{0}s'").format(self.datastore.lock_timeout),
            SQL("LOCK TABLE {0}.{1} IN EXCLUSIVE MODE").format(
                Identifier(self.datastore.schema),
                Identifier(self.dcb_events_table_name),
            ),
        ]

    def _create_table(self, curs: Cursor[DictRow]) -> None:
        # Create types.
        with contextlib.suppress(DuplicateObject):
            curs.execute(self.create_dcb_event_type_statement)
        with contextlib.suppress(DuplicateObject):
            curs.execute(self.create_dcb_query_item_type_statement)
        with contextlib.suppress(DuplicateObject):
            curs.execute(self.create_dcb_append_condition_type_statement)
        type(self).register_type_adapters(curs.connection)
        super()._create_table(curs)

    def drop_types(self) -> None:
        with self.datastore.get_connection() as conn:
            for statement in [
                self.drop_dcb_event_type_statement,
                self.drop_dcb_query_item_type_statement,
                self.drop_dcb_append_condition_type_statement,
            ]:
                conn.execute(statement)

    # https://www.psycopg.org/psycopg3/docs/basic/pgtypes.html#composite-types-casting
    @classmethod
    def register_type_adapters(cls, conn: Connection[Any]) -> None:
        type_names = [
            cls.dcb_event_type_name,
            cls.dcb_query_item_type_name,
            cls.dcb_append_condition_type_name,
        ]
        for name in type_names:
            info = CompositeInfo.fetch(conn, name)
            if info is not None:
                register_composite(info, conn)
                cls.pg_dcb_types[name] = info

    @property
    def pg_dcb_event(self) -> Any:
        return type(self).pg_dcb_types[type(self).dcb_event_type_name].python_type

    @property
    def pg_query_item(self) -> Any:
        return type(self).pg_dcb_types[type(self).dcb_query_item_type_name].python_type

    @property
    def pg_append_condition(self) -> Any:
        return (
            type(self)
            .pg_dcb_types[type(self).dcb_append_condition_type_name]
            .python_type
        )

    def read(
        self, query: DCBQuery, after: int | None = None, limit: int | None = None
    ) -> Iterator[DCBSequencedEvent]:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            return self._read(curs, query, after, limit)

    def _read(
        self,
        curs: Cursor[DictRow],
        query: DCBQuery,
        after: int | None = None,
        limit: int | None = None,
    ) -> Iterator[DCBSequencedEvent]:
        # Construct parameters using pg composite type.
        pg_query_items = [
            self.pg_query_item(item.types, item.tags) for item in query.items
        ]
        # Execute the select statement.
        curs.execute(self.select_events_statement, (pg_query_items, after or 0, limit))
        return (
            DCBSequencedEvent(
                event=DCBEvent(
                    type=row["type"],
                    data=row["data"],
                    tags=row["tags"],
                ),
                position=row["posn"],
            )
            for row in curs.fetchall()
        )

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        len_events = len(events)
        if len_events == 0:
            msg = "Should be at least one event. Avoid this elsewhere"
            raise ProgrammingError(msg)
        with self.datastore.transaction(commit=True) as curs:

            # Construct parameters using pg composite type.
            pg_dcb_events = [
                self.pg_dcb_event(event.type, event.data, event.tags)
                for event in events
            ]
            if condition is not None:
                pg_dcb_query_items = [
                    self.pg_query_item(types=item.types, tags=item.tags)
                    for item in condition.fail_if_events_match.items
                ]
                after = condition.after or 0
                assert after >= 0
            else:
                pg_dcb_query_items = []
                after = -1  # Indicates no "fail condition".
            params = (
                pg_dcb_events,
                pg_dcb_query_items,
                after,
            )

            # Execute append statement.
            result = curs.execute(self.append_events_statement, params)

            # Get the last new position.
            last_new_position = result.fetchall()[-1]["after"]
            if last_new_position < 0:
                raise IntegrityError
            return last_new_position
