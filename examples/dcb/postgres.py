from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, TypedDict, cast

from psycopg.errors import DuplicateObject
from psycopg.sql import SQL, Identifier

from eventsourcing.persistence import IntegrityError, ProgrammingError
from eventsourcing.postgres import (
    PostgresDatastore,
    PostgresFactory,
    PostgresRecorder,
    PostgresTrackingRecorder,
)
from examples.dcb.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBEventStore,
    DCBInfrastructureFactory,
    DCBQuery,
    DCBQueryItem,
    DCBSequencedEvent,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence


class PostgresDCBEventStore(DCBEventStore, PostgresRecorder):
    pg_composite_type_adapters: ClassVar[dict[str, Any]] = {}

    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        dcb_table_name: str = "dcb_events",
    ):
        super().__init__(datastore)
        self.dcb_event_type_name = "dcb_event"
        self.dcb_query_item_type_name = "dcb_query_item"
        self.datastore.pg_type_names.update(
            [
                self.dcb_event_type_name,
            ]
        )
        self.check_table_name_length(dcb_table_name)
        self.dcb_events_table_name = dcb_table_name
        self.dcb_channel_name = dcb_table_name.replace(".", "_")

        self.sql_create_table = SQL(
            "CREATE TABLE IF NOT EXISTS {schema}.{table} ("
            "position BIGSERIAL PRIMARY KEY, "
            "type TEXT NOT NULL, "
            "data bytea, "
            "tags TEXT[] NOT NULL,"
            "text_vector tsvector) "
            "WITH (autovacuum_enabled=false)"
        ).format(
            schema=Identifier(self.datastore.schema),
            table=Identifier(self.dcb_events_table_name),
        )

        self.sql_create_index_on_text_vector = SQL(
            "CREATE INDEX IF NOT EXISTS {index} "
            "ON {schema}.{table} "
            "USING GIN (text_vector)"
        ).format(
            index=Identifier(self.dcb_events_table_name + "_text_vector_idx"),
            schema=Identifier(self.datastore.schema),
            table=Identifier(self.dcb_events_table_name),
        )

        # https://www.psycopg.org/psycopg3/docs/basic/pgtypes.html
        # #composite-types-casting
        # Can't do 'CREATE TYPE IF NOT EXISTS', but if exists we get a DuplicateObject
        # error from psycopg (which terminates the transaction), but maybe do:
        # https://stackoverflow.com/questions/7624919/
        # check-if-a-user-defined-type-already-exists-in-postgresql
        self.sql_create_type_dcb_event_type = SQL(
            "CREATE TYPE {schema}.{name} "
            "AS (type TEXT, data bytea, tags TEXT[], text_vector tsvector)"
        ).format(
            schema=Identifier(self.datastore.schema),
            name=Identifier(self.dcb_event_type_name),
        )

        self.pg_function_name_insert_events = "dcb_insert_events"

        self.sql_invoke_pg_insert_events_function = SQL(
            "SELECT * FROM {insert_events}((%s))"
        ).format(insert_events=Identifier(self.pg_function_name_insert_events))

        self.sql_create_pg_function_insert_events = SQL(
            "CREATE OR REPLACE FUNCTION {insert_events} ( events {event}[] ) "
            "RETURNS TABLE ( "
            "    posn bigint "
            ") "
            "LANGUAGE plpgsql "
            "AS "
            "$BODY$ "
            "BEGIN "
            "    RETURN QUERY "
            "    INSERT INTO {schema}.{table} (type, data, tags, text_vector) "
            "    SELECT type, data, tags, text_vector from unnest(events) "
            "    RETURNING position; "
            "END; "
            "$BODY$; "
        ).format(
            insert_events=Identifier(self.pg_function_name_insert_events),
            event=Identifier(self.dcb_event_type_name),
            schema=Identifier(self.datastore.schema),
            table=Identifier(self.dcb_events_table_name),
        )

        self.pg_function_name_select_events = "dcb_select_events"

        self.sql_invoke_pg_select_events_function = SQL(
            "SELECT * FROM {select_events}((%s), (%s), (%s))"
        ).format(select_events=Identifier(self.pg_function_name_select_events))

        self.sql_create_pg_function_select_events = SQL(
            "CREATE OR REPLACE FUNCTION {select_events} ("
            "    text_query tsquery, "
            "    after BIGINT, "
            "    max_results BIGINT DEFAULT NULL, "
            "    unsorted boolean DEFAULT FALSE "
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
            "BEGIN"
            "    IF text_query <> '' THEN "
            "        IF after is NULL THEN "
            "            RETURN QUERY "
            "            SELECT t.position, t.type, t.data, t.tags "
            "            FROM {schema}.{table} t "
            "            WHERE t.text_vector @@ text_query "
            "            ORDER BY t.position ASC "
            "            LIMIT max_results;"
            "        ELSIF unsorted THEN"
            "            RETURN QUERY "
            "            SELECT t.position, t.type, t.data, t.tags "
            "            FROM {schema}.{table} t "
            "            WHERE t.text_vector @@ text_query "
            "            AND t.position > COALESCE(after, 0) "
            "            LIMIT max_results;"
            "        ELSE"
            "            RETURN QUERY "
            "            SELECT t.position, t.type, t.data, t.tags "
            "            FROM {schema}.{table} t "
            "            WHERE t.text_vector @@ text_query "
            "            AND t.position > COALESCE(after, 0) "
            "            ORDER BY t.position ASC "
            "            LIMIT max_results;"
            "        END IF;"
            "    ELSE"
            "        /* no text query - return limited sorted rows from table */"
            "        RETURN QUERY SELECT t.position, t.type, t.data, t.tags "
            "        FROM {schema}.{table} t "
            "        WHERE t.position > COALESCE(after, 0) "
            "        ORDER BY t.position ASC LIMIT max_results;"
            "    END IF;"
            "END; "
            "$BODY$;"
        ).format(
            select_events=Identifier(self.pg_function_name_select_events),
            schema=Identifier(self.datastore.schema),
            table=Identifier(self.dcb_events_table_name),
        )

        self.pg_procedure_name_append_events = "dcb_append_events"

        self.sql_invoke_pg_append_events_procedure = SQL(
            "CALL {append_events}((%s), (%s), (%s))"
        ).format(
            append_events=Identifier(self.pg_procedure_name_append_events),
        )

        self.sql_create_pg_procedure_append_events = SQL(
            "CREATE OR REPLACE PROCEDURE {append_events} ("
            "    in events dcb_event[],"
            "    in text_query tsquery,"
            "    inout after BIGINT"
            ") "
            "LANGUAGE plpgsql "
            "AS "
            "$BODY$ "
            "DECLARE "
            "    num_rows integer;"
            "BEGIN "
            "    IF (after < 0) OR (SELECT COUNT(*) FROM"
            "    {select_events}(text_query, after, 1, TRUE)) = 0 "
            "    THEN"
            "        SELECT MAX(posn) FROM {insert_events}(events) INTO after;"
            "        NOTIFY {channel};"
            "    ELSE"
            "        after = -1;"
            "    END IF;"
            "    RETURN;"
            "END; "
            "$BODY$; "
        ).format(
            append_events=Identifier(self.pg_procedure_name_append_events),
            select_events=Identifier(self.pg_function_name_select_events),
            insert_events=Identifier(self.pg_function_name_insert_events),
            schema=Identifier(self.datastore.schema),
            table=Identifier(self.dcb_events_table_name),
            lock_timeout=self.datastore.lock_timeout,
            channel=Identifier(self.dcb_channel_name),
        )

        # Extend statements executed when create_table() is called. Don't do any
        # 'CREATE TYPE' statements in that transaction because if types exist, the
        # transaction is aborted, causing opaque errors when running the test suite.
        self.create_table_statements.extend(
            [
                self.sql_create_table,
                self.sql_create_index_on_text_vector,
                self.sql_create_pg_function_insert_events,
                self.sql_create_pg_function_select_events,
                self.sql_create_pg_procedure_append_events,
            ]
        )

    def create_table(self) -> None:
        # Create types each in their own transaction (see above).
        with (
            self.datastore.transaction(commit=True) as curs,
            contextlib.suppress(DuplicateObject),
        ):
            curs.execute(self.sql_create_type_dcb_event_type)
        super().create_table()

    def read(
        self,
        query: DCBQuery | None = None,
        after: int | None = None,
        limit: int | None = None,
    ) -> Iterator[DCBSequencedEvent]:
        # Prepare arguments and invoke pg function.
        if not query or not query.items:
            text_query = ""
        else:
            text_query = self.construct_text_query(query.items)
        return (
            DCBSequencedEvent(
                event=DCBEvent(
                    type=row["type"],
                    data=row["data"],
                    tags=row["tags"],
                ),
                position=row["posn"],
            )
            for row in self.invoke_pg_select_events_function(
                text_query=text_query,
                after=after,
                limit=limit,
            )
        )

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        if len(events) == 0:
            msg = "Should be at least one event. Avoid this elsewhere"
            raise ProgrammingError(msg)

        # Prepare events argument.
        pg_dcb_events = [
            self.construct_pg_dcb_event(
                type=event.type,
                data=event.data,
                tags=event.tags,
            )
            for event in events
        ]

        # Prepare query and after argument.
        text_query = ""
        if condition:
            if condition.fail_if_events_match.items:
                text_query = self.construct_text_query(
                    condition.fail_if_events_match.items,
                )
            after = condition.after
        else:
            after = -1  # Indicates no "fail condition".

        # Invoke pg procedure.
        last_new_position = self.invoke_pg_append_events_procedure(
            events=pg_dcb_events,
            text_query=text_query,
            after=after,
        )
        assert last_new_position is not None  # Because 'events' wasn't empty.
        if last_new_position == -1:  # Indicates "fail condition" failed.
            raise IntegrityError
        return last_new_position

    def invoke_pg_insert_events_function(
        self,
        events: list[PgDCBEvent],
    ) -> list[int]:
        with self.datastore.get_connection() as conn:
            results = conn.execute(
                self.sql_invoke_pg_insert_events_function,
                (events,),
            ).fetchall()
            assert results is not None
            return [r["posn"] for r in results]

    def invoke_pg_select_events_function(
        self,
        text_query: str = "",
        after: int | None = None,
        limit: int | None = None,
    ) -> Iterator[PgDCBEventRow]:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            curs.execute(
                self.sql_invoke_pg_select_events_function,
                (text_query, after, limit),
            )
            return cast("Iterator[PgDCBEventRow]", curs.fetchall())

    def invoke_pg_append_events_procedure(
        self,
        events: list[PgDCBEvent],
        text_query: str = "",
        after: int | None = None,
    ) -> int | None:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            result = curs.execute(
                self.sql_invoke_pg_append_events_procedure,
                [events, text_query, after],
            )
            return result.fetchall()[-1]["after"]

    def construct_pg_dcb_event(
        self,
        type: str,  # noqa: A002
        data: bytes,
        tags: list[str],
    ) -> PgDCBEvent:
        return self.datastore.pg_type_adapters[self.dcb_event_type_name].python_type(
            type, data, tags, self.construct_text_vector(type, tags)
        )

    def construct_text_vector(self, type: str, tags: list[str]) -> str:  # noqa: A002
        assert type or tags
        type = type.replace(":", "-")  # noqa: A001
        tags = [t.replace(":", "-") for t in tags]
        all_tokens = [type, *tags]
        # Check for reserved prefixes.
        for reserved_prefix in ["TYPE-", "TAG-"]:
            assert not any(
                t.startswith(reserved_prefix) for t in all_tokens
            ), reserved_prefix
        # Check for reserved chars.
        for reserved_char in ":&|()":
            assert not any(reserved_char in t for t in all_tokens), (
                reserved_char,
                all_tokens,
            )
        # Prefix and join.
        return " ".join([f"TYPE-{type}"] + [f"TAG-{tag}" for tag in tags])

    def construct_text_query(self, query_items: list[DCBQueryItem]) -> str:
        text_queries = [
            self.construct_text_query_from_types_and_tags(types=q.types, tags=q.tags)
            for q in query_items
        ]
        return " | ".join([f"({t})" for t in text_queries])

    def construct_text_query_from_types_and_tags(
        self, types: list[str], tags: list[str]
    ) -> str:
        assert types or tags
        # TODO: Check for reserved prefixes, chars, words.
        tstags = [t.replace(":", "-") for t in tags]
        tstypes = [t.replace(":", "-") for t in types]
        prefixed_types = [f"TYPE-{type}" for type in tstypes]  # noqa: A001
        prefixed_tags = [f"TAG-{tag}" for tag in tstags]
        types_query = " | ".join(prefixed_types)
        tags_query = " & ".join(prefixed_tags)
        if types_query and tags_query:
            text_query = f"({types_query}) & {tags_query}"
        else:
            text_query = types_query or tags_query
        return text_query


class PgDCBEvent(NamedTuple):
    type: str
    data: bytes
    tags: list[str]
    text_vector: str


class PgDCBQueryItem(NamedTuple):
    types: list[str]
    tags: list[str]
    text_query: str


class PgDCBEventRow(TypedDict):
    posn: int
    type: str
    data: bytes
    tags: list[str]


class DCBPostgresFactory(
    PostgresFactory,
    DCBInfrastructureFactory[PostgresTrackingRecorder],
):
    def dcb_event_store(self) -> DCBEventStore:
        prefix = self.env.name.lower() or "dcb"

        dcb_table_name = prefix + "_events"
        recorder = PostgresDCBEventStore(
            datastore=self.datastore,
            dcb_table_name=dcb_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder
