from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple, TypedDict, cast

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
        self.dcb_event_type_name = "dcb_event"
        self.dcb_sequenced_event_type_name = "dcb_sequenced_event"
        self.datastore.pg_type_names.add(self.dcb_event_type_name)
        self.datastore.pg_type_names.add(self.dcb_sequenced_event_type_name)

        self.sql_create_table = SQL(
            "CREATE TABLE IF NOT EXISTS {schema}.{table} ("
            "sequence_position bigserial PRIMARY KEY, "
            "type text NOT NULL, "
            "data bytea, "
            "tags text[] NOT NULL,"
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

        self.sql_create_type_dcb_event = SQL(
            "CREATE TYPE {schema}.{type_name} "
            "AS (type text, data bytea, tags text[], text_vector tsvector)"
        ).format(
            schema=Identifier(self.datastore.schema),
            type_name=Identifier(self.dcb_event_type_name),
        )

        self.sql_create_type_dcb_sequenced_event = SQL(
            "CREATE TYPE {schema}.{type_name} "
            "AS (sequence_position bigint, type text, data bytea, tags text[])"
        ).format(
            schema=Identifier(self.datastore.schema),
            type_name=Identifier(self.dcb_sequenced_event_type_name),
        )

        self.pg_function_name_insert_events = "dcb_insert_events"
        self.sql_create_pg_function_insert_events = SQL(
            "CREATE OR REPLACE FUNCTION {insert_events}("
            "    events {schema}.{event_type}[]"
            ") "
            "RETURNS TABLE ("
            "    sequence_position bigint"
            ") "
            "LANGUAGE plpgsql "
            "AS "
            "$BODY$"
            "BEGIN"
            "    RETURN QUERY"
            "    INSERT INTO {schema}.{table} AS t (type, data, tags, text_vector)"
            "    SELECT type, data, tags, text_vector FROM unnest(events)"
            "    RETURNING t.sequence_position;"
            "END;"
            "$BODY$"
        ).format(
            insert_events=Identifier(self.pg_function_name_insert_events),
            schema=Identifier(self.datastore.schema),
            event_type=Identifier(self.dcb_event_type_name),
            table=Identifier(self.dcb_events_table_name),
        )

        self.pg_function_name_select_events = "dcb_select_events"
        self.sql_invoke_pg_function_select_events = SQL(
            "SELECT * FROM {select_events}((%s), (%s), (%s))"
        ).format(select_events=Identifier(self.pg_function_name_select_events))
        self.sql_create_pg_function_select_events = SQL(
            "CREATE OR REPLACE FUNCTION {select_events} ("
            "    text_query tsquery,"
            "    after bigint,"
            "    max_results bigint DEFAULT NULL,"
            "    unsorted boolean DEFAULT FALSE"
            ") "
            "RETURNS TABLE ("
            "    sequence_position bigint,"
            "    type text,"
            "    data bytea,"
            "    tags text[]"
            ") "
            "LANGUAGE plpgsql "
            "AS "
            "$BODY$"
            "BEGIN"
            "    IF text_query <> '' THEN"
            "        IF unsorted THEN"
            "            /* for append condition query */"
            "            RETURN QUERY"
            "            SELECT t.sequence_position, t.type, t.data, t.tags"
            "            FROM {schema}.{table} t"
            "            WHERE t.sequence_position > COALESCE(after, 0)"
            "            AND t.text_vector @@ text_query"
            "            LIMIT max_results;"
            "        ELSIF after is NULL THEN"
            "            /* for initial command query */"
            "            RETURN QUERY"
            "            SELECT t.sequence_position, t.type, t.data, t.tags"
            "            FROM {schema}.{table} t"
            "            WHERE t.text_vector @@ text_query"
            "            ORDER BY t.sequence_position ASC"
            "            LIMIT max_results;"
            "        ELSE"
            "            /* more unusual to get here */"
            "            RETURN QUERY"
            "            SELECT t.sequence_position, t.type, t.data, t.tags"
            "            FROM {schema}.{table} t"
            "            WHERE t.text_vector @@ text_query"
            "            AND t.sequence_position > COALESCE(after, 0)"
            "            ORDER BY t.sequence_position ASC"
            "            LIMIT max_results;"
            "        END IF;"
            "    ELSE"
            "        /* no text query - return limited sorted rows from table */"
            "        RETURN QUERY SELECT t.sequence_position, t.type, t.data, t.tags"
            "        FROM {schema}.{table} t"
            "        WHERE t.sequence_position > COALESCE(after, 0)"
            "        ORDER BY t.sequence_position ASC LIMIT max_results;"
            "    END IF;"
            "END;"
            "$BODY$"
        ).format(
            select_events=Identifier(self.pg_function_name_select_events),
            schema=Identifier(self.datastore.schema),
            table=Identifier(self.dcb_events_table_name),
        )

        self.pg_function_name_select_events2 = "dcb_select_events2"
        self.sql_invoke_pg_function_select_events2 = SQL(
            "SELECT * FROM {select_events2}((%s), (%s), (%s), (%s))"
        ).format(select_events2=Identifier(self.pg_function_name_select_events2))
        self.sql_create_pg_function_select_events2 = SQL(
            "CREATE OR REPLACE FUNCTION {select_events2} ("
            "    text_query tsquery,"
            "    after bigint,"
            "    max_results bigint DEFAULT NULL,"
            "    unsorted boolean DEFAULT FALSE,"
            "    OUT result_array {sequenced_event_type}[],"
            "    OUT result_integer integer"
            ") "
            "LANGUAGE plpgsql "
            "AS "
            "$BODY$ "
            "BEGIN "
            # "    result_array := ARRAY[(1, NULL, NULL, NULL)::{sequenced_event_type}, (2, NULL, NULL, NULL)::{sequenced_event_type}];"
            # "    result_integer := 42; "
            "    SELECT MAX(sequence_position) "
            "    FROM {schema}.{table}"
            "    INTO result_integer;"
            "    IF text_query <> '' THEN"
            "        IF unsorted THEN"
            "            /* for append condition query - no ordering */"
            "            SELECT ARRAY_AGG("
            "                ("
            "                    subq.sequence_position,"
            "                    subq.type, "
            "                    subq.data,"
            "                    subq.tags"
            "                )::{sequenced_event_type}"
            "            )"
            "            FROM ("
            "                SELECT sequence_position, type, data, tags"
            "                FROM {schema}.{table}"
            "                WHERE sequence_position > COALESCE(after, 0)"
            "                AND text_vector @@ text_query"
            "                LIMIT max_results"
            "            ) AS subq"
            "            INTO result_array;"
            "        ELSIF after is NULL THEN"
            # "            /* for initial command query - no 'after' */"
            "            SELECT ARRAY_AGG("
            "                ("
            "                    subq.sequence_position,"
            "                    subq.type, "
            "                    subq.data,"
            "                    subq.tags"
            "                )::{sequenced_event_type}"
            "            )"
            "            FROM ("
            "                SELECT sequence_position, type, data, tags"
            "                FROM {schema}.{table}"
            "                WHERE text_vector @@ text_query"
            "                ORDER BY sequence_position ASC"
            "                LIMIT max_results"
            "            ) AS subq"
            "            INTO result_array;"
            "        ELSE"
            "            /* more unusual to get here - query after and ordered */"
            "            SELECT ARRAY_AGG("
            "                ("
            "                    subq.sequence_position,"
            "                    subq.type, "
            "                    subq.data,"
            "                    subq.tags"
            "                )::{sequenced_event_type}"
            "            )"
            "            FROM ("
            "                SELECT sequence_position, type, data, tags"
            "                FROM {schema}.{table}"
            "                WHERE text_vector @@ text_query"
            "                AND sequence_position > COALESCE(after, 0)"
            "                ORDER BY sequence_position ASC"
            "                LIMIT max_results"
            "            ) AS subq"
            "            INTO result_array;"
            "        END IF;"
            "    ELSE"
            "        /* no text query - return limited sorted rows from table */"
            "        SELECT ARRAY_AGG("
            "            ("
            "                subq.sequence_position,"
            "                subq.type,"
            "                subq.data,"
            "                subq.tags"
            "            )::{sequenced_event_type}"
            "        )"
            "        FROM ("
            "            SELECT sequence_position, type, data, tags"
            "            FROM {schema}.{table}"
            "            WHERE sequence_position > COALESCE(after, 0)"
            "            ORDER BY sequence_position ASC"
            "            LIMIT max_results"
            "        ) AS subq"
            "        INTO result_array;"
            "END IF;"
            "END;"
            "$BODY$"
        ).format(
            select_events2=Identifier(self.pg_function_name_select_events2),
            sequenced_event_type=Identifier(self.dcb_sequenced_event_type_name),
            schema=Identifier(self.datastore.schema),
            table=Identifier(self.dcb_events_table_name),
        )

        self.pg_procedure_name_append_events = "dcb_append_events"
        self.sql_invoke_pg_procedure_append_events = SQL(
            "CALL {append_events}((%s), (%s), (%s))"
        ).format(
            append_events=Identifier(self.pg_procedure_name_append_events),
        )
        self.sql_create_pg_procedure_append_events = SQL(
            "CREATE OR REPLACE PROCEDURE {append_events} ("
            "    in events {schema}.{event_type}[],"
            "    in text_query tsquery,"
            "    inout after bigint"
            ") "
            "LANGUAGE plpgsql "
            "AS "
            "$BODY$ "
            "DECLARE"
            "    num_rows integer; "
            "BEGIN "
            "    IF (after < 0) OR (SELECT COUNT(*) FROM"
            "        {select_events}(text_query, after, 1, TRUE)) = 0"
            "    THEN"
            "        SELECT MAX(sequence_position) "
            "        FROM {insert_events}(events) "
            "        INTO after;"
            "        NOTIFY {channel};"
            "    ELSE"
            "        after = -1;"
            "    END IF;"
            "    RETURN;"
            "END;"
            "$BODY$"
        ).format(
            append_events=Identifier(self.pg_procedure_name_append_events),
            schema=Identifier(self.datastore.schema),
            event_type=Identifier(self.dcb_event_type_name),
            select_events=Identifier(self.pg_function_name_select_events),
            insert_events=Identifier(self.pg_function_name_insert_events),
            channel=Identifier(self.dcb_channel_name),
        )

        self.sql_create_statements.extend(
            [
                self.sql_create_table,
                self.sql_create_index_on_text_vector,
                self.sql_create_type_dcb_event,
                self.sql_create_type_dcb_sequenced_event,
                self.sql_create_pg_function_insert_events,
                self.sql_create_pg_function_select_events,
                self.sql_create_pg_function_select_events2,
                self.sql_create_pg_procedure_append_events,
            ]
        )

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
                position=row["sequence_position"],
            )
            for row in self.invoke_pg_function_select_events(
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

        # Prepare 'events' argument.
        pg_dcb_events = [
            self.construct_pg_dcb_event(
                type=event.type,
                data=event.data,
                tags=event.tags,
            )
            for event in events
        ]

        # Prepare 'text_query' and 'after' arguments.
        text_query = ""
        if condition:
            if condition.fail_if_events_match.items:
                text_query = self.construct_text_query(
                    condition.fail_if_events_match.items,
                )
            after = condition.after
            print("Append condition after:", after)  # noqa: T201
        else:
            after = -1  # Indicates no "fail condition".

        # Invoke pg procedure.
        last_new_sequence_position = self.invoke_pg_procedure_append_events(
            events=pg_dcb_events,
            text_query=text_query,
            after=after,
        )
        assert last_new_sequence_position is not None  # Because 'events' wasn't empty.
        if last_new_sequence_position == -1:  # Indicates "fail condition" failed.
            raise IntegrityError
        return last_new_sequence_position

    def construct_text_query(self, query_items: list[DCBQueryItem]) -> str:
        text_queries = [
            self.construct_text_query_from_query_item(query_item)
            for query_item in query_items
        ]
        return " | ".join([f"({t})" for t in text_queries])

    def construct_text_query_from_query_item(self, query_item: DCBQueryItem) -> str:
        types = self.prefix_types(self.replace_reserved_chars(query_item.types))
        tags = self.prefix_tags(self.replace_reserved_chars(query_item.tags))
        types_tq = " | ".join(types)
        tags_tq = " & ".join(tags)
        return f"({types_tq}) & {tags_tq}" if types and tags else types_tq or tags_tq

    def construct_text_vector(self, type: str, tags: list[str]) -> str:  # noqa: A002
        self.assert_no_reserved_prefixes([type, *tags])
        type = self.prefix_types(self.replace_reserved_chars([type]))[0]  # noqa: A001
        tags = self.prefix_tags(self.replace_reserved_chars(tags))
        return " ".join([type, *tags])

    def assert_no_reserved_prefixes(self, all_tokens: list[str]) -> None:
        for reserved_prefix in ["TYPE-", "TAG-"]:
            assert not any(
                t.startswith(reserved_prefix) for t in all_tokens
            ), reserved_prefix

    def replace_reserved_chars(self, all_tokens: list[str]) -> list[str]:
        for reserved in ":&|()":
            all_tokens = [t.replace(reserved, "-") for t in all_tokens]
        return all_tokens

    def prefix_types(self, all_tokens: list[str]) -> list[str]:
        return [f"TYPE-{t}" for t in all_tokens]

    def prefix_tags(self, all_tokens: list[str]) -> list[str]:
        return [f"TAG-{t}" for t in all_tokens]

    def invoke_pg_function_select_events(
        self,
        text_query: str = "",
        after: int | None = None,
        limit: int | None = None,
    ) -> Iterator[PgDCBEventRow]:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            curs.execute(
                self.sql_invoke_pg_function_select_events,
                (text_query, after, limit),
            )
            return cast("Iterator[PgDCBEventRow]", curs.fetchall())

    def invoke_pg_function_select_events2(
        self,
        text_query: str,
        after: int | None = None,
        limit: int | None = None,
        *,
        unsorted: bool = False,
    ) -> tuple[list[PgDCBSequencedEvent], int]:
        with self.datastore.get_connection() as conn:
            result = conn.execute(
                self.sql_invoke_pg_function_select_events2,
                (text_query, after, limit, "true" if unsorted else "false"),
            ).fetchone()
            assert result is not None
            return result["result_array"], result["result_integer"]

    def invoke_pg_procedure_append_events(
        self,
        events: list[PgDCBEvent],
        text_query: str = "",
        after: int | None = None,
    ) -> int | None:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            result = curs.execute(
                self.sql_invoke_pg_procedure_append_events,
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


class PgDCBEvent(NamedTuple):
    type: str
    data: bytes
    tags: list[str]
    text_vector: str


class PgDCBSequencedEvent(NamedTuple):
    sequence_position: int
    type: str
    data: bytes
    tags: list[str]


class PgDCBEventRow(TypedDict):
    sequence_position: int
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
