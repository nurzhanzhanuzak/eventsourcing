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
                self.dcb_query_item_type_name,
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

        # self.sql_create_index_on_type = SQL(
        #     "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table}(type, position)"
        # ).format(
        #     index=Identifier(self.dcb_events_table_name + "_type_idx"),
        #     schema=Identifier(self.datastore.schema),
        #     table=Identifier(self.dcb_events_table_name),
        # )
        #
        # self.sql_create_index_on_tags = SQL(
        #     "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} USING GIN (tags)"
        # ).format(
        #     index=Identifier(self.dcb_events_table_name + "_tag_idx"),
        #     schema=Identifier(self.datastore.schema),
        #     table=Identifier(self.dcb_events_table_name),
        # )

        self.sql_create_index_on_searchtext = SQL(
            "CREATE INDEX IF NOT EXISTS {index} "
            "ON {schema}.{table} "
            "USING GIN (text_vector)"
        ).format(
            index=Identifier(self.dcb_events_table_name + "_searchtext_idx"),
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
        self.sql_create_type_dcb_query_item = SQL(
            "CREATE TYPE {schema}.{name} "
            "AS (types TEXT[], tags TEXT[], text_query tsquery)"
        ).format(
            schema=Identifier(self.datastore.schema),
            name=Identifier(self.dcb_query_item_type_name),
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
            "SELECT * FROM {select_events}((%s), (%s), (%s), (%s))"
        ).format(select_events=Identifier(self.pg_function_name_select_events))

        self.sql_create_pg_function_select_events = SQL(
            "CREATE OR REPLACE FUNCTION {select_events} ("
            "    query_items {query_item}[], "
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
            "    query_item {query_item};"
            "    internal_max integer DEFAULT NULL;"
            "BEGIN"
            "    after = COALESCE(after, 0);"
            "    IF fail_fast THEN "
            "        max_results = 1;"
            "        internal_max = 1;"
            "    ELSE"
            "        internal_max = max_results;"
            "    END IF;"
            "    IF array_length(query_items, 1) IS NOT NULL THEN "
            "        /* at least one query item... */"
            "        CREATE TEMP TABLE temp_results ("
            "            tmpposn BIGINT PRIMARY KEY, "
            "            tmptype TEXT, "
            "            tmpdata bytea, "
            "            tmptags TEXT[]"
            "        ) ON COMMIT DROP;"
            "        IF array_length(query_items, 1) = 1 THEN "
            "            /* one item... */"
            "            query_item = query_items[1];"
            "            IF query_item.text_query <> '' THEN "
            "                RETURN QUERY "
            "                SELECT t.position, t.type, t.data, t.tags "
            "                FROM {schema}.{table} t "
            "                WHERE t.position > after "
            "                AND t.text_vector @@ query_item.text_query "
            "                ORDER BY t.position ASC "
            "                LIMIT max_results;"
            "            ELSIF array_length(query_item.types, 1) IS NOT NULL THEN "
            "                /* one item, non-zero types... */"
            "                IF array_length(query_item.tags, 1) IS NOT NULL THEN "
            "                    /* one item - non-zero tags - non-zero types  */"
            "                    RETURN QUERY "
            "                    SELECT t.position, t.type, t.data, t.tags "
            "                    FROM {schema}.{table} t "
            "                    WHERE t.tags @> query_item.tags "
            "                    AND t.type = ANY (query_item.types) "
            "                    AND t.position > after "
            "                    ORDER BY t.position ASC "
            "                    LIMIT max_results;"
            "                ELSIF array_length(query_item.types, 1) = 1 THEN "
            "                    /* one item - zero tags - one type  */"
            "                    RETURN QUERY "
            "                    SELECT t.position, t.type, t.data, t.tags "
            "                    FROM {schema}.{table} t "
            "                    WHERE t.type = query_item.types[1] "
            "                    AND t.position > after "
            "                    ORDER BY t.position ASC "
            "                    LIMIT max_results;"
            "                ELSE "
            "                    /* one item - zero tags - many types */"
            "                    RETURN QUERY "
            "                    SELECT t.position, t.type, t.data, t.tags "
            "                    FROM {schema}.{table} t "
            "                    WHERE t.type = ANY(query_item.types) "
            "                    AND t.position > after "
            "                    ORDER BY t.position ASC "
            "                    LIMIT max_results;"
            "                END IF;"
            "            ELSE"
            "                /* one item - non-zero tags - zero types */"
            "                RETURN QUERY "
            "                SELECT t.position, t.type, t.data, t.tags "
            "                FROM {schema}.{table} t "
            "                WHERE t.tags @> query_item.tags "
            "                AND t.position > after "
            "                ORDER BY t.position ASC "
            "                LIMIT max_results;"
            "            END IF;"
            "        ELSE"
            "            FOREACH query_item IN ARRAY query_items"
            "            LOOP"
            "                IF array_length(query_item.types, 1) IS NOT NULL THEN "
            "                    /* nth item - non-zero types... */"
            "                    IF array_length(query_item.tags, 1) IS NOT NULL THEN "
            "                        /* nth item - non-zero tags - non-zero types */"
            "                        INSERT INTO temp_results ("
            "                           tmpposn, tmptype, tmpdata, tmptags"
            "                        ) "
            "                        SELECT t.position, t.type, t.data, t.tags "
            "                        FROM {schema}.{table} t "
            "                        WHERE t.tags @> query_item.tags "
            "                        AND t.type = ANY (query_item.types) "
            "                        AND t.position > after "
            "                        LIMIT internal_max"
            "                        ON CONFLICT DO NOTHING;"
            "                    ELSIF array_length(query_item.types, 1) = 1 THEN "
            "                        /* nth item - zero tags - one type */"
            "                        INSERT INTO temp_results ("
            "                            tmpposn, tmptype, tmpdata, tmptags"
            "                        ) "
            "                        SELECT t.position, t.type, t.data, t.tags "
            "                        FROM {schema}.{table} t "
            "                        WHERE t.type = query_item.types[1] "
            "                        AND t.position > after "
            "                        LIMIT internal_max"
            "                        ON CONFLICT DO NOTHING;"
            "                    ELSE "
            "                        /* nth item - zero tags - many types */"
            "                        INSERT INTO temp_results ("
            "                            tmpposn, tmptype, tmpdata, tmptags"
            "                        ) "
            "                        SELECT t.position, t.type, t.data, t.tags "
            "                        FROM {schema}.{table} t "
            "                        WHERE t.type = ANY(query_item.types) "
            "                        AND t.position > after "
            "                        LIMIT internal_max"
            "                        ON CONFLICT DO NOTHING;"
            "                    END IF;"
            "                ELSE"
            "                    /* nth item - non-zero tags - zero types */"
            "                    INSERT INTO temp_results ("
            "                        tmpposn, tmptype, tmpdata, tmptags"
            "                    ) "
            "                    SELECT t.position, t.type, t.data, t.tags "
            "                        FROM {schema}.{table} t "
            "                    WHERE t.tags @> query_item.tags "
            "                    AND t.position > after "
            "                    LIMIT internal_max"
            "                    ON CONFLICT DO NOTHING;"
            "                END IF;"
            "                /* exit loop if only searching for any existing rows */"
            "                IF fail_fast AND EXISTS (SELECT FROM temp_results) THEN"
            "                    EXIT;"
            "                END IF;"
            "            END LOOP;"
            "            /* return distinct limited sorted rows from temp table */"
            "        END IF;"
            "        RETURN QUERY SELECT *"
            # "            DISTINCT ON (tmpposn) "
            # "            tmpposn, tmptype, tmpdata, tmptags "
            "        FROM temp_results "
            "        WHERE tmpposn > after "
            "        ORDER BY tmpposn ASC "
            "        LIMIT max_results;"
            "    ELSE"
            "        /* no query items - return limited sorted rows from table */"
            "        RETURN QUERY SELECT t.position, t.type, t.data, t.tags "
            "        FROM {schema}.{table} t "
            "        WHERE t.position > after "
            "        ORDER BY t.position ASC LIMIT max_results;"
            "    END IF;"
            "END; "
            "$BODY$;"
        ).format(
            select_events=Identifier(self.pg_function_name_select_events),
            query_item=Identifier(self.dcb_query_item_type_name),
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
            "    in query_items {query_item}[],"
            "    inout after BIGINT"
            ") "
            "LANGUAGE plpgsql "
            "AS "
            "$BODY$ "
            "BEGIN "
            # "    LOCK TABLE {schema}.{table} IN EXCLUSIVE MODE;"
            "    IF (after < 0) "
            "    OR (NOT EXISTS ("
            "        SELECT FROM {select_events}(query_items, after, 1, True)"
            "    )) "
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
            query_item=Identifier(self.dcb_query_item_type_name),
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
                # self.sql_create_index_on_type,
                # self.sql_create_index_on_tags,
                self.sql_create_index_on_searchtext,
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
        with (
            self.datastore.transaction(commit=True) as curs,
            contextlib.suppress(DuplicateObject),
        ):
            curs.execute(self.sql_create_type_dcb_query_item)
        super().create_table()

    def read(
        self,
        query: DCBQuery | None = None,
        after: int | None = None,
        limit: int | None = None,
    ) -> Iterator[DCBSequencedEvent]:
        # Prepare arguments and invoke pg function.
        pg_dcb_query_items = [
            self.construct_pg_query_item(
                types=item.types,
                tags=item.tags,
            )
            for item in (query or DCBQuery()).items
        ]
        pg_dcb_query_items = self.construct_query_items_with_text_query(
            pg_dcb_query_items
        )
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
                query_items=pg_dcb_query_items,
                after=after,
                limit=limit,
            )
        )

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        # Prepare arguments.
        if len(events) == 0:
            msg = "Should be at least one event. Avoid this elsewhere"
            raise ProgrammingError(msg)
        pg_dcb_events = [
            self.construct_pg_dcb_event(
                type=event.type,
                data=event.data,
                tags=event.tags,
            )
            for event in events
        ]
        if condition is not None:
            pg_dcb_query_items = [
                self.construct_pg_query_item(
                    types=item.types,
                    tags=item.tags,
                )
                for item in condition.fail_if_events_match.items
            ]
            pg_dcb_query_items = self.construct_query_items_with_text_query(
                pg_dcb_query_items
            )
            # print("Text query:", query_items[0].text_query)

            after = condition.after or 0
            assert after >= 0
        else:
            pg_dcb_query_items = []
            after = -1  # Indicates no "fail condition".

        # Invoke pg procedure.
        last_new_position = self.invoke_pg_append_events_procedure(
            events=pg_dcb_events,
            query_items=pg_dcb_query_items,
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
        query_items: list[PgDCBQueryItem],
        after: int | None,
        limit: int | None = None,
        *,
        fail_fast: bool = False,
    ) -> Iterator[PgDCBEventRow]:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            curs.execute(
                self.sql_invoke_pg_select_events_function,
                (query_items, after, limit, fail_fast),
            )
            return cast("Iterator[PgDCBEventRow]", curs.fetchall())

    def invoke_pg_append_events_procedure(
        self,
        events: list[PgDCBEvent],
        query_items: list[PgDCBQueryItem],
        after: int,
    ) -> int | None:
        # with self.datastore.transaction(commit=True) as curs:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            result = curs.execute(
                self.sql_invoke_pg_append_events_procedure,
                [events, query_items, after],
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

    def construct_pg_query_item(
        self, types: list[str], tags: list[str], text_query: str = ""
    ) -> PgDCBQueryItem:
        return self.datastore.pg_type_adapters[
            self.dcb_query_item_type_name
        ].python_type(types, tags, text_query)

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

    def construct_query_items_with_text_query(
        self, query_items: list[PgDCBQueryItem]
    ) -> list[PgDCBQueryItem]:
        text_queries = [self.construct_text_query(q.types, q.tags) for q in query_items]
        text_query = " | ".join([f"({t})" for t in text_queries])
        return [cast(PgDCBQueryItem, self.construct_pg_query_item([], [], text_query))]

    def construct_text_query(self, types: list[str], tags: list[str]) -> str:
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
