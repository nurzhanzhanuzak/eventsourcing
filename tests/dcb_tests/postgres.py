from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, TypedDict, cast

from psycopg.errors import DuplicateObject
from psycopg.sql import SQL, Identifier
from psycopg.types.composite import CompositeInfo, register_composite

from eventsourcing.persistence import IntegrityError, ProgrammingError
from eventsourcing.postgres import PostgresDatastore, PostgresRecorder
from tests.dcb_tests.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBEventStore,
    DCBQuery,
    DCBSequencedEvent,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from psycopg import Connection


class PostgresDCBEventStore(DCBEventStore, PostgresRecorder):
    dcb_event_type_name = "dcb_event"
    dcb_query_item_type_name = "dcb_query_item"
    pg_composite_type_adapters: ClassVar[dict[str, Any]] = {}

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

        self.sql_create_table = SQL(
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

        self.sql_create_index_on_type = SQL(
            "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table}(type, position)"
        ).format(
            index=Identifier(self.dcb_events_table_name + "_type_idx"),
            schema=Identifier(self.datastore.schema),
            table=Identifier(self.dcb_events_table_name),
        )

        self.sql_create_index_on_tags = SQL(
            "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} USING GIN (tags)"
        ).format(
            index=Identifier(self.dcb_events_table_name + "_tag_idx"),
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
            "CREATE TYPE {schema}.{name} AS (type TEXT, data bytea, tags TEXT[]) "
        ).format(
            schema=Identifier(self.datastore.schema),
            name=Identifier(self.dcb_event_type_name),
        )
        self.sql_create_type_dcb_query_item = SQL(
            "CREATE TYPE {schema}.{name} AS (types TEXT[], tags TEXT[]) "
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
            "    INSERT INTO {schema}.{table} (type, data, tags) "
            "    SELECT * from unnest(events) "
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
            "        IF array_length(query_items, 1) = 1 THEN "
            "            /* one item... */"
            "            query_item = query_items[1];"
            "            IF array_length(query_item.types, 1) IS NOT NULL THEN "
            "                /* one item, non-zero types... */"
            "                IF array_length(query_item.tags, 1) IS NOT NULL THEN "
            "                    /* one item - non-zero tags - non-zero types  */"
            "                    RETURN QUERY "
            "                    SELECT * from {schema}.{table} e "
            "                    WHERE e.tags @> query_item.tags "
            "                    AND e.type = ANY (query_item.types) "
            "                    AND position > after "
            "                    ORDER BY position ASC LIMIT max_results;"
            "                ELSIF array_length(query_item.types, 1) = 1 THEN "
            "                    /* one item - zero tags - one type  */"
            "                    RETURN QUERY "
            "                    SELECT * from {schema}.{table} e "
            "                    WHERE e.type = query_item.types[1] "
            "                    AND position > after "
            "                    ORDER BY position ASC LIMIT max_results;"
            "                ELSE "
            "                    /* one item - zero tags - many types */"
            "                    RETURN QUERY "
            "                    SELECT * from {schema}.{table} e "
            "                    WHERE e.type = ANY(query_item.types) "
            "                    AND position > after "
            "                    ORDER BY position ASC LIMIT max_results;"
            "                END IF;"
            "            ELSE"
            "                /* one item - non-zero tags - zero types */"
            "                RETURN QUERY "
            "                SELECT * from {schema}.{table} e "
            "                WHERE e.tags @> query_item.tags "
            "                AND position > after "
            "                ORDER BY position ASC LIMIT max_results;"
            "            END IF;"
            "        ELSE"
            "            /* many query items... for each select into temp table */"
            "            CREATE TEMP TABLE temp_results ("
            "                tmpposn BIGINT PRIMARY KEY, "
            "                tmptype TEXT, "
            "                tmpdata bytea, "
            "                tmptags TEXT[]"
            "            ) ON COMMIT DROP;"
            "            FOREACH query_item IN ARRAY query_items"
            "            LOOP"
            "                IF array_length(query_item.types, 1) IS NOT NULL THEN "
            "                    /* nth item - non-zero types... */"
            "                    IF array_length(query_item.tags, 1) IS NOT NULL THEN "
            "                        /* nth item - non-zero tags - non-zero types */"
            "                        INSERT INTO temp_results ("
            "                           tmpposn, tmptype, tmpdata, tmptags"
            "                        ) "
            "                        SELECT * from {schema}.{table} e "
            "                        WHERE e.tags @> query_item.tags "
            "                        AND e.type = ANY (query_item.types) "
            "                        AND position > after "
            "                        LIMIT internal_max"
            "                        ON CONFLICT DO NOTHING;"
            "                    ELSIF array_length(query_item.types, 1) = 1 THEN "
            "                        /* nth item - zero tags - one type */"
            "                        INSERT INTO temp_results ("
            "                            tmpposn, tmptype, tmpdata, tmptags"
            "                        ) "
            "                        SELECT * from {schema}.{table} e "
            "                        WHERE e.type = query_item.types[1] "
            "                        AND position > after "
            "                        LIMIT internal_max"
            "                        ON CONFLICT DO NOTHING;"
            "                    ELSE "
            "                        /* nth item - zero tags - many types */"
            "                        INSERT INTO temp_results ("
            "                            tmpposn, tmptype, tmpdata, tmptags"
            "                        ) "
            "                        SELECT * from {schema}.{table} e "
            "                        WHERE e.type = ANY(query_item.types) "
            "                        AND position > after "
            "                        LIMIT internal_max"
            "                        ON CONFLICT DO NOTHING;"
            "                    END IF;"
            "                ELSE"
            "                    /* nth item - non-zero tags - zero types */"
            "                    INSERT INTO temp_results ("
            "                        tmpposn, tmptype, tmpdata, tmptags"
            "                    ) "
            "                    SELECT * from {schema}.{table} e "
            "                    WHERE e.tags @> query_item.tags "
            "                    AND position > after "
            "                    LIMIT internal_max"
            "                    ON CONFLICT DO NOTHING;"
            "                END IF;"
            "                /* exit loop if only searching for any existing rows */"
            "                IF fail_fast AND EXISTS (SELECT FROM temp_results) THEN"
            "                    EXIT;"
            "                END IF;"
            "            END LOOP;"
            "            /* return distinct limited sorted rows from temp table */"
            "            RETURN QUERY SELECT *"
            # "            DISTINCT ON (tmpposn) "
            # "            tmpposn, tmptype, tmpdata, tmptags "
            "            FROM temp_results "
            "            WHERE tmpposn > after "
            "            ORDER BY tmpposn ASC "
            "            LIMIT max_results;"
            "        END IF;"
            "    ELSE"
            "        /* no query items - return limited sorted rows from table */"
            "        RETURN QUERY SELECT * from {schema}.{table} "
            "        WHERE position > after "
            "        ORDER BY position ASC LIMIT max_results;"
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
                self.sql_create_index_on_type,
                self.sql_create_index_on_tags,
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
        type(self).register_pg_composite_type_adapters(curs.connection)
        super()._create_table(curs)

    # https://www.psycopg.org/psycopg3/docs/basic/pgtypes.html#composite-types-casting
    # This is all a little bit awkward. Adapters are registered in a "context", and
    # we can't register them if the types haven't been created, and registering type
    # adapters doesn't affect objects already created, so each connection needs to
    # try to register type adapters. And the connection that creates the types also
    # needs to register type adapters, in case it will be used again. And to avoid
    # circular references in the datastore, this function, which is registered to
    # be called after connection is created, cannot be an object method. And anyway,
    # it can't be an object method because we need to create the datastore before we
    # create the DCB event store. Hence, it is a class method, and so necessarily the
    # type adapter objects are remembered on a class attribute. There might be a
    # better way........ :-)
    @classmethod
    def register_pg_composite_type_adapters(cls, conn: Connection[Any]) -> None:
        type_names = [
            cls.dcb_event_type_name,
            cls.dcb_query_item_type_name,
        ]
        for name in type_names:
            info = CompositeInfo.fetch(conn, name)
            if info is not None:
                register_composite(info, conn)
                cls.pg_composite_type_adapters[name] = info

    def read(
        self,
        query: DCBQuery | None = None,
        after: int | None = None,
        limit: int | None = None,
    ) -> Iterator[DCBSequencedEvent]:
        # Prepare arguments and invoke pg function.
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
                query_items=[
                    self.construct_pg_query_item(
                        types=item.types,
                        tags=item.tags,
                    )
                    for item in (query or DCBQuery()).items
                ],
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
        self, type: str, data: bytes, tags: list[str]  # noqa: A002
    ) -> PgDCBEvent:
        return self.pg_composite_type_adapters[self.dcb_event_type_name].python_type(
            type, data, tags
        )

    def construct_pg_query_item(
        self, types: list[str], tags: list[str]
    ) -> PgDCBQueryItem:
        return self.pg_composite_type_adapters[
            self.dcb_query_item_type_name
        ].python_type(types, tags)


class PgDCBEvent(NamedTuple):
    type: str
    data: bytes
    tags: list[str]


class PgDCBQueryItem(NamedTuple):
    types: list[str]
    tags: list[str]


class PgDCBEventRow(TypedDict):
    posn: int
    type: str
    data: bytes
    tags: list[str]
