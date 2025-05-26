from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

from psycopg.sql import SQL, Identifier

from eventsourcing.persistence import IntegrityError, ProgrammingError
from eventsourcing.postgres import (
    PostgresDatastore,
    PostgresFactory,
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
from examples.dcb.postgres_ts import PostgresDCBEventStore

if TYPE_CHECKING:
    from collections.abc import Sequence

    from psycopg import Cursor
    from psycopg.rows import DictRow

PG_TYPE_NAME_DCB_EVENT_TT = "dcb_event_tt"

PG_TYPE_DCB_EVENT = SQL(
    """
CREATE TYPE {schema}.{type_name} AS (
    type text,
    data bytea,
    tags text[]
)
"""
)

PG_TYPE_NAME_DCB_QUERY_ITEM_TT = "query_item_tt"

PG_TYPE_DCB_QUERY_ITEM = SQL(
    """
CREATE TYPE {schema}.{type_name} AS (
    types text[],
    tags text[]
)
"""
)

PG_TABLE_DCB_EVENTS = SQL(
    """
CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    id bigserial,
    type text NOT NULL ,
    data bytea,
    tags text[] NOT NULL
) WITH (
  autovacuum_enabled = true,
  autovacuum_vacuum_threshold = 100000000,  -- Effectively disables VACUUM
  autovacuum_vacuum_scale_factor = 0.5,     -- Same here, high scale factor
  autovacuum_analyze_threshold = 1000,      -- Triggers ANALYZE more often
  autovacuum_analyze_scale_factor = 0.01    -- Triggers after 1% new rows
)
"""
)

PG_INDEX_UNIQUE_ID_COVER_TYPE = SQL(
    """
CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON
{schema}.{table_name} (id) INCLUDE (type)
"""
)

PG_TABLE_DCB_TAGS = SQL(
    """
CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    tag text,
    type text,
    main_id bigint REFERENCES {main_table} (id)
) WITH (
    autovacuum_enabled = true,
    autovacuum_vacuum_threshold = 100000000,  -- Effectively disables VACUUM
    autovacuum_vacuum_scale_factor = 0.5,     -- Same here, high scale factor
    autovacuum_analyze_threshold = 1000,      -- Triggers ANALYZE more often
    autovacuum_analyze_scale_factor = 0.01    -- Triggers after 1% new rows
)
"""
)

PG_INDEX_TAG_MAIN_ID = SQL(
    """
CREATE INDEX IF NOT EXISTS {index_name} ON
{schema}.{table_name} (tag, main_id)
"""
)

SQL_EXPLAIN = SQL("EXPLAIN ANALYSE")

SQL_STATEMENT_DCB_INSERT_EVENTS = SQL(
    """
WITH input AS (
      SELECT * FROM unnest(%(events)s::{event_type}[])
),
inserted AS (
    INSERT INTO {schema}.{main_table} (type, data, tags)
    SELECT i.type, i.data, i.tags
    FROM input i
    RETURNING id, type, tags
),
expanded_tags AS (
    SELECT
        ins.id AS main_id,
        ins.type,
        tag
    FROM inserted ins,
       unnest(ins.tags) AS tag
),
tag_insert AS (
    INSERT INTO {schema}.{tag_table} (tag, type, main_id)
    SELECT tag, type, main_id
    FROM expanded_tags
)
SELECT id FROM inserted
"""
)

SQL_STATEMENT_SELECT_EVENTS_BY_TAGS = SQL(
    """
WITH query_items AS (
    SELECT * FROM unnest(
        %(query_items)s::{schema}.{query_item_type}[]
    ) WITH ORDINALITY
),
initial_matches AS (
    SELECT
        t.main_id,
        qi.ordinality,
        t.type,
        t.tag,
        qi.tags AS required_tags,
        qi.types AS allowed_types
    FROM query_items qi
    JOIN {schema}.{tag_table} t
      ON t.tag = ANY(qi.tags)
),
matched_groups AS (
    SELECT
        main_id,
        ordinality,
    COUNT(DISTINCT tag) AS matched_tag_count,
        array_length(required_tags, 1) AS required_tag_count,
        allowed_types
    FROM initial_matches
    GROUP BY main_id, ordinality, required_tag_count, allowed_types
),
qualified_ids AS (
    SELECT main_id, allowed_types
    FROM matched_groups
    WHERE matched_tag_count = required_tag_count
),
filtered_ids AS (
    SELECT m.id
    FROM {schema}.{main_table} m
    JOIN qualified_ids q ON q.main_id = m.id
    WHERE
        m.id > COALESCE(%(after)s, 0)
        AND (
            array_length(q.allowed_types, 1) IS NULL
            OR array_length(q.allowed_types, 1) = 0
            OR m.type = ANY(q.allowed_types)
        )
    ORDER BY m.id ASC
    LIMIT COALESCE(%(limit)s, 9223372036854775807)
)
SELECT *
FROM {schema}.{main_table}  m
WHERE m.id IN (SELECT id FROM filtered_ids)
ORDER BY m.id ASC;
"""
)


SQL_STATEMENT_SELECT_EVENTS_ALL = SQL(
    """
SELECT * FROM {schema}.{main_table}
WHERE id > COALESCE(%(after)s, 0)
ORDER BY id ASC
LIMIT COALESCE(%(limit)s, 9223372036854775807)
"""
)

SQL_STATEMENT_SELECT_EVENTS_BY_TYPE = SQL(
    """
SELECT * FROM {schema}.{main_table}
WHERE type = %(event_type)s
AND id > COALESCE(%(after)s, 0)
ORDER BY id ASC
LIMIT COALESCE(%(limit)s, 9223372036854775807)
"""
)

SQL_STATEMENT_SELECT_MAX_ID = SQL(
    """
SELECT MAX(id) FROM {schema}.{main_table}
"""
)


class PostgresDCBEventStoreTT(PostgresDCBEventStore):
    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        events_table_name: str = "dcb_events",
    ):
        super().__init__(datastore)
        self.pg_main_table_name = events_table_name + "_tt_main"
        self.pg_tag_table_name = events_table_name + "_tt_tag"
        self.pg_index_name_id_cover_type = self.pg_main_table_name + "_idx_id_type"
        self.pg_index_name_tag_main_id = self.pg_tag_table_name + "_idx_tag_main_id"
        self.check_identifier_length(self.pg_main_table_name)
        self.check_identifier_length(self.pg_tag_table_name)
        self.check_identifier_length(self.pg_index_name_id_cover_type)
        self.check_identifier_length(self.pg_index_name_tag_main_id)
        self.datastore.pg_type_names.add(PG_TYPE_NAME_DCB_EVENT_TT)
        self.datastore.pg_type_names.add(PG_TYPE_NAME_DCB_QUERY_ITEM_TT)
        self.datastore.register_type_adapters()

        self.sql_create_statements.extend(
            [
                PG_TYPE_DCB_EVENT.format(
                    schema=Identifier(self.datastore.schema),
                    type_name=Identifier(PG_TYPE_NAME_DCB_EVENT_TT),
                ),
                PG_TYPE_DCB_QUERY_ITEM.format(
                    schema=Identifier(self.datastore.schema),
                    type_name=Identifier(PG_TYPE_NAME_DCB_QUERY_ITEM_TT),
                ),
                PG_TABLE_DCB_EVENTS.format(
                    schema=Identifier(self.datastore.schema),
                    table_name=Identifier(self.pg_main_table_name),
                ),
                PG_INDEX_UNIQUE_ID_COVER_TYPE.format(
                    index_name=Identifier(self.pg_index_name_id_cover_type),
                    schema=Identifier(self.datastore.schema),
                    table_name=Identifier(self.pg_main_table_name),
                ),
                PG_TABLE_DCB_TAGS.format(
                    schema=Identifier(self.datastore.schema),
                    table_name=Identifier(self.pg_tag_table_name),
                    main_table=Identifier(self.pg_main_table_name),
                ),
                PG_INDEX_TAG_MAIN_ID.format(
                    index_name=Identifier(self.pg_index_name_tag_main_id),
                    schema=Identifier(self.datastore.schema),
                    table_name=Identifier(self.pg_tag_table_name),
                ),
            ]
        )

        self.sql_statement_insert_events = SQL_STATEMENT_DCB_INSERT_EVENTS.format(
            event_type=Identifier(PG_TYPE_NAME_DCB_EVENT_TT),
            schema=Identifier(self.datastore.schema),
            main_table=Identifier(self.pg_main_table_name),
            tag_table=Identifier(self.pg_tag_table_name),
        )
        self.explain_sql_statement_insert_events = (
            SQL_EXPLAIN + self.sql_statement_insert_events
        )
        self.sql_statement_select_events_by_tags = (
            SQL_STATEMENT_SELECT_EVENTS_BY_TAGS.format(
                query_item_type=Identifier(PG_TYPE_NAME_DCB_QUERY_ITEM_TT),
                schema=Identifier(self.datastore.schema),
                tag_table=Identifier(self.pg_tag_table_name),
                main_table=Identifier(self.pg_main_table_name),
            )
        )
        self.explain_sql_statement_select_events_by_tags = (
            SQL_EXPLAIN + self.sql_statement_select_events_by_tags
        )
        self.sql_statement_select_events_all = SQL_STATEMENT_SELECT_EVENTS_ALL.format(
            schema=Identifier(self.datastore.schema),
            main_table=Identifier(self.pg_main_table_name),
        )
        self.sql_statement_select_events_by_type = (
            SQL_STATEMENT_SELECT_EVENTS_BY_TYPE.format(
                schema=Identifier(self.datastore.schema),
                main_table=Identifier(self.pg_main_table_name),
            )
        )
        self.sql_statement_select_max_id = SQL_STATEMENT_SELECT_MAX_ID.format(
            schema=Identifier(self.datastore.schema),
            main_table=Identifier(self.pg_main_table_name),
        )

    def read(
        self,
        query: DCBQuery | None = None,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> tuple[Sequence[DCBSequencedEvent], int | None]:
        with self.datastore.cursor() as curs:
            return self._read(
                curs=curs,
                query=query,
                after=after,
                limit=limit,
                return_head=True,
            )

    def _read(
        self,
        curs: Cursor[DictRow],
        query: DCBQuery | None = None,
        *,
        after: int | None = None,
        limit: int | None = None,
        return_head: bool = True,
    ) -> tuple[Sequence[DCBSequencedEvent], int | None]:
        if return_head and limit is None:
            curs.execute(self.sql_statement_select_max_id, prepare=True)
            row = curs.fetchone()
            head = None if row is None else row["max"]
        else:
            head = None

        if not query or not query.items:
            # Select all.
            curs.execute(
                self.sql_statement_select_events_all,
                {
                    "after": after,
                    "limit": limit,
                },
                prepare=True,
            )
            rows = curs.fetchall()

        elif self.one_query_item_one_type(query):
            # Select for one type.
            curs.execute(
                self.sql_statement_select_events_by_type,
                {
                    "event_type": query.items[0].types[0],
                    "after": after,
                    "limit": limit,
                },
                prepare=True,
            )
            rows = curs.fetchall()

        elif self.all_query_items_have_tags(query):
            # Select with tags.
            pg_dcb_query_items = self.construct_db_query_items(query.items)
            
            # # Run EXPLAIN ANALYZE and print report...
            # print()
            # for q in query.items:
            #     print("Query item tags:", q.tags, "types:", q.types)
            # print("After:", after, "Limit:", limit)
            # curs.execute(
            #     self.explain_sql_statement_select_events_by_tags,
            #     {
            #         "query_items": pg_dcb_query_items,
            #         "after": after,
            #         "limit": limit,
            #     },
            #     prepare=True,
            # )
            # rows = curs.fetchall()
            # print("\n".join([r["QUERY PLAN"] for r in rows]))
            # print()

            curs.execute(
                self.sql_statement_select_events_by_tags,
                {
                    "query_items": pg_dcb_query_items,
                    "after": after,
                    "limit": limit,
                },
                prepare=True,
            )
            rows = curs.fetchall()
        else:
            msg = f"Unsupported query: {query}"
            raise ProgrammingError(msg)

        events = [
            DCBSequencedEvent(
                event=DCBEvent(
                    type=row["type"],
                    data=row["data"],
                    tags=row["tags"],
                ),
                position=row["id"],
            )
            for row in rows
        ]

        # Maybe update head.
        if return_head and events:
            head = max(head or 0, *[e.position for e in events])

        return events, head

    def construct_db_query_items(self, query_items: Sequence[DCBQueryItem]) -> list[PgDCBQueryItem]:
        return [self.construct_pg_dcb_query_item(q) for q in query_items]

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        assert len(events) > 0
        pg_dcb_events = [self.construct_pg_dcb_event(e) for e in events]
        with self.datastore.transaction(commit=True) as curs:
            if condition is not None:
                failed, head = self._read(
                    curs=curs,
                    query=condition.fail_if_events_match,
                    after=condition.after,
                    limit=1,
                    return_head=False,
                )
                if failed:
                    raise IntegrityError(failed)
                
            # # Run EXPLAIN ANALYZE and print report...
            # print()
            # curs.execute(
            #     self.explain_sql_statement_insert_events,
            #     {
            #         "events": pg_dcb_events,
            #     },
            #     prepare=True,
            # )
            # rows = curs.fetchall()
            # print("\n".join([r["QUERY PLAN"] for r in rows]))
            # print()

            curs.execute(
                self.sql_statement_insert_events,
                {
                    "events": pg_dcb_events,
                },
                prepare=True,
            )
            rows = curs.fetchall()
            assert len(rows) > 0
            return max(row["id"] for row in rows)

    def construct_pg_dcb_event(self, dcb_event: DCBEvent) -> PgDCBEvent:
        return self.datastore.pg_python_types[PG_TYPE_NAME_DCB_EVENT_TT](
            type=dcb_event.type,
            data=dcb_event.data,
            tags=dcb_event.tags,
        )

    def construct_pg_dcb_query_item(
        self, dcb_query_item: DCBQueryItem
    ) -> PgDCBQueryItem:
        return self.datastore.pg_python_types[PG_TYPE_NAME_DCB_QUERY_ITEM_TT](
            types=dcb_query_item.types,
            tags=dcb_query_item.tags,
        )

    def one_query_item_one_type(self, query: DCBQuery) -> bool:
        return (
            len(query.items) == 1
            and len(query.items[0].types) == 1
            and len(query.items[0].tags) == 0
        )

    def all_query_items_have_tags(self, query: DCBQuery) -> bool:
        return all(len(q.tags) > 0 for q in query.items)


class PgDCBEvent(NamedTuple):
    type: str
    data: bytes
    tags: list[str]


class PgDCBQueryItem(NamedTuple):
    types: list[str]
    tags: list[str]


class PostgresTTDCBFactory(
    PostgresFactory,
    DCBInfrastructureFactory[PostgresTrackingRecorder],
):
    def dcb_event_store(self) -> DCBEventStore:
        prefix = self.env.name.lower() or "dcb"

        dcb_table_name = prefix + "_events"
        recorder = PostgresDCBEventStoreTT(
            datastore=self.datastore,
            events_table_name=dcb_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder
