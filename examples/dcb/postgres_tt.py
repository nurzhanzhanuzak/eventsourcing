from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

from psycopg.sql import SQL, Composed, Identifier

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
    from psycopg.abc import Params
    from psycopg.rows import DictRow

PG_TYPE_NAME_DCB_EVENT_TT = "dcb_event_tt"

PG_TYPE_DCB_EVENT = SQL(
    """
CREATE TYPE {schema}.{event_type} AS (
    type text,
    data bytea,
    tags text[]
)
"""
)

PG_TYPE_NAME_DCB_QUERY_ITEM_TT = "query_item_tt"

PG_TYPE_DCB_QUERY_ITEM = SQL(
    """
CREATE TYPE {schema}.{query_item_type} AS (
    types text[],
    tags text[]
)
"""
)

PG_TABLE_DCB_EVENTS = SQL(
    """
CREATE TABLE IF NOT EXISTS {schema}.{events_table} (
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
CREATE UNIQUE INDEX IF NOT EXISTS {id_cover_type_index} ON
{schema}.{events_table} (id) INCLUDE (type)
"""
)

PG_TABLE_DCB_TAGS = SQL(
    """
CREATE TABLE IF NOT EXISTS {schema}.{tags_table} (
    tag text,
    type text,
    main_id bigint REFERENCES {events_table} (id)
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
CREATE INDEX IF NOT EXISTS {tag_main_id_index} ON
{schema}.{tags_table} (tag, main_id)
"""
)

SQL_STATEMENT_SELECT_EVENTS_ALL = SQL(
    """
SELECT * FROM {schema}.{events_table}
WHERE id > COALESCE(%(after)s, 0)
ORDER BY id ASC
LIMIT COALESCE(%(limit)s, 9223372036854775807)
"""
)

SQL_STATEMENT_SELECT_EVENTS_BY_TYPE = SQL(
    """
SELECT * FROM {schema}.{events_table}
WHERE type = %(event_type)s
AND id > COALESCE(%(after)s, 0)
ORDER BY id ASC
LIMIT COALESCE(%(limit)s, 9223372036854775807)
"""
)

SQL_STATEMENT_SELECT_MAX_ID = SQL(
    """
SELECT MAX(id) FROM {schema}.{events_table}
"""
)

SQL_STATEMENT_INSERT_EVENTS = SQL(
    """
WITH input AS (
      SELECT * FROM unnest(%(events)s::{event_type}[])
),
inserted AS (
    INSERT INTO {schema}.{events_table} (type, data, tags)
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
    INSERT INTO {schema}.{tags_table} (tag, type, main_id)
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
    JOIN {schema}.{tags_table} t
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
    FROM {schema}.{events_table} m
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
FROM {schema}.{events_table}  m
WHERE m.id IN (SELECT id FROM filtered_ids)
ORDER BY m.id ASC;
"""
)

SQL_STATEMENT_CONDITIONAL_APPEND = SQL(
    """
"""
)

SQL_EXPLAIN = SQL("EXPLAIN")
SQL_EXPLAIN_ANALYZE = SQL("EXPLAIN ANALYZE")


class PostgresDCBEventStoreTT(PostgresDCBEventStore):
    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        events_table_name: str = "dcb_events",
    ):
        super().__init__(datastore)
        self.events_table_name = events_table_name + "_tt_main"
        self.tags_table_name = events_table_name + "_tt_tag"
        self.index_name_id_cover_type = self.events_table_name + "_idx_id_type"
        self.index_name_tag_main_id = self.tags_table_name + "_idx_tag_main_id"
        self.check_identifier_length(self.events_table_name)
        self.check_identifier_length(self.tags_table_name)
        self.check_identifier_length(self.index_name_id_cover_type)
        self.check_identifier_length(self.index_name_tag_main_id)
        self.datastore.pg_type_names.add(PG_TYPE_NAME_DCB_EVENT_TT)
        self.datastore.pg_type_names.add(PG_TYPE_NAME_DCB_QUERY_ITEM_TT)
        self.datastore.register_type_adapters()
        self.sql_kwargs = {
            "schema": Identifier(self.datastore.schema),
            "events_table": Identifier(self.events_table_name),
            "tags_table": Identifier(self.tags_table_name),
            "event_type": Identifier(PG_TYPE_NAME_DCB_EVENT_TT),
            "query_item_type": Identifier(PG_TYPE_NAME_DCB_QUERY_ITEM_TT),
            "id_cover_type_index": Identifier(self.index_name_id_cover_type),
            "tag_main_id_index": Identifier(self.index_name_tag_main_id),
        }

        self.sql_create_statements.extend(
            [
                PG_TYPE_DCB_EVENT.format(**self.sql_kwargs),
                PG_TYPE_DCB_QUERY_ITEM.format(**self.sql_kwargs),
                PG_TABLE_DCB_EVENTS.format(**self.sql_kwargs),
                PG_INDEX_UNIQUE_ID_COVER_TYPE.format(**self.sql_kwargs),
                PG_TABLE_DCB_TAGS.format(**self.sql_kwargs),
                PG_INDEX_TAG_MAIN_ID.format(**self.sql_kwargs),
            ]
        )

        self.sql_statement_select_events_by_tags = (
            SQL_STATEMENT_SELECT_EVENTS_BY_TAGS.format(**self.sql_kwargs)
        )
        self.explain_sql_statement_select_events_by_tags = (
            SQL_EXPLAIN_ANALYZE + self.sql_statement_select_events_by_tags
        )
        self.sql_statement_select_events_all = SQL_STATEMENT_SELECT_EVENTS_ALL.format(
            **self.sql_kwargs
        )
        self.sql_statement_select_events_by_type = (
            SQL_STATEMENT_SELECT_EVENTS_BY_TYPE.format(**self.sql_kwargs)
        )
        self.sql_statement_select_max_id = SQL_STATEMENT_SELECT_MAX_ID.format(
            **self.sql_kwargs
        )
        self.sql_statement_insert_events = SQL_STATEMENT_INSERT_EVENTS.format(
            **self.sql_kwargs
        )
        self.sql_statement_conditional_append = SQL_STATEMENT_CONDITIONAL_APPEND.format(
            **self.sql_kwargs
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
            self.execute(curs, self.sql_statement_select_max_id, explain=False)
            row = curs.fetchone()
            head = None if row is None else row["max"]
        else:
            head = None

        if not query or not query.items:
            # Select all.
            self.execute(
                curs,
                self.sql_statement_select_events_all,
                {
                    "after": after,
                    "limit": limit,
                },
                explain=False,
            )
            rows = curs.fetchall()

        elif self.has_all_query_items_have_tags(query):
            # Select with tags.
            pg_dcb_query_items = self.construct_db_query_items(query.items)

            self.execute(
                curs,
                self.sql_statement_select_events_by_tags,
                {
                    "query_items": pg_dcb_query_items,
                    "after": after,
                    "limit": limit,
                },
                explain=False,
            )
            rows = curs.fetchall()

        elif self.has_one_query_item_one_type(query):
            # Select for one type.
            self.execute(
                curs,
                self.sql_statement_select_events_by_type,
                {
                    "event_type": query.items[0].types[0],
                    "after": after,
                    "limit": limit,
                },
                explain=False,
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

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        assert len(events) > 0
        pg_dcb_events = [self.construct_pg_dcb_event(e) for e in events]
        # if condition and self.has_all_query_items_have_tags(condition.fail_if_events_match):
        #     with self.datastore.cursor() as curs:
        #         self.execute(
        #             curs,
        #             self.sql_statement_conditional_append,
        #             {
        #                 "events": pg_dcb_events,
        #             },
        #             explain=True,
        #         )
        #         rows = curs.fetchall()
        #         if len(rows) > 0:
        #             return max(row["id"] for row in rows)
        #         else:
        #             raise IntegrityError

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

            self.execute(
                curs,
                self.sql_statement_insert_events,
                {
                    "events": pg_dcb_events,
                },
                explain=False,
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

    def construct_db_query_items(
        self, query_items: Sequence[DCBQueryItem]
    ) -> list[PgDCBQueryItem]:
        return [self.construct_pg_dcb_query_item(q) for q in query_items]

    def construct_pg_dcb_query_item(
        self, dcb_query_item: DCBQueryItem
    ) -> PgDCBQueryItem:
        return self.datastore.pg_python_types[PG_TYPE_NAME_DCB_QUERY_ITEM_TT](
            types=dcb_query_item.types,
            tags=dcb_query_item.tags,
        )

    def has_one_query_item_one_type(self, query: DCBQuery) -> bool:
        return (
            len(query.items) == 1
            and len(query.items[0].types) == 1
            and len(query.items[0].tags) == 0
        )

    def has_all_query_items_have_tags(self, query: DCBQuery) -> bool:
        return all(len(q.tags) > 0 for q in query.items) and len(query.items) > 0

    def execute(
        self,
        cursor: Cursor[DictRow],
        statement: Composed,
        params: Params | None = None,
        *,
        explain: bool = False,
    ) -> None:
        if explain:
            self.datastore.pool.resize(2, 2)
            print()
            print("Statement:", statement.as_string())
            print("Params:", params)
            with self.datastore.transaction(commit=False) as explain_cursor:
                explain_cursor.execute(SQL_EXPLAIN + statement, params)
                rows = explain_cursor.fetchall()
                print("\n".join([r["QUERY PLAN"] for r in rows]))
                print()
            self.datastore.pool.resize(1, 1)
        cursor.execute(statement, params, prepare=True)


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
