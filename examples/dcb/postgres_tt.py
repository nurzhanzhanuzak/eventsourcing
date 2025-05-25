from __future__ import annotations

from typing import Sequence, NamedTuple

from psycopg.sql import SQL, Identifier

from eventsourcing.persistence import ProgrammingError, IntegrityError
from eventsourcing.postgres import PostgresDatastore, PostgresFactory, \
    PostgresTrackingRecorder
from examples.dcb.api import DCBAppendCondition, DCBEvent, DCBQuery, DCBSequencedEvent, \
    DCBQueryItem, DCBInfrastructureFactory, DCBEventStore
from examples.dcb.postgres_ts import PostgresDCBEventStore


PG_TYPE_NAME_DCB_EVENT_TT = "dcb_event_tt"

PG_TYPE_DCB_EVENT_TT = SQL(
    """
CREATE TYPE {schema}.{type_name} AS (
    type text,
    data bytea,
    tags text[]
)
"""
)

PG_TYPE_NAME_DCB_QUERY_ITEM_TT = "query_item_tt"

PG_TYPE_DCB_QUERY_ITEM_TT = SQL(
    """
CREATE TYPE {schema}.{type_name} AS (
    types text[],
    tags text[]
)
"""
)

PG_TABLE_DCB_EVENTS_MAIN_TABLE = SQL(
    """
CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    id bigserial PRIMARY KEY,
    type text NOT NULL ,
    data bytea,
    tags text[] NOT NULL
) WITH (autovacuum_enabled=false)
"""
)

PG_TABLE_DCB_EVENTS_TAG_TABLE = SQL(
    """
CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    tag text,
    type text,
    main_id bigint REFERENCES {main_table} (id)
) WITH (autovacuum_enabled=false)
"""
)

PG_TABLE_INDEX_DCB_TAG_TYPE_MAIN_ID = SQL(
    """
CREATE INDEX IF NOT EXISTS {index_name} ON
{schema}.{tag_table} (tag, type, main_id)
"""
)

PG_TABLE_INDEX_DCB_TAG_MAIN_ID_TYPE = SQL(
    """
CREATE INDEX IF NOT EXISTS {index_name} ON
{schema}.{tag_table} (tag, main_id, type)
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
  SELECT * FROM unnest(%(query_items)s::{schema}.{query_item_type}[]) WITH ORDINALITY
),
tag_matches AS (
  SELECT
    t.main_id,
    qi.ordinality,
    COUNT(DISTINCT t.tag) AS matched_tag_count,
    array_length(qi.tags, 1) AS required_tag_count
  FROM query_items qi
  JOIN {schema}.{tag_table} t
    ON t.tag = ANY(qi.tags)
  AND (
    array_length(qi.types, 1) IS NULL
    OR array_length(qi.types, 1) = 0
    OR t.type = ANY(qi.types)
  )
  GROUP BY t.main_id, qi.ordinality, qi.tags
),
qualified_main_ids AS (
  SELECT DISTINCT main_id
  FROM tag_matches
  WHERE matched_tag_count = required_tag_count
),
matched_events AS (
  SELECT m.*
  FROM {schema}.{main_table} m
  JOIN qualified_main_ids q ON q.main_id = m.id
  WHERE m.id > COALESCE(%(after)s, 0)
)
SELECT * FROM matched_events
ORDER BY id ASC
LIMIT COALESCE(%(limit)s, 9223372036854775807)
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
        self.pg_index_name_tt_tag_type_main_id = self.pg_tag_table_name + "_idx_tag_type_main_id"
        self.pg_index_name_tt_tag_main_id_type = self.pg_tag_table_name + "_idx_tag_main_id_type"
        self.check_identifier_length(self.pg_main_table_name)
        self.check_identifier_length(self.pg_tag_table_name)
        self.check_identifier_length(self.pg_index_name_tt_tag_type_main_id)
        self.check_identifier_length(self.pg_index_name_tt_tag_main_id_type)
        self.datastore.pg_type_names.add(PG_TYPE_NAME_DCB_EVENT_TT)
        self.datastore.pg_type_names.add(PG_TYPE_NAME_DCB_QUERY_ITEM_TT)
        self.datastore.register_type_adapters()

        self.sql_create_statements.extend(
            [
                PG_TYPE_DCB_EVENT_TT.format(
                    schema=Identifier(self.datastore.schema),
                    type_name=Identifier(PG_TYPE_NAME_DCB_EVENT_TT),
                ),
                PG_TYPE_DCB_QUERY_ITEM_TT.format(
                    schema=Identifier(self.datastore.schema),
                    type_name=Identifier(PG_TYPE_NAME_DCB_QUERY_ITEM_TT),
                ),
                PG_TABLE_DCB_EVENTS_MAIN_TABLE.format(
                    schema=Identifier(self.datastore.schema),
                    table_name=Identifier(self.pg_main_table_name),
                ),
                PG_TABLE_DCB_EVENTS_TAG_TABLE.format(
                    schema=Identifier(self.datastore.schema),
                    table_name=Identifier(self.pg_tag_table_name),
                    main_table=Identifier(self.pg_main_table_name),
                )
            ]
        )

        self.sql_statement_insert_events = SQL_STATEMENT_DCB_INSERT_EVENTS.format(
            event_type=Identifier(PG_TYPE_NAME_DCB_EVENT_TT),
            schema=Identifier(self.datastore.schema),
            main_table=Identifier(self.pg_main_table_name),
            tag_table=Identifier(self.pg_tag_table_name),
        )
        print(self.sql_statement_insert_events.as_string())
        self.sql_statement_select_events_by_tags = (
            SQL_STATEMENT_SELECT_EVENTS_BY_TAGS.format(
                query_item_type=Identifier(PG_TYPE_NAME_DCB_QUERY_ITEM_TT),
                schema=Identifier(self.datastore.schema),
                tag_table=Identifier(self.pg_tag_table_name),
                main_table=Identifier(self.pg_main_table_name),
            )
        )
        print(self.sql_statement_select_events_by_tags.as_string())
        self.explain_sql_statement_select_events_by_tags = (
            SQL_EXPLAIN + self.sql_statement_select_events_by_tags
        )
        print(self.explain_sql_statement_select_events_by_tags.as_string())
        self.sql_statement_select_events_all = (
            SQL_STATEMENT_SELECT_EVENTS_ALL.format(
                schema=Identifier(self.datastore.schema),
                main_table=Identifier(self.pg_main_table_name),
            )
        )
        print(self.sql_statement_select_events_all.as_string())

        self.sql_statement_select_events_by_type = (
            SQL_STATEMENT_SELECT_EVENTS_BY_TYPE.format(
                schema=Identifier(self.datastore.schema),
                main_table=Identifier(self.pg_main_table_name),
            )
        )
        print(self.sql_statement_select_events_by_type.as_string())

        self.sql_statement_select_max_id = (
            SQL_STATEMENT_SELECT_MAX_ID.format(
                schema=Identifier(self.datastore.schema),
                main_table=Identifier(self.pg_main_table_name),
            )
        )
        print(self.sql_statement_select_max_id.as_string())


    def read(
        self,
        query: DCBQuery | None = None,
        after: int | None = None,
        limit: int | None = None,
    ) -> tuple[Sequence[DCBSequencedEvent], int | None]:
        if not query or not query.items:
            # Select all.
            with self.datastore.get_connection() as conn:
                with conn.execute(
                    self.sql_statement_select_events_all,
                    {
                        "after": after,
                        "limit": limit,
                    },

                ) as curs:
                    rows = curs.fetchall()

        elif self.one_query_item_one_type(query):
            # Select for one type.
            with self.datastore.get_connection() as conn:
                with conn.execute(
                    self.sql_statement_select_events_by_type,
                    {
                        "event_type": query.items[0].types[0],
                        "after": after,
                        "limit": limit,
                    },
                ) as curs:
                    rows = curs.fetchall()

        elif self.all_query_items_have_tags(query):
            # Select with tags.
            with self.datastore.get_connection() as conn, conn.cursor() as curs:
                pg_dcb_query_items = [
                    self.construct_pg_dcb_query_item(q) for q in query.items
                ]
                curs.execute(
                    self.explain_sql_statement_select_events_by_tags,
                    {
                        "query_items": pg_dcb_query_items,
                        "after": after,
                        "limit": limit,
                    },
                    prepare=True,
                )
                rows = curs.fetchall()
                print("\n".join([r["QUERY PLAN"] for r in rows]))
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
            raise ProgrammingError("Unsupported query: %s" % query)

        # Get max id.
        # TODO: This needs to be atomic, but good enough for testing with single thread.
        with self.datastore.get_connection() as conn:
            with conn.execute(self.sql_statement_select_max_id) as curs:
                row = curs.fetchone()
                if row is None:
                    head = None
                else:
                    head = row["max"]
        events = [
            DCBSequencedEvent(
                event=DCBEvent(
                    type=row["type"],
                    data=row["data"],
                    tags=row["tags"],
                ),
                position=row["id"]
            ) for row in rows
        ]

        return events, head

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        assert len(events) > 0
        if condition is not None:
            failed, head = self.read(condition.fail_if_events_match, after=condition.after)
            if failed:
                raise IntegrityError(failed)
        with self.datastore.get_connection() as conn:
            pg_dcb_events = [self.construct_pg_dcb_event(e) for e in events]
            with conn.execute(
                self.sql_statement_insert_events,
                {
                    "events": pg_dcb_events,
                },
            ) as curs:
                rows = curs.fetchall()
                assert len(rows) > 0
                max_position = max(row["id"] for row in rows)
                return max_position

    def construct_pg_dcb_event(self, dcb_event: DCBEvent) -> PgDCBEvent:
        return self.datastore.pg_python_types[PG_TYPE_NAME_DCB_EVENT_TT](
           type=dcb_event.type, data=dcb_event.data, tags=dcb_event.tags,
        )

    def construct_pg_dcb_query_item(self, dcb_query_item: DCBQueryItem) -> PgDCBQueryItem:
        return self.datastore.pg_python_types[PG_TYPE_NAME_DCB_QUERY_ITEM_TT](
           types=dcb_query_item.types, tags=dcb_query_item.tags,
        )

    def one_query_item_one_type(self, query: DCBQuery) -> bool:
        return len(query.items) == 1 and len(query.items[0].types) == 1 and len(query.items[0].tags) == 0

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
