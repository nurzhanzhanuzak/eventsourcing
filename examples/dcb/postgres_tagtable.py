from __future__ import annotations

from typing import Any, Sequence

from psycopg.sql import SQL, Identifier

from eventsourcing.postgres import PostgresDatastore
from examples.dcb.api import DCBAppendCondition, DCBEvent, DCBQuery, DCBSequencedEvent
from examples.dcb.postgres_textsearch import PostgresDCBEventStore


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

    def read(
        self,
        query: DCBQuery | None = None,
        after: int | None = None,
        limit: int | None = None,
    ) -> tuple[Sequence[DCBSequencedEvent], int | None]:
        return [], None

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        return 1
