from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from psycopg.sql import SQL, Identifier

from eventsourcing.postgres import (
    PostgresApplicationRecorder,
    PostgresDatastore,
    PostgresFactory,
)
from examples.searchabletimestamps.persistence import SearchableTimestampsRecorder

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime
    from uuid import UUID

    from psycopg import Cursor
    from psycopg.rows import DictRow

    from eventsourcing.persistence import ApplicationRecorder, StoredEvent


class SearchableTimestampsApplicationRecorder(
    SearchableTimestampsRecorder, PostgresApplicationRecorder
):
    def __init__(
        self,
        datastore: PostgresDatastore,
        events_table_name: str = "stored_events",
        event_timestamps_table_name: str = "event_timestamps",
    ):
        super().__init__(datastore, events_table_name=events_table_name)
        self.check_table_name_length(event_timestamps_table_name)
        self.event_timestamps_table_name = event_timestamps_table_name

        self.sql_create_statements.append(
            SQL(
                "CREATE TABLE IF NOT EXISTS {0}.{1} ("
                "originator_id uuid NOT NULL, "
                "timestamp timestamp with time zone, "
                "originator_version bigint NOT NULL, "
                "PRIMARY KEY "
                "(originator_id, timestamp))"
            ).format(
                Identifier(self.datastore.schema),
                Identifier(self.event_timestamps_table_name),
            )
        )

        self.insert_event_timestamp_statement = SQL(
            "INSERT INTO {0}.{1} VALUES (%s, %s, %s)"
        ).format(
            Identifier(self.datastore.schema),
            Identifier(self.event_timestamps_table_name),
        )

        self.select_event_timestamp_statement = SQL(
            "SELECT originator_version FROM {0}.{1} WHERE "
            "originator_id = %s AND "
            "timestamp <= %s "
            "ORDER BY originator_version DESC "
            "LIMIT 1"
        ).format(
            Identifier(self.datastore.schema),
            Identifier(self.event_timestamps_table_name),
        )

    def _insert_events(
        self,
        curs: Cursor[DictRow],
        stored_events: Sequence[StoredEvent],
        **kwargs: Any,
    ) -> None:
        # Insert event timestamps.
        event_timestamps_data = cast(
            "list[tuple[UUID, datetime, int]]", kwargs.get("event_timestamps_data")
        )
        for event_timestamp_data in event_timestamps_data:
            curs.execute(
                query=self.insert_event_timestamp_statement,
                params=event_timestamp_data,
                prepare=True,
            )
        super()._insert_events(curs, stored_events, **kwargs)

    def get_version_at_timestamp(
        self, originator_id: UUID, timestamp: datetime
    ) -> int | None:
        with self.datastore.get_connection() as conn, conn.cursor() as curs:
            curs.execute(
                query=self.select_event_timestamp_statement,
                params=(originator_id, timestamp),
                prepare=True,
            )
            for row in curs.fetchall():
                return row["originator_version"]
            return None


class SearchableTimestampsInfrastructureFactory(PostgresFactory):
    def application_recorder(self) -> ApplicationRecorder:
        prefix = self.env.name.lower() or "stored"
        events_table_name = prefix + "_events"
        event_timestamps_table_name = prefix + "_timestamps"
        recorder = SearchableTimestampsApplicationRecorder(
            datastore=self.datastore,
            events_table_name=events_table_name,
            event_timestamps_table_name=event_timestamps_table_name,
        )
        recorder.create_table()
        return recorder


del PostgresFactory
