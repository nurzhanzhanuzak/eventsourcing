from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Sequence

from eventsourcing.postgres import (
    PostgresApplicationRecorder,
    PostgresDatastore,
    PostgresRecorder,
)
from examples.contentmanagement.application import PageNotFoundError
from examples.ftscontentmanagement.persistence import FtsRecorder, PageInfo

if TYPE_CHECKING:  # pragma: no cover
    from uuid import UUID

    from psycopg import Cursor
    from psycopg.rows import DictRow

    from eventsourcing.persistence import StoredEvent


class PostgresFtsRecorder(
    PostgresRecorder,
    FtsRecorder,
):
    def __init__(
        self,
        datastore: PostgresDatastore,
        fts_table_name: str = "ftsprojection",
        **kwargs: Any,
    ):
        self.check_table_name_length(fts_table_name, datastore.schema)
        self.fts_table_name = fts_table_name
        super().__init__(datastore, **kwargs)
        self.create_table_statements.append(
            "CREATE TABLE IF NOT EXISTS "
            f"{self.fts_table_name} ("
            "page_id uuid, "
            "page_slug text, "
            "page_title text, "
            "page_body text, "
            "PRIMARY KEY "
            "(page_id))"
        )
        self.create_table_statements.append(
            f"CREATE INDEX IF NOT EXISTS {self.fts_table_name}_idx "
            f"ON {self.fts_table_name} "
            "USING GIN (to_tsvector('english', page_body))"
        )
        self.select_page_statement = (
            f"SELECT page_slug, page_title, page_body FROM {fts_table_name}"
            " WHERE page_id = %s"
        )
        self.insert_page_statement = (
            f"INSERT INTO {fts_table_name} VALUES (%s, %s, %s, %s)"
        )
        self.update_page_statement = (
            f"UPDATE {fts_table_name}"
            " SET page_slug = %s, page_title = %s, page_body = %s WHERE page_id = %s"
        )
        self.search_pages_statement = (
            f"SELECT page_id FROM {fts_table_name} WHERE"
            " to_tsvector('english', page_body) @@ websearch_to_tsquery('english', %s)"
        )

    def insert_pages(self, pages: Sequence[PageInfo]) -> None:
        with self.datastore.transaction(commit=True) as curs:
            self._insert_pages(curs, pages)

    def update_pages(self, pages: Sequence[PageInfo]) -> None:
        with self.datastore.transaction(commit=True) as curs:
            self._update_pages(curs, pages)

    def _insert_pages(self, c: Cursor[DictRow], pages: Sequence[PageInfo]) -> None:
        for page in pages:
            params = (page.id, page.slug, page.title, page.body)
            c.execute(self.insert_page_statement, params, prepare=True)

    def _update_pages(self, c: Cursor[DictRow], pages: Sequence[PageInfo]) -> None:
        for page in pages:
            params = (page.slug, page.title, page.body, page.id)
            c.execute(self.update_page_statement, params, prepare=True)

    def search_pages(self, query: str) -> List[UUID]:
        with self.datastore.transaction(commit=False) as curs:
            curs.execute(self.search_pages_statement, [query], prepare=True)
            return [row["page_id"] for row in curs.fetchall()]

    def select_page(self, page_id: UUID) -> PageInfo:
        with self.datastore.transaction(commit=False) as curs:
            curs.execute(self.select_page_statement, [str(page_id)], prepare=True)
            for row in curs.fetchall():
                return PageInfo(
                    id=page_id,
                    slug=row["page_slug"],
                    title=row["page_title"],
                    body=row["page_body"],
                )
        msg = f"Page ID {page_id} not found"
        raise PageNotFoundError(msg)


class PostgresFtsApplicationRecorder(PostgresFtsRecorder, PostgresApplicationRecorder):
    def _insert_events(
        self,
        c: Cursor[DictRow],
        stored_events: List[StoredEvent],
        *,
        insert_pages: Sequence[PageInfo] = (),
        update_pages: Sequence[PageInfo] = (),
        **kwargs: Any,
    ) -> None:
        notification_ids = super()._insert_events(c, stored_events, **kwargs)
        self._insert_pages(c, pages=insert_pages)
        self._update_pages(c, pages=update_pages)
        return notification_ids
