from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Sequence

from eventsourcing.persistence import Recorder

if TYPE_CHECKING:
    from uuid import UUID


@dataclass(frozen=True)
class PageInfo:
    id: UUID
    slug: str
    title: str
    body: str


class FtsRecorder(Recorder, ABC):
    @abstractmethod
    def insert_pages(self, pages: Sequence[PageInfo]) -> None:
        """
        Insert a sequence of pages (id, slug, title, body).
        """

    @abstractmethod
    def update_pages(self, pages: Sequence[PageInfo]) -> None:
        """
        Update a sequence of pages (id, slug, title, body).
        """

    @abstractmethod
    def search_pages(self, query: str) -> List[UUID]:
        """
        Returns IDs for pages that match query.
        """

    @abstractmethod
    def select_page(self, page_id: UUID) -> PageInfo:
        """
        Returns slug, title and body for given ID.
        """

    def search(self, query: str) -> Sequence[PageInfo]:
        pages = []
        for page_id in self.search_pages(query):
            page = self.select_page(page_id)
            pages.append(page)
        return pages
