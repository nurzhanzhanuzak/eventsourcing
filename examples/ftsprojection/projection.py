from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.persistence import Tracking, TrackingRecorder
from eventsourcing.postgres import PostgresTrackingRecorder
from eventsourcing.projection import Projection
from examples.contentmanagement.domainmodel import Page  # noqa: TCH001
from examples.contentmanagement.utils import apply_diff
from examples.ftscontentmanagement.persistence import FtsRecorder, PageInfo
from examples.ftscontentmanagement.postgres import PostgresFtsRecorder

if TYPE_CHECKING:
    from collections.abc import Sequence

    from eventsourcing.domain import DomainEventProtocol


class FtsViewInterface(FtsRecorder, TrackingRecorder, ABC):
    @abstractmethod
    def insert_pages_with_tracking(
        self, pages: Sequence[PageInfo], tracking: Tracking
    ) -> None:
        pass

    @abstractmethod
    def update_pages_with_tracking(
        self, pages: Sequence[PageInfo], tracking: Tracking
    ) -> None:
        pass


class FtsProjection(Projection[FtsViewInterface]):
    @singledispatchmethod
    def process_event(
        self, domain_event: DomainEventProtocol, tracking: Tracking
    ) -> None:
        self.view.insert_tracking(tracking)

    @process_event.register
    def page_created(self, domain_event: Page.Created, tracking: Tracking) -> None:
        new_page = PageInfo(
            id=domain_event.originator_id,
            slug=domain_event.slug,
            title=domain_event.title,
            body=domain_event.body,
        )
        self.view.insert_pages_with_tracking([new_page], tracking)

    @process_event.register
    def body_updated(self, domain_event: Page.BodyUpdated, tracking: Tracking) -> None:
        page_id = domain_event.originator_id
        old_page = self.view.select_page(page_id)
        new_page = PageInfo(
            id=page_id,
            slug=old_page.slug,
            title=old_page.title,
            body=apply_diff(old_page.body, domain_event.diff),
        )
        self.view.update_pages_with_tracking([new_page], tracking)


class PostgresFtsView(PostgresFtsRecorder, PostgresTrackingRecorder, FtsViewInterface):
    def insert_pages_with_tracking(
        self, pages: Sequence[PageInfo], tracking: Tracking
    ) -> None:
        with self.datastore.transaction(commit=True) as curs:
            self._insert_pages(curs, pages)
            self._insert_tracking(curs, tracking)

    def update_pages_with_tracking(
        self, pages: Sequence[PageInfo], tracking: Tracking
    ) -> None:
        with self.datastore.transaction(commit=True) as curs:
            self._update_pages(curs, pages)
            self._insert_tracking(curs, tracking)
