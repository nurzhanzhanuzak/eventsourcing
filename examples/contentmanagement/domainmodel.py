from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import cast
from uuid import NAMESPACE_URL, UUID, uuid5

from eventsourcing.domain import Aggregate, DomainEvent, event
from examples.contentmanagement.utils import apply_diff, create_diff

user_id_cvar: ContextVar[UUID | None] = ContextVar("user_id", default=None)
"""
Context variable holding a user ID for the current thread.
"""


@dataclass
class Page(Aggregate):
    title: str
    """The title of the page."""

    slug: str
    """The slug of the page - used in URLs."""

    body: str
    """The proper content of the page."""

    modified_by: UUID | None = field(init=False)
    """The ID of the user who last modified the page."""

    class Event(Aggregate.Event):
        user_id: UUID | None = field(default_factory=user_id_cvar.get, init=False)

        def apply(self, aggregate: Aggregate) -> None:
            """Sets the aggregate's `modified_by` attribute to the
            value of the event's `user_id` attribute.
            """
            cast("Page", aggregate).modified_by = self.user_id

    @event("SlugUpdated")
    def update_slug(self, slug: str) -> None:
        self.slug = slug

    @event("TitleUpdated")
    def update_title(self, title: str) -> None:
        self.title = title

    def update_body(self, body: str) -> None:
        diff = create_diff(old=self.body, new=body)
        self._update_body(diff=diff)

    class Created(Aggregate.Created, Event):
        title: str
        slug: str
        body: str

    class BodyUpdated(Event):
        diff: str

    @event(BodyUpdated)
    def _update_body(self, diff: str) -> None:
        new_body = apply_diff(old=self.body, diff=diff)
        self.body = new_body


@dataclass
class Slug(Aggregate):
    name: str
    page_id: UUID | None

    class Event(Aggregate.Event):
        pass

    @staticmethod
    def create_id(name: str) -> UUID:
        return uuid5(NAMESPACE_URL, f"/slugs/{name}")

    @event("PageUpdated")
    def update_page(self, page_id: UUID | None) -> None:
        self.page_id = page_id


class PageLogged(DomainEvent):
    page_id: UUID
