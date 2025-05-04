from __future__ import annotations

from uuid import UUID  # noqa: TC003

from examples.shopvertical.common import Command, get_events, put_events
from examples.shopvertical.events import AdjustedProductInventory, DomainEvents
from examples.shopvertical.exceptions import ProductNotFoundInShopError


class AdjustProductInventory(Command):
    product_id: UUID
    adjustment: int

    def handle(self, events: DomainEvents) -> DomainEvents:
        if not events:
            raise ProductNotFoundInShopError
        return (
            AdjustedProductInventory(
                originator_id=self.product_id,
                originator_version=len(events) + 1,
                adjustment=self.adjustment,
            ),
        )

    def execute(self) -> int | None:
        return put_events(self.handle(get_events(self.product_id)))
