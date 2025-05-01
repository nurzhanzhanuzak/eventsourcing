from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from examples.shopvertical.common import Command, get_events, put_events
from examples.shopvertical.events import AdjustedProductInventory, DomainEvent
from examples.shopvertical.exceptions import ProductNotFoundInShopError

if TYPE_CHECKING:
    from uuid import UUID


@dataclass(frozen=True)
class AdjustProductInventory(Command):
    product_id: UUID
    adjustment: int

    def handle(self, events: tuple[DomainEvent, ...]) -> tuple[DomainEvent, ...]:
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
