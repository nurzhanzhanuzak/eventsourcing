from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from examples.shopvertical.common import Command, get_events, put_events
from examples.shopvertical.events import (
    AddedItemToCart,
    AdjustedProductInventory,
    ClearedCart,
    DomainEvent,
    RemovedItemFromCart,
    SubmittedCart,
)
from examples.shopvertical.exceptions import InsufficientInventoryError

if TYPE_CHECKING:
    from uuid import UUID


@dataclass(frozen=True)
class SubmitCart(Command):
    cart_id: UUID

    def handle(self, events: tuple[DomainEvent, ...]) -> tuple[DomainEvent, ...]:
        requested_products: dict[UUID, int] = defaultdict(int)
        for event in events:
            if isinstance(event, AddedItemToCart):
                requested_products[event.product_id] += 1
            elif isinstance(event, RemovedItemFromCart):
                requested_products[event.product_id] -= 1
            elif isinstance(event, ClearedCart):
                requested_products.clear()

        # Check inventory.
        for product_id, requested_amount in requested_products.items():
            current_inventory = 0
            for product_event in get_events(product_id):
                if isinstance(product_event, AdjustedProductInventory):
                    current_inventory += product_event.adjustment
            if current_inventory < requested_amount:
                raise InsufficientInventoryError(product_id)

        return (
            SubmittedCart(
                originator_id=self.cart_id,
                originator_version=len(events) + 1,
            ),
        )

    def execute(self) -> int | None:
        return put_events(self.handle(get_events(self.cart_id)))
