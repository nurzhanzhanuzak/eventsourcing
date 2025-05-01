from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from examples.shopvertical.common import Command, get_events, put_events
from examples.shopvertical.events import (
    AddedItemToCart,
    ClearedCart,
    DomainEvent,
    RemovedItemFromCart,
)
from examples.shopvertical.exceptions import CartFullError

if TYPE_CHECKING:
    from decimal import Decimal
    from uuid import UUID


@dataclass(frozen=True)
class AddItemToCart(Command):
    cart_id: UUID
    product_id: UUID
    description: str
    price: Decimal
    name: str

    def handle(self, events: tuple[DomainEvent, ...]) -> tuple[DomainEvent, ...]:
        product_ids = []
        for event in events:
            if isinstance(event, AddedItemToCart):
                product_ids.append(event.product_id)
            elif isinstance(event, RemovedItemFromCart):
                product_ids.remove(event.product_id)
            elif isinstance(event, ClearedCart):
                product_ids.clear()

        if len(product_ids) >= 3:
            raise CartFullError

        return (
            AddedItemToCart(
                originator_id=self.cart_id,
                originator_version=len(events) + 1,
                product_id=self.product_id,
                name=self.name,
                description=self.description,
                price=self.price,
            ),
        )

    def execute(self) -> int | None:
        return put_events(self.handle(get_events(self.cart_id)))
