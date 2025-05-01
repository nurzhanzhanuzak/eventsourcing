from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from examples.shopvertical.common import Command, get_events, put_events
from examples.shopvertical.events import AddedProductToShop, DomainEvent
from examples.shopvertical.exceptions import ProductAlreadyInShopError

if TYPE_CHECKING:
    from decimal import Decimal
    from uuid import UUID


@dataclass(frozen=True)
class AddProductToShop(Command):
    product_id: UUID
    name: str
    description: str
    price: Decimal

    def handle(self, events: tuple[DomainEvent, ...]) -> tuple[DomainEvent, ...]:
        if len(events):
            raise ProductAlreadyInShopError
        return (
            AddedProductToShop(
                originator_id=self.product_id,
                originator_version=1,
                name=self.name,
                description=self.description,
                price=self.price,
            ),
        )

    def execute(self) -> int | None:
        return put_events(self.handle(get_events(self.product_id)))
