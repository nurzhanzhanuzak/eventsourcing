from __future__ import annotations

from decimal import Decimal  # noqa: TC003
from uuid import UUID  # noqa: TC003

from examples.shopvertical.common import Command, get_events, put_events
from examples.shopvertical.events import AddedProductToShop, DomainEvent
from examples.shopvertical.exceptions import ProductAlreadyInShopError


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
