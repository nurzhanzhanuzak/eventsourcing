from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from examples.shopvertical.common import Query, get_all_events
from examples.shopvertical.events import (
    AddedProductToShop,
    AdjustedProductInventory,
    DomainEvent,
)


@dataclass(frozen=True)
class ProductDetails:
    id: UUID
    name: str
    description: str
    price: Decimal
    inventory: int = 0


class ListProductsInShop(Query):
    @staticmethod
    def projection(events: tuple[DomainEvent, ...]) -> tuple[ProductDetails, ...]:
        products: dict[UUID, ProductDetails] = {}
        for event in events:
            if isinstance(event, AddedProductToShop):
                products[event.originator_id] = ProductDetails(
                    id=event.originator_id,
                    name=event.name,
                    description=event.description,
                    price=event.price,
                )
            elif isinstance(event, AdjustedProductInventory):
                product = products[event.originator_id]
                products[event.originator_id] = ProductDetails(
                    id=event.originator_id,
                    name=product.name,
                    description=product.description,
                    price=product.price,
                    inventory=product.inventory + event.adjustment,
                )
        return tuple(products.values())

    def execute(self) -> Any:
        # TODO: Make this a materialised view.
        return self.projection(get_all_events())
