from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal  # noqa: TC003
from uuid import UUID  # noqa: TC003

from eventsourcing.domain import event
from examples.aggregate7.immutablemodel import Immutable
from examples.aggregate8.mutablemodel import Aggregate
from examples.shopstandard.exceptions import (
    CartAlreadySubmittedError,
    CartFullError,
    ProductNotInCartError,
)


class ProductDetails(Immutable):
    id: UUID
    name: str
    description: str
    price: Decimal
    inventory: int


@dataclass
class Product(Aggregate):
    product_id: UUID
    name: str
    description: str
    price: Decimal
    inventory: int = 0

    @staticmethod
    def create_id(product_id: UUID) -> UUID:
        return product_id

    class InventoryAdjusted(Aggregate.Event):
        adjustment: int

    @event(InventoryAdjusted)
    def adjust_inventory(self, adjustment: int) -> None:
        self.inventory += adjustment


class CartItem(Immutable):
    product_id: UUID
    name: str
    description: str
    price: Decimal


@dataclass
class Cart(Aggregate):
    cart_id: UUID
    items: list[CartItem] = field(default_factory=list, init=False)
    is_submitted: bool = False

    @staticmethod
    def create_id(cart_id: UUID) -> UUID:
        return cart_id

    class ItemAdded(Aggregate.Event):
        product_id: UUID
        name: str
        description: str
        price: Decimal

    class ItemRemoved(Aggregate.Event):
        product_id: UUID

    class Cleared(Aggregate.Event):
        pass

    class Submitted(Aggregate.Event):
        pass

    @event(ItemAdded)
    def add_item(
        self, product_id: UUID, name: str, description: str, price: Decimal
    ) -> None:
        if self.is_submitted:
            raise CartAlreadySubmittedError

        if len(self.items) >= 3:
            raise CartFullError

        self.items.append(
            CartItem(
                product_id=product_id,
                name=name,
                description=description,
                price=price,
            )
        )

    @event(ItemRemoved)
    def remove_item(self, product_id: UUID) -> None:
        if self.is_submitted:
            raise CartAlreadySubmittedError

        for i, item in enumerate(self.items):
            if item.product_id == product_id:
                self.items.pop(i)
                break
        else:
            raise ProductNotInCartError

    @event(Cleared)
    def clear(self) -> None:
        if self.is_submitted:
            raise CartAlreadySubmittedError
        self.items = []

    @event(Submitted)
    def submit(self) -> None:
        if self.is_submitted:
            raise CartAlreadySubmittedError
        self.is_submitted = True
