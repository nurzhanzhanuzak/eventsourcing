from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from eventsourcing.domain import event
from examples.aggregate8.mutablemodel import Aggregate
from examples.shopstandard.exceptions import (
    CartAlreadySubmittedError,
    CartFullError,
    ProductNotInCartError,
)


class ProductDetails(BaseModel, frozen=True):
    model_config = ConfigDict(extra="forbid")

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

    class InventoryAdjusted(Aggregate.Event, frozen=True):
        adjustment: int

    @event(InventoryAdjusted)
    def adjust_inventory(self, adjustment: int) -> None:
        self.inventory += adjustment


class CartItem(BaseModel, frozen=True):
    model_config = ConfigDict(extra="forbid")

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

    class ItemAdded(Aggregate.Event, frozen=True):
        product_id: UUID
        name: str
        description: str
        price: Decimal

    class ItemRemoved(Aggregate.Event, frozen=True):
        product_id: UUID

    class Cleared(Aggregate.Event, frozen=True):
        pass

    class Submitted(Aggregate.Event, frozen=True):
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
