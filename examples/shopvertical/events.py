from __future__ import annotations

from decimal import Decimal  # noqa: TC003
from uuid import UUID  # noqa: TC003

from examples.aggregate7.immutablemodel import Immutable


class DomainEvent(Immutable):
    originator_id: UUID
    originator_version: int


class AddedProductToShop(DomainEvent):
    name: str
    description: str
    price: Decimal


class AdjustedProductInventory(DomainEvent):
    adjustment: int


class AddedItemToCart(DomainEvent):
    product_id: UUID
    name: str
    description: str
    price: Decimal


class RemovedItemFromCart(DomainEvent):
    product_id: UUID


class ClearedCart(DomainEvent):
    pass


class SubmittedCart(DomainEvent):
    pass
