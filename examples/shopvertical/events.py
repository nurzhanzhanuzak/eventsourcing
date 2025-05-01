from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class DomainEvent(BaseModel, frozen=True):
    model_config = ConfigDict(extra="forbid")

    originator_id: UUID
    originator_version: int


class AddedProductToShop(DomainEvent, frozen=True):
    name: str
    description: str
    price: Decimal


class AdjustedProductInventory(DomainEvent, frozen=True):
    adjustment: int


class AddedItemToCart(DomainEvent, frozen=True):
    product_id: UUID
    name: str
    description: str
    price: Decimal


class RemovedItemFromCart(DomainEvent, frozen=True):
    product_id: UUID


class ClearedCart(DomainEvent, frozen=True):
    pass


class SubmittedCart(DomainEvent, frozen=True):
    pass
