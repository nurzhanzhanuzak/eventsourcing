from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, cast

from eventsourcing.application import AggregateNotFoundError
from eventsourcing.persistence import IntegrityError
from eventsourcing.utils import get_topic
from examples.aggregate7.orjsonpydantic import PydanticApplication
from examples.shopstandard.domain import Cart, CartItem, Product, ProductDetails
from examples.shopstandard.exceptions import (
    InsufficientInventoryError,
    ProductAlreadyInShopError,
    ProductNotFoundInShopError,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from decimal import Decimal
    from uuid import UUID


class Shop(PydanticApplication):
    def add_product_to_shop(
        self, product_id: UUID, name: str, description: str, price: Decimal
    ) -> None:
        try:
            self.save(Product(product_id, name, description, price))
        except IntegrityError:
            raise ProductAlreadyInShopError from None

    def adjust_product_inventory(self, product_id: UUID, adjustment: int) -> None:
        try:
            product: Product = self.repository.get(product_id)
        except AggregateNotFoundError:
            raise ProductNotFoundInShopError from None
        else:
            product.adjust_inventory(adjustment)
            self.save(product)

    def list_products_in_shop(self) -> Sequence[ProductDetails]:
        # TODO: Make this a materialised view.
        return tuple(
            ProductDetails(
                id=product.id,
                name=product.name,
                description=product.description,
                price=product.price,
                inventory=product.inventory,
            )
            for n in self.recorder.select_notifications(
                start=None,
                limit=1000000,
                topics=[get_topic(Product.Created)],
            )
            if (product := cast(Product, self.repository.get(n.originator_id)))
        )

    def get_cart_items(self, cart_id: UUID) -> Sequence[CartItem]:
        return tuple(self._get_cart(cart_id).items)

    def add_item_to_cart(
        self,
        cart_id: UUID,
        product_id: UUID,
        name: str,
        description: str,
        price: Decimal,
    ) -> None:
        cart = self._get_cart(cart_id)
        cart.add_item(product_id, name, description, price)
        self.save(cart)

    def remove_item_from_cart(self, cart_id: UUID, product_id: UUID) -> None:
        cart = self._get_cart(cart_id)
        cart.remove_item(product_id)
        self.save(cart)

    def clear_cart(self, cart_id: UUID) -> None:
        cart = self._get_cart(cart_id)
        cart.clear()
        self.save(cart)

    def submit_cart(self, cart_id: UUID) -> None:
        cart = self._get_cart(cart_id)

        # Check inventory.
        requested_products = Counter(i.product_id for i in cart.items)
        for product_id, requested_amount in requested_products.items():
            try:
                product: Product = self.repository.get(product_id)
            except AggregateNotFoundError:
                current_inventory = 0
            else:
                current_inventory = product.inventory

            if current_inventory < requested_amount:
                msg = f"Insufficient inventory for product with ID {product_id}"
                raise InsufficientInventoryError(msg)

        cart.submit()
        self.save(cart)

    def _get_cart(self, cart_id: UUID) -> Cart:
        try:
            return self.repository.get(cart_id)
        except AggregateNotFoundError:
            return Cart(id=cart_id)
