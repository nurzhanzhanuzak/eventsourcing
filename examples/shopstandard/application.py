from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, ClassVar

from eventsourcing.application import AggregateNotFoundError, Application
from eventsourcing.persistence import IntegrityError
from eventsourcing.utils import get_topic
from examples.aggregate7.orjsonpydantic import OrjsonTranscoder, PydanticMapper
from examples.shopstandard.domain import Cart, CartItem, Product, ProductDetails
from examples.shopstandard.exceptions import (
    InsufficientInventoryError,
    ProductAlreadyInShopError,
    ProductNotFoundInShopError,
)

if TYPE_CHECKING:
    from decimal import Decimal
    from uuid import UUID


class Shop(Application):
    env: ClassVar[dict[str, str]] = {
        "TRANSCODER_TOPIC": get_topic(OrjsonTranscoder),
        "MAPPER_TOPIC": get_topic(PydanticMapper),
    }

    def add_product_to_shop(
        self, product_id: UUID, name: str, description: str, price: Decimal
    ) -> None:
        product = Product(
            product_id=product_id,
            name=name,
            description=description,
            price=price,
        )
        try:
            self.save(product)
        except IntegrityError:
            raise ProductAlreadyInShopError from None

    def adjust_product_inventory(self, product_id: UUID, adjustment: int) -> None:
        try:
            product: Product = self.repository.get(product_id)
        except AggregateNotFoundError:
            msg = f"Product with ID {product_id} not found"
            raise ProductNotFoundInShopError(msg) from None

        product.adjust_inventory(adjustment)
        self.save(product)

    def list_products_in_shop(self) -> list[ProductDetails]:
        # Find all product IDs
        product_ids: list[UUID] = [
            n.originator_id
            for n in self.recorder.select_notifications(start=0, limit=1000000)
            if n.topic.startswith("examples.shopstandard.domain:Product.Created")
        ]

        # Get all products
        products: list[ProductDetails] = []
        for product_id in product_ids:
            product: Product = self.repository.get(product_id)
            products.append(
                ProductDetails(
                    id=product.id,
                    name=product.name,
                    description=product.description,
                    price=product.price,
                    inventory=product.inventory,
                )
            )

        return products

    def get_cart_items(self, cart_id: UUID) -> tuple[CartItem, ...]:
        cart = self._get_cart(cart_id)
        return tuple(cart.items)

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
        requested_products: dict[UUID, int] = defaultdict(int)
        for item in cart.items:
            requested_products[item.product_id] += 1

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

        # Submit cart
        cart.submit()
        self.save(cart)

    def _get_cart(self, cart_id: UUID) -> Cart:
        try:
            return self.repository.get(cart_id)
        except AggregateNotFoundError:
            return Cart(cart_id=cart_id)
