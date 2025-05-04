from __future__ import annotations

from uuid import UUID  # noqa: TC003

from examples.shopvertical.common import Command, get_events, put_events
from examples.shopvertical.events import (
    AddedItemToCart,
    ClearedCart,
    DomainEvents,
    RemovedItemFromCart,
    SubmittedCart,
)
from examples.shopvertical.exceptions import (
    CartAlreadySubmittedError,
    ProductNotInCartError,
)


class RemoveItemFromCart(Command):
    cart_id: UUID
    product_id: UUID

    def handle(self, events: DomainEvents) -> DomainEvents:
        product_ids = []
        is_submitted = False

        for event in events:
            if isinstance(event, AddedItemToCart):
                product_ids.append(event.product_id)
            elif isinstance(event, RemovedItemFromCart):
                product_ids.remove(event.product_id)
            elif isinstance(event, ClearedCart):
                product_ids.clear()
            elif isinstance(event, SubmittedCart):
                is_submitted = True

        if is_submitted:
            raise CartAlreadySubmittedError

        if self.product_id not in product_ids:
            raise ProductNotInCartError
        return (
            RemovedItemFromCart(
                originator_id=self.cart_id,
                originator_version=len(events) + 1,
                product_id=self.product_id,
            ),
        )

    def execute(self) -> int | None:
        return put_events(self.handle(get_events(self.cart_id)))
