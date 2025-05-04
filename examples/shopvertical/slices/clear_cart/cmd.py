from __future__ import annotations

from uuid import UUID  # noqa: TC003

from examples.shopvertical.common import Command, get_events, put_events
from examples.shopvertical.events import ClearedCart, DomainEvent, SubmittedCart
from examples.shopvertical.exceptions import CartAlreadySubmittedError


class ClearCart(Command):
    cart_id: UUID

    def handle(self, events: tuple[DomainEvent, ...]) -> tuple[DomainEvent, ...]:
        is_submitted = False
        for event in events:
            if isinstance(event, SubmittedCart):
                is_submitted = True

        if is_submitted:
            raise CartAlreadySubmittedError

        return (
            ClearedCart(
                originator_id=self.cart_id,
                originator_version=len(events) + 1,
            ),
        )

    def execute(self) -> int | None:
        return put_events(self.handle(get_events(self.cart_id)))
