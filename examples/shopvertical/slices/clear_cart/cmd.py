from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from examples.shopvertical.common import Command, get_events, put_events
from examples.shopvertical.events import ClearedCart, DomainEvent

if TYPE_CHECKING:
    from uuid import UUID


@dataclass(frozen=True)
class ClearCart(Command):
    cart_id: UUID

    def handle(self, events: tuple[DomainEvent, ...]) -> tuple[DomainEvent, ...]:
        return (
            ClearedCart(
                originator_id=self.cart_id,
                originator_version=len(events) + 1,
            ),
        )

    def execute(self) -> int | None:
        return put_events(self.handle(get_events(self.cart_id)))
