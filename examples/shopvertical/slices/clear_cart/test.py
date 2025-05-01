import unittest
from typing import cast
from uuid import uuid4

from examples.shopvertical.events import ClearedCart, DomainEvent
from examples.shopvertical.slices.clear_cart.cmd import (
    ClearCart,
)


class TestClearCart(unittest.TestCase):
    def test(self) -> None:
        cart_id = uuid4()
        cmd = ClearCart(
            cart_id=cart_id,
        )
        events: tuple[DomainEvent, ...] = ()
        new_events = cmd.handle(events)
        self.assertEqual(len(new_events), 1)
        self.assertIsInstance(new_events[0], ClearedCart)
        new_event = cast(ClearedCart, new_events[0])
        self.assertEqual(new_event.originator_id, cart_id)
        self.assertEqual(new_event.originator_version, 1)
