from dataclasses import dataclass
from unittest import TestCase
from uuid import uuid4

from eventsourcing.application import Application
from eventsourcing.domain import Aggregate


class TestAlternativeAggregateIdType(TestCase):
    def test(self) -> None:
        @dataclass
        class Product(Aggregate):
            tenant_id: str

            # https://github.com/pyeventsourcing/eventsourcing/issues/294
            @staticmethod
            def create_id(tenant_id: str) -> str:
                """
                Create a deterministic ID for a product based on the tenant ID.

                Args:
                    tenant_id: The tenant ID (as string)
                    to use for generating the product ID

                Returns:
                    A string ID in the format product-{tenant_id}-{uuid}
                """
                product_id = uuid4()
                return f"product-{tenant_id}-{product_id}"

        tenant_id = "tenant1"
        a = Product(tenant_id=tenant_id)
        a_id = a.id
        self.assertIsInstance(a_id, str)

        assert isinstance(a_id, str)
        self.assertTrue(a_id.startswith("product-tenant1-"))

        # Does it work as a POPO application?
        app = Application()
        app.save(a)

        copy: Product = app.repository.get(a_id)
        self.assertIsInstance(copy, Product)
        self.assertEqual(copy.id, a_id)
        self.assertEqual(copy.tenant_id, tenant_id)
