from unittest.case import TestCase

import eventsourcing.popo
from eventsourcing.persistence import InfrastructureFactory
from eventsourcing.utils import Environment, get_topic


class TestInfrastructureFactory(TestCase):
    def test_constructs_popo_factory_by_default(self):
        factory = InfrastructureFactory.construct()
        self.assertIsInstance(factory, InfrastructureFactory)
        self.assertIsInstance(factory, eventsourcing.popo.Factory)

    def test_construct_raises_exception(self):
        with self.assertRaises(EnvironmentError):
            InfrastructureFactory.construct(
                Environment(
                    env={InfrastructureFactory.PERSISTENCE_MODULE: "invalid topic"}
                )
            )

        with self.assertRaises(AssertionError):
            InfrastructureFactory.construct(
                Environment(
                    env={InfrastructureFactory.PERSISTENCE_MODULE: get_topic(object)}
                )
            )
