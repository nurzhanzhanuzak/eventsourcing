import os
from unittest import TestCase

from eventsourcing.tests.application import (
    ApplicationTestCase,
    ExampleApplicationTestCase,
)
from eventsourcing.tests.postgres_utils import drop_tables


class WithPostgres(TestCase):
    expected_factory_topic = "eventsourcing.postgres:PostgresFactory"

    def setUp(self) -> None:
        super().setUp()

        os.environ["PERSISTENCE_MODULE"] = "eventsourcing.postgres"
        os.environ["CREATE_TABLE"] = "y"
        os.environ["POSTGRES_DBNAME"] = "eventsourcing"
        os.environ["POSTGRES_HOST"] = "127.0.0.1"
        os.environ["POSTGRES_PORT"] = "5432"
        os.environ["POSTGRES_USER"] = "eventsourcing"
        os.environ["POSTGRES_PASSWORD"] = "eventsourcing"  # noqa: S105
        os.environ["POSTGRES_SCHEMA"] = "public"
        drop_tables()

    def tearDown(self) -> None:
        drop_tables()

        del os.environ["PERSISTENCE_MODULE"]
        del os.environ["CREATE_TABLE"]
        del os.environ["POSTGRES_DBNAME"]
        del os.environ["POSTGRES_HOST"]
        del os.environ["POSTGRES_PORT"]
        del os.environ["POSTGRES_USER"]
        del os.environ["POSTGRES_PASSWORD"]
        del os.environ["POSTGRES_SCHEMA"]

        super().tearDown()


class TestApplicationWithPostgres(WithPostgres, ApplicationTestCase):
    pass


class TestExampleApplicationWithPostgres(WithPostgres, ExampleApplicationTestCase):
    pass


del ApplicationTestCase
del ExampleApplicationTestCase
del WithPostgres
