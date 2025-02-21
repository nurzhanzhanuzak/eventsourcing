import os
from abc import ABC
from typing import Sequence
from unittest import TestCase

from eventsourcing.tests.application import (
    TIMEIT_FACTOR,
    ApplicationTestCase,
    ExampleApplicationTestCase,
)
from eventsourcing.tests.persistence import tmpfile_uris


class WithSQLite(TestCase, ABC):
    timeit_number = 30 * TIMEIT_FACTOR
    expected_factory_topic = "eventsourcing.sqlite:SQLiteFactory"
    uris: Sequence[str] = ()

    def setUp(self) -> None:
        super().setUp()
        os.environ["PERSISTENCE_MODULE"] = "eventsourcing.sqlite"
        os.environ["CREATE_TABLE"] = "y"
        os.environ["SQLITE_DBNAME"] = next(self.uris)

    def tearDown(self) -> None:
        del os.environ["PERSISTENCE_MODULE"]
        del os.environ["CREATE_TABLE"]
        del os.environ["SQLITE_DBNAME"]
        super().tearDown()


class WithSQLiteFile(WithSQLite):
    uris = tmpfile_uris()


def memory_uris():
    db_number = 1
    while True:
        uri = f"file:db{db_number}?mode=memory&cache=shared"
        print(uri)
        yield uri
        db_number += 1


class WithSQLiteInMemory(WithSQLite):
    uris = memory_uris()


class TestApplicationWithSQLiteFile(WithSQLiteFile, ApplicationTestCase):
    def test_catchup_subscription(self):
        self.skipTest("SQLite recorder doesn't support subscriptions")


class TestApplicationWithSQLiteInMemory(WithSQLiteInMemory, ApplicationTestCase):
    def test_catchup_subscription(self):
        self.skipTest("SQLite recorder doesn't support subscriptions")


class TestExampleApplicationWithSQLiteFile(WithSQLiteFile, ExampleApplicationTestCase):
    pass


class TestExampleApplicationWithSQLiteInMemory(
    WithSQLiteInMemory, ExampleApplicationTestCase
):
    pass


del ApplicationTestCase
del ExampleApplicationTestCase
del WithSQLiteFile
del WithSQLiteInMemory
