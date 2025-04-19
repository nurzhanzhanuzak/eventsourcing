import warnings
from dataclasses import _DataclassParams, dataclass  # type: ignore[attr-defined]
from datetime import datetime, timedelta, timezone
from time import sleep
from unittest.case import TestCase
from uuid import UUID, uuid4

import eventsourcing.domain
from eventsourcing.domain import (
    DomainEvent,
    MetaDomainEvent,
    create_utc_datetime_now,
    datetime_now_with_tzinfo,
)


class TestMetaDomainEvent(TestCase):
    def test_class_instance_defined_as_frozen_dataclass(self) -> None:
        class A(metaclass=MetaDomainEvent):
            pass

        self.assertIsInstance(A, type)
        self.assertTrue("__dataclass_params__" in A.__dict__)
        dataclass_params = A.__dict__["__dataclass_params__"]
        self.assertIsInstance(dataclass_params, _DataclassParams)
        self.assertTrue(dataclass_params.frozen)


class TestDomainEvent(TestCase):
    def test_domain_event_class_is_a_meta_domain_event(self) -> None:
        self.assertIsInstance(DomainEvent, MetaDomainEvent)

    def test_create_timestamp(self) -> None:
        before = datetime.now(tz=timezone.utc)
        sleep(1e-5)
        timestamp = DomainEvent.create_timestamp()
        sleep(1e-5)
        after = datetime.now(tz=timezone.utc)
        self.assertGreater(timestamp, before)
        self.assertGreater(after, timestamp)

    def test_domain_event_instance(self) -> None:
        originator_id = uuid4()
        originator_version = 101
        timestamp = DomainEvent.create_timestamp()
        a = DomainEvent(
            originator_id=originator_id,
            originator_version=originator_version,
            timestamp=timestamp,
        )
        self.assertEqual(a.originator_id, originator_id)
        self.assertEqual(a.originator_version, originator_version)
        self.assertEqual(a.timestamp, timestamp)

    def test_examples(self) -> None:
        # Define an 'account opened' domain event.
        @dataclass(frozen=True)
        class AccountOpened(DomainEvent):
            full_name: str

        # Create an 'account opened' event.
        event3 = AccountOpened(
            originator_id=uuid4(),
            originator_version=0,
            timestamp=AccountOpened.create_timestamp(),
            full_name="Alice",
        )

        self.assertEqual(event3.full_name, "Alice")
        assert isinstance(event3.originator_id, UUID)
        self.assertEqual(event3.originator_version, 0)

        # Define a 'full name updated' domain event.
        @dataclass(frozen=True)
        class FullNameUpdated(DomainEvent):
            full_name: str
            timestamp: datetime

        # Create a 'full name updated' domain event.
        event4 = FullNameUpdated(
            originator_id=event3.originator_id,
            originator_version=1,
            timestamp=FullNameUpdated.create_timestamp(),
            full_name="Bob",
        )

        # Check the attribute values of the domain event.
        self.assertEqual(event4.full_name, "Bob")
        assert isinstance(event4.originator_id, UUID)
        self.assertEqual(event4.originator_version, 1)


class TestDatetimeNowWithTzinfo(TestCase):
    def test(self) -> None:
        # Check datetime_now_with_tzinfo() returns a datetime with tzinfo.
        timestamp = datetime_now_with_tzinfo()
        self.assertIsInstance(timestamp, datetime)
        self.assertEqual(timestamp.tzinfo, timezone.utc)

        orig_tzinfo = eventsourcing.domain.TZINFO
        alt_tzinfo = timezone(offset=timedelta(hours=1), name="AltTimeZone")
        eventsourcing.domain.TZINFO = alt_tzinfo
        try:
            timestamp = datetime_now_with_tzinfo()
            self.assertNotEqual(timestamp.tzinfo, timezone.utc)
            self.assertEqual(timestamp.tzinfo, alt_tzinfo)
        finally:
            eventsourcing.domain.TZINFO = orig_tzinfo

        # Verify deprecation warning for create_utc_datetime_now().
        with warnings.catch_warnings(record=True) as w:
            timestamp = create_utc_datetime_now()

        self.assertIsInstance(timestamp, datetime)
        self.assertEqual(timestamp.tzinfo, timezone.utc)

        self.assertEqual(len(w), 1)
        self.assertIs(w[-1].category, DeprecationWarning)
        self.assertIn(
            (
                "'create_utc_datetime_now()' is deprecated, "
                "use 'datetime_now_with_tzinfo()' instead"
            ),
            str(w[-1].message),
        )
