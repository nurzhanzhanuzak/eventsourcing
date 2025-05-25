from __future__ import annotations

import json
from threading import Event, Thread
from time import sleep
from typing import TYPE_CHECKING, Any, cast
from unittest import TestCase
from uuid import uuid4

import pytest

from eventsourcing.persistence import IntegrityError, ProgrammingError
from eventsourcing.postgres import PostgresDatastore, PostgresRecorder
from eventsourcing.tests.postgres_utils import drop_tables
from eventsourcing.utils import Environment
from examples.dcb.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBEventStore,
    DCBQuery,
    DCBQueryItem,
    DCBSequencedEvent,
)
from examples.dcb.popo import InMemoryDCBEventStore
from examples.dcb.postgres_tt import PostgresDCBEventStoreTT
from examples.dcb.postgres_ts import (
    PostgresDCBEventStore,
    PostgresDCBEventStoreTS,
    PostgresTSDCBFactory,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pytest_benchmark.fixture import BenchmarkFixture


# https://dcb.events/specification/


class TestDCBObjects(TestCase):
    def test_query_item(self) -> None:
        # Can have zero tags and zero items.
        item = DCBQueryItem()
        self.assertEqual([], item.types)
        self.assertEqual([], item.tags)

        # Can have more than zero types.
        item = DCBQueryItem(types=["EventType1", "EventType2"])
        self.assertEqual(["EventType1", "EventType2"], item.types)
        self.assertEqual([], item.tags)

        # Can have more than zero tags.
        item = DCBQueryItem(tags=["tag1", "tag2"])
        self.assertEqual([], item.types)
        self.assertEqual(["tag1", "tag2"], item.tags)

    def test_query(self) -> None:
        # Can have zero items.
        query = DCBQuery()
        self.assertEqual(0, len(query.items))

        # Can have more than zero items.
        query = DCBQuery(items=[DCBQueryItem(), DCBQueryItem()])
        self.assertEqual(2, len(query.items))

    def test_append_condition(self) -> None:
        query = DCBQuery()
        # Must have one "fail if events match" query.
        condition = DCBAppendCondition(fail_if_events_match=query)
        self.assertEqual(query, condition.fail_if_events_match)
        self.assertEqual(None, condition.after)

        # May have an integer "after" value.
        condition = DCBAppendCondition(fail_if_events_match=query, after=12)
        self.assertEqual(query, condition.fail_if_events_match)
        self.assertEqual(12, condition.after)

    def test_event(self) -> None:
        # Must contain "type" and "data".
        event = DCBEvent(type="EventType1", data=b"data")
        self.assertEqual("EventType1", event.type)
        self.assertEqual(b"data", event.data)
        self.assertEqual([], event.tags)

        # May contain tags.
        event = DCBEvent(type="EventType1", data=b"data", tags=["tag1", "tag2"])
        self.assertEqual("EventType1", event.type)
        self.assertEqual(b"data", event.data)
        self.assertEqual(["tag1", "tag2"], event.tags)

    def test_sequenced_event(self) -> None:
        sequenced_event = DCBSequencedEvent(
            event=DCBEvent(type="EventType1", data=b"data"),
            position=3,
        )
        self.assertEqual("EventType1", sequenced_event.event.type)
        self.assertEqual(b"data", sequenced_event.event.data)
        self.assertEqual(3, sequenced_event.position)


class DCBEventStoreTestCase(TestCase):

    def _test_event_store(self, eventstore: DCBEventStore) -> None:
        # Query for all with zero rows, expect no results.
        result, head = eventstore.read()
        self.assertEqual(0, len(list(result)))
        self.assertIsNone(head)

        # Must atomically persist one or many events.
        position = eventstore.append(
            events=(DCBEvent(type="EventType1", data=b"data1", tags=["tagX"]),),
        )
        self.assertEqual(1, position)

        # Query for all with one row, expect one result.
        result, head = eventstore.read()
        self.assertEqual(1, len(result))
        self.assertEqual(1, head)

        # Append more than one...
        position = eventstore.append(
            events=[
                DCBEvent(type="EventType2", data=b"data2", tags=["tagA", "tagB"]),
                DCBEvent(type="EventType3", data=b"data3", tags=["tagA", "tagC"]),
            ],
        )
        self.assertEqual(3, position)
        result, head = eventstore.read()
        self.assertEqual(3, len(result))
        self.assertEqual(3, head)

        # Query for all with three rows, expect three results.
        result, head = eventstore.read()
        self.assertEqual(3, len(result))
        self.assertEqual(3, head)

        # Can query for type "EventType1".
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType1"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(1, result[0].position)
        self.assertEqual("EventType1", result[0].event.type)
        self.assertEqual(b"data1", result[0].event.data)
        self.assertEqual(["tagX"], result[0].event.tags)
        self.assertEqual(3, head)

        # Can query for type "EventType2".
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual(b"data2", result[0].event.data)
        self.assertEqual(["tagA", "tagB"], result[0].event.tags)
        self.assertEqual(3, head)

        # Can query for type "EventType1" after position 1 - no events.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType1"])]), after=1
        )
        self.assertEqual(0, len(result))
        self.assertEqual(3, head)

        # Can query for type "EventType2" after position 1 - one event.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"])]), after=1
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual(3, head)

        # Can query for tag "tagA" - two events with "tagA".
        result, head = eventstore.read(DCBQuery(items=[DCBQueryItem(tags=["tagA"])]))
        self.assertEqual(2, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual(3, result[1].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual("EventType3", result[1].event.type)
        self.assertEqual(3, head)

        # Can query for tag "tagB" - one event with "tagB".
        result, head = eventstore.read(DCBQuery(items=[DCBQueryItem(tags=["tagB"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual(3, head)

        # Can query for tag "tagC" - one event with "tagC".
        result, head = eventstore.read(DCBQuery(items=[DCBQueryItem(tags=["tagC"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(3, result[0].position)
        self.assertEqual("EventType3", result[0].event.type)
        self.assertEqual(3, head)

        # Can query for tags "tagA" and tagB" - one event has both.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(tags=["tagA", "tagB"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual(3, head)

        # Can query for tags "tagB" and tagC" - no events have both.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(tags=["tagB", "tagC"])])
        )
        self.assertEqual(0, len(result))
        self.assertEqual(3, head)

        # Can query for tags "tagB" or tagC" - two events have one or the other.
        result, head = eventstore.read(
            DCBQuery(
                items=[
                    DCBQueryItem(tags=["tagB"]),
                    DCBQueryItem(tags=["tagC"]),
                ]
            )
        )
        self.assertEqual(2, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual(3, result[1].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual("EventType3", result[1].event.type)
        self.assertEqual(3, head)

        # Can query for tags "tagB" or tagD" - only one event.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(tags=["tagB"]), DCBQueryItem(tags=["tagD"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual(3, head)

        # Can query for tag "tagA" after position 2 - only one event.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(tags=["tagA"])]),
            after=2,
        )
        self.assertEqual(1, len(result))
        self.assertEqual(3, result[0].position)
        self.assertEqual("EventType3", result[0].event.type)
        self.assertEqual(3, head)

        # Can query for type "EventType1" and tag "tagA" - zero events.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType1"], tags=["tagA"])])
        )
        self.assertEqual(0, len(result))
        self.assertEqual(3, head)

        # Can query for type "EventType2" and tag "tagA" - only one event.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"], tags=["tagA"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual(3, head)

        # Can query for type "EventType2" and tag "tagA" after position 2 - no events.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"], tags=["tagA"])]),
            after=2,
        )
        self.assertEqual(0, len(result))
        self.assertEqual(3, head)

        # Append must fail if event store has events matching append condition.

        # Fail because append condition matches all events.
        with self.assertRaises(IntegrityError):
            eventstore.append(
                events=(
                    DCBEvent(type="EventType4", data=b"data4"),
                    DCBEvent(type="EventType5", data=b"data5"),
                ),
                condition=DCBAppendCondition(after=0),
            )

        result, head = eventstore.read()
        self.assertEqual(3, len(result))
        self.assertEqual(3, head)

        # Okay, because append condition after last position.
        position = eventstore.append(
            events=(
                DCBEvent(type="EventType4", data=b"data4"),
                DCBEvent(type="EventType5", data=b"data5"),
            ),
            condition=DCBAppendCondition(after=3),
        )
        self.assertEqual(5, position)

        result, head = eventstore.read()
        self.assertEqual(5, len(result))
        self.assertEqual(5, head)

        # Fail because event types match.
        with self.assertRaises(IntegrityError):
            eventstore.append(
                events=[
                    DCBEvent(type="EventType6", data=b"data6"),
                    DCBEvent(type="EventType7", data=b"data7"),
                ],
                condition=DCBAppendCondition(
                    fail_if_events_match=DCBQuery(
                        # items=[DCBQueryItem(types=["EventType4", "EventType5"])],
                        items=[DCBQueryItem(types=["EventType4"])],
                    ),
                    after=3,
                ),
            )

        # Okay because event types don't match.
        position = eventstore.append(
            events=[
                DCBEvent(type="EventType6", data=b"data6", tags=["tagD"]),
                DCBEvent(type="EventType7", data=b"data7", tags=["tagD"]),
            ],
            condition=DCBAppendCondition(
                fail_if_events_match=DCBQuery(
                    # items=[DCBQueryItem(types=["EventType6", "EventType7"])],
                    items=[DCBQueryItem(types=["EventType6"])],
                ),
                after=3,
            ),
        )
        self.assertEqual(7, position)

        # Fail because tag matches.
        with self.assertRaises(IntegrityError):
            eventstore.append(
                events=[
                    DCBEvent(type="EventType8", data=b"data8"),
                    DCBEvent(type="EventType9", data=b"data9"),
                ],
                condition=DCBAppendCondition(
                    fail_if_events_match=DCBQuery(
                        items=[DCBQueryItem(tags=["tagD"])],
                    ),
                    after=3,
                ),
            )

        # Can query without query items and limit.
        result, head = eventstore.read(limit=2)
        self.assertEqual(2, len(result))
        self.assertEqual(7, head)

        # Can query with query items and limit.
        result, head = eventstore.read(
            DCBQuery(
                items=[DCBQueryItem(tags=["tagA"])],
            ),
            limit=2,
        )
        self.assertEqual(2, len(result))
        self.assertEqual(7, head)

        student_id = f"student1-{uuid4()}"
        student_registered = DCBEvent(
            type="StudentRegistered",
            data=json.dumps({"name": "Student1", "max_courses": 10}).encode(),
            tags=[student_id],
        )
        course_id = f"course1-{uuid4()}"
        course_registered = DCBEvent(
            type="CourseRegistered",
            data=json.dumps({"name": "Course1", "places": 10}).encode(),
            tags=[course_id],
        )
        student_joined_course = DCBEvent(
            type="StudentJoinedCourse",
            data=json.dumps(
                {"student_id": student_id, "course_id": course_id}
            ).encode(),
            tags=[course_id, student_id],
        )

        eventstore.append(
            events=[student_registered],
            condition=DCBAppendCondition(
                fail_if_events_match=DCBQuery(
                    items=[DCBQueryItem(tags=student_registered.tags)],
                ),
                after=3,
            ),
        )
        eventstore.append(
            events=[course_registered],
            condition=DCBAppendCondition(
                fail_if_events_match=DCBQuery(
                    items=[DCBQueryItem(tags=course_registered.tags)],
                ),
                after=3,
            ),
        )
        eventstore.append(
            events=[student_joined_course],
            condition=DCBAppendCondition(
                fail_if_events_match=DCBQuery(
                    items=[DCBQueryItem(tags=student_joined_course.tags)],
                ),
                after=3,
            ),
        )

        result, head = eventstore.read()
        self.assertEqual(10, len(result))
        self.assertEqual(result[-3].event.type, student_registered.type)
        self.assertEqual(result[-2].event.type, course_registered.type)
        self.assertEqual(result[-1].event.type, student_joined_course.type)
        self.assertEqual(result[-3].event.data, student_registered.data)
        self.assertEqual(result[-2].event.data, course_registered.data)
        self.assertEqual(result[-1].event.data, student_joined_course.data)
        self.assertEqual(result[-3].event.tags, student_registered.tags)
        self.assertEqual(result[-2].event.tags, course_registered.tags)
        self.assertEqual(result[-1].event.tags, student_joined_course.tags)
        self.assertEqual(10, head)

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_registered.tags)],
            )
        )
        self.assertEqual(2, len(result))
        self.assertEqual(10, head)

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=course_registered.tags)],
            )
        )
        self.assertEqual(2, len(result))
        self.assertEqual(10, head)

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_joined_course.tags)],
            )
        )
        self.assertEqual(1, len(result))
        self.assertEqual(10, head)

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_registered.tags)],
            ),
            after=2,
        )
        self.assertEqual(2, len(result))
        self.assertEqual(10, head)

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=course_registered.tags)],
            ),
            after=2,
        )
        self.assertEqual(2, len(result))
        self.assertEqual(10, head)

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_joined_course.tags)],
            ),
            after=2,
        )
        self.assertEqual(1, len(result))
        self.assertEqual(10, head)

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_registered.tags)],
            ),
            after=2,
            limit=1,
        )
        self.assertEqual(1, len(result))
        self.assertEqual(10, head)

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=course_registered.tags)],
            ),
            after=2,
            limit=1,
        )
        self.assertEqual(1, len(result))
        self.assertEqual(10, head)

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_joined_course.tags)],
            ),
            after=2,
            limit=1,
        )
        self.assertEqual(1, len(result))
        self.assertEqual(10, head)

        consistency_boundary = DCBQuery(
            items=[
                DCBQueryItem(
                    types=["StudentRegistered", "StudentJoinedCourse"],
                    tags=[student_id],
                ),
                DCBQueryItem(
                    types=["CourseRegistered", "StudentJoinedCourse"],
                    tags=[course_id],
                ),
            ]
        )
        result, head = eventstore.read(
            query=consistency_boundary,
        )
        self.assertEqual(3, len(result))
        self.assertEqual(10, head)


class TestInMemoryDCBEventStore(DCBEventStoreTestCase):
    def test_in_memory_event_store(self) -> None:
        self._test_event_store(InMemoryDCBEventStore())


class WithPostgres(TestCase):
    postgres_dcb_eventstore_class: type[PostgresDCBEventStore]

    def setUp(self) -> None:
        self.datastore = PostgresDatastore(
            dbname="eventsourcing",
            host="127.0.0.1",
            port=5432,
            user="eventsourcing",
            password="eventsourcing",  # noqa:  S106
        )
        self.eventstore = self.postgres_dcb_eventstore_class(self.datastore)
        self.eventstore.create_table()

    def tearDown(self) -> None:
        self.datastore.close()
        # Drop tables.
        drop_tables()


class TestPostgresDCBEventStoreTS(DCBEventStoreTestCase, WithPostgres):
    postgres_dcb_eventstore_class = PostgresDCBEventStoreTS

    def test_postgres_event_store(self) -> None:
        self._test_event_store(self.eventstore)

    def test_pg_type_dcb_event(self) -> None:
        # Check "dcb_event" type.
        event = cast(
            PostgresDCBEventStoreTS, self.eventstore
        ).construct_pg_dcb_event(
            type="EventType1",
            data=b"data",
            tags=["tag1", "tag2"],
        )
        self.assertEqual("EventType1", event.type)
        self.assertEqual(b"data", event.data)
        self.assertEqual(["tag1", "tag2"], event.tags)

        with self.datastore.get_connection() as conn:
            result = conn.execute(
                (
                    "SELECT pg_typeof(%(dcb_event)s), "
                    "(%(dcb_event)s).type, "
                    "(%(dcb_event)s).data, "
                    "(%(dcb_event)s).tags"
                ),
                {"dcb_event": event},
            ).fetchone()

        assert result is not None
        self.assertEqual("dcb_event", result["pg_typeof"])
        self.assertEqual("EventType1", result["type"])
        self.assertEqual(b"data", result["data"])
        self.assertEqual(["tag1", "tag2"], result["tags"])

        with (
            self.assertRaises(ProgrammingError) as cm,
            self.datastore.get_connection() as conn,
        ):
            conn.execute(
                (
                    "SELECT pg_typeof(%(dcb_event)s), "
                    "(%(dcb_event)s).typeyyyyyyyyyyy, "
                    "(%(dcb_event)s).data, "
                    "(%(dcb_event)s).tags"
                ),
                {"dcb_event": event},
            ).fetchone()

        self.assertIn(
            'column "typeyyyyyyyyyyy" not found in data type dcb_event',
            str(cm.exception),
        )


class TestPostgresDCBEventStoreTT(DCBEventStoreTestCase, WithPostgres):
    postgres_dcb_eventstore_class = PostgresDCBEventStoreTT

    def test_postgres_event_store(self) -> None:
        self._test_event_store(self.eventstore)


class TestDCBPostgresFactory(TestCase):
    def test(self) -> None:
        # For now, just cover the case of not creating a table.
        factory = PostgresTSDCBFactory(
            Environment(
                name="test",
                env={
                    "POSTGRES_DBNAME": "eventsourcing",
                    "POSTGRES_HOST": "localhost",
                    "POSTGRES_PORT": "5432",
                    "POSTGRES_USER": "eventsourcing",
                    "POSTGRES_PASSWORD": "eventsourcing",
                    "CREATE_TABLE": "f",
                },
            )
        )
        recorder = factory.dcb_event_store()
        self.assertIsInstance(recorder, PostgresRecorder)


class ConcurrentAppendTestCase(TestCase):
    insert_num = 10000

    def _test_commit_vs_insert_order(self, event_store: DCBEventStore) -> None:
        race_started = Event()

        tag1 = str(uuid4())
        tag2 = str(uuid4())

        stack1 = self.create_stack(tag1)
        stack2 = self.create_stack(tag2)

        errors = []

        def append_stack(stack: list[DCBEvent]) -> None:
            try:
                race_started.wait()
                event_store.append(stack)
            except Exception as e:
                errors.append(e)

        thread1 = Thread(target=append_stack, args=(stack1,), daemon=True)
        thread2 = Thread(target=append_stack, args=(stack2,), daemon=True)

        thread1.start()
        thread2.start()

        sleep(0.1)

        race_started.set()

        thread1.join()
        thread2.join()

        if errors:
            raise errors[0]

        # sleep(1)  # Added to make eventsourcing-axon tests work.
        sequenced_events, head = event_store.read()
        positions_for_tag1 = [
            s.position for s in sequenced_events if tag1 in s.event.tags
        ]
        positions_for_tag2 = [
            s.position for s in sequenced_events if tag2 in s.event.tags
        ]
        self.assertEqual(self.insert_num, len(positions_for_tag1))
        self.assertEqual(self.insert_num, len(positions_for_tag2))

        max_position_for_tag1 = max(positions_for_tag1)
        max_position_for_tag2 = max(positions_for_tag2)
        min_position_for_tag1 = min(positions_for_tag1)
        min_position_for_tag2 = min(positions_for_tag2)

        if max_position_for_tag1 > min_position_for_tag2:
            self.assertGreater(min_position_for_tag1, max_position_for_tag2)
        else:
            self.assertGreater(min_position_for_tag2, max_position_for_tag1)

    def _test_fail_condition_is_effective(self, event_store: DCBEventStore) -> None:
        race_started = Event()

        tag1 = str(uuid4())
        tag2 = str(uuid4())

        stack1 = self.create_stack(tag1)
        stack2 = self.create_stack(tag2)

        errors = []

        def append_stack(stack: list[DCBEvent]) -> None:
            try:
                race_started.wait()
                event_store.append(stack, DCBAppendCondition(after=0))
            except Exception as e:
                errors.append(e)

        thread1 = Thread(target=append_stack, args=(stack1,), daemon=True)
        thread2 = Thread(target=append_stack, args=(stack2,), daemon=True)

        thread1.start()
        thread2.start()

        sleep(0.1)

        race_started.set()

        thread1.join()
        thread2.join()

        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], IntegrityError)

        sequenced_events, head = event_store.read()
        positions_for_tag1 = [
            s.position for s in sequenced_events if tag1 in s.event.tags
        ]
        positions_for_tag2 = [
            s.position for s in sequenced_events if tag2 in s.event.tags
        ]

        if len(positions_for_tag1) == self.insert_num:
            self.assertEqual(len(positions_for_tag2), 0)
        elif len(positions_for_tag2) == self.insert_num:
            self.assertEqual(len(positions_for_tag1), 0)
        else:
            self.fail(
                f"Inserted {len(positions_for_tag1)} for tag1 "
                f"and {len(positions_for_tag2)} for tag2"
            )

    def create_stack(self, tag: str) -> list[DCBEvent]:
        return [
            DCBEvent(
                type="CommitOrderTest",
                data=b"",
                tags=[tag],
            )
            for i in range(self.insert_num)
        ]


class TestPostgresDCBEventStoreTSCommitOrderVsInsertOrder(
    ConcurrentAppendTestCase, WithPostgres
):
    postgres_dcb_eventstore_class = PostgresDCBEventStoreTS

    def test_commit_vs_insert_order(self) -> None:
        self._test_commit_vs_insert_order(self.eventstore)

    def test_fail_condition_is_effective(self) -> None:
        self._test_fail_condition_is_effective(self.eventstore)


@pytest.fixture
def eventstore() -> Iterator[DCBEventStore]:
    datastore = PostgresDatastore(
        dbname="eventsourcing",
        host="127.0.0.1",
        port=5432,
        user="eventsourcing",
        password="eventsourcing",  # noqa:  S106
    )
    recorder = PostgresDCBEventStoreTS(datastore)
    recorder.create_table()
    yield recorder

    drop_tables()


@pytest.mark.benchmark(group="dcb-append-one-event")
def test_recorder_append_one_event(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:

    def setup() -> Any:
        events = generate_events(1)
        return (events,), {}

    class Context:
        position: int = 0

    def func(events: list[DCBEvent]) -> None:
        Context.position = eventstore.append(
            events,
            DCBAppendCondition(
                fail_if_events_match=DCBQuery(
                    items=[DCBQueryItem(tags=events[0].tags)]
                ),
                after=Context.position,
            ),
        )

    benchmark.pedantic(func, setup=setup, rounds=500)


@pytest.mark.benchmark(group="dcb-append-ten-events")
def test_recorder_append_ten_events(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:

    def setup() -> Any:
        events = generate_events(10)
        return (events,), {}

    class Context:
        position: int = 0

    def func(events: list[DCBEvent]) -> None:
        Context.position = eventstore.append(
            events,
            DCBAppendCondition(
                fail_if_events_match=DCBQuery(
                    items=[DCBQueryItem(tags=events[0].tags)]
                ),
                after=Context.position,
            ),
        )

    benchmark.pedantic(func, setup=setup, rounds=500)


@pytest.mark.benchmark(group="dcb-read-events-no-query-limit-ten")
def test_recorder_read_events_no_query_limit_ten(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:
    events = generate_events(50000)
    eventstore.append(events)

    def func() -> None:
        results = eventstore.read(limit=10)
        assert len(results) == 10

    benchmark(func)


@pytest.mark.benchmark(group="dcb-read-events-no-query-after-thousand-limit-ten")
def test_recorder_read_events_no_query_after_thousand_limit_ten(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:
    events = generate_events(50000)
    eventstore.append(events)

    def func() -> None:
        results = eventstore.read(after=1000, limit=10)
        assert len(results) == 10

    benchmark(func)


@pytest.mark.benchmark(group="dcb-read-events-one-query-one-type")
def test_recorder_read_events_one_query_one_type(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:
    events = generate_events(50000)
    eventstore.append(events)

    query = DCBQuery(items=[DCBQueryItem(types=[events[-1].type])])

    def func() -> None:
        results = eventstore.read(query)
        assert len(results) == 1

    benchmark(func)


@pytest.mark.benchmark(group="dcb-read-events-two-queries-one-type")
def test_recorder_read_events_two_queries_one_type(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:
    events = generate_events(50000)
    eventstore.append(events)

    query = DCBQuery(
        items=[
            DCBQueryItem(types=[events[0].type]),
            DCBQueryItem(types=[events[-1].type]),
        ],
    )

    def func() -> None:
        results = eventstore.read(query)
        assert len(results) == 2

    benchmark(func)


@pytest.mark.benchmark(group="dcb-read-events-one-query-two-types")
def test_recorder_read_events_one_query_two_types(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:
    events = generate_events(50000)
    eventstore.append(events)

    query = DCBQuery(
        items=[
            DCBQueryItem(
                types=[
                    events[0].type,
                    events[-1].type,
                ]
            )
        ]
    )

    def func() -> None:
        results = eventstore.read(query)
        assert len(results) == 2

    benchmark(func)


@pytest.mark.benchmark(group="dcb-read-events-one-query-one-tag")
def test_recorder_read_events_one_query_one_tag(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:
    events = generate_events(50000)
    eventstore.append(events)
    query = DCBQuery(items=[DCBQueryItem(tags=events[-1].tags)])

    def func() -> None:
        results = eventstore.read(query)
        assert len(results) == 1

    benchmark(func)


@pytest.mark.benchmark(group="dcb-read-events-two-queries-one-tag")
def test_recorder_read_events_two_queries_one_tag(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:
    events = generate_events(50000)
    eventstore.append(events)
    query = DCBQuery(
        items=[
            DCBQueryItem(tags=events[0].tags),
            DCBQueryItem(tags=events[-1].tags),
        ]
    )

    def func() -> None:
        results = eventstore.read(query)
        assert len(results) == 2

    benchmark(func)


@pytest.mark.benchmark(group="dcb-read-events-one-query-two-tags")
def test_recorder_read_events_one_query_two_tags(
    eventstore: DCBEventStore, benchmark: BenchmarkFixture
) -> None:
    events = generate_events(50000)
    eventstore.append(events)
    query = DCBQuery(
        items=[
            DCBQueryItem(tags=events[0].tags + events[-1].tags),
        ]
    )

    def func() -> None:
        results = eventstore.read(query)
        assert len(results) == 0

    benchmark(func)


# class MySetupForRunningExplainAnalyzeInPsql(TestCase):
#     def setUp(self) -> None:
#         datastore = PostgresDatastore(
#             dbname="eventsourcing",
#             host="127.0.0.1",
#             port=5432,
#             user="eventsourcing",
#             password="eventsourcing",  # no qa:  S106
#             after_connect=PostgresDCBEventStoreTS.register_pg_composite_type_adapters,
#         )
#         self.eventstore = PostgresDCBEventStoreTS(datastore)
#         self.eventstore.create_table()
#
#     def test(self):
#         events = generate_events(500000)
#         self.eventstore.append(events)
#         self.eventstore.read()
#
#
#     def tearDown(self) -> None:
#         drop_tables()


def generate_events(num_events: int) -> list[DCBEvent]:
    return [
        DCBEvent(
            type=f"topic{i}",
            data=b"state{i}",
            tags=[str(uuid4())],
        )
        for i in range(num_events)
    ]


useful_for_listing_functions_and_procedures = """
select n.nspname as schema_name,
       p.proname as specific_name,
       case p.prokind
            when 'f' then 'FUNCTION'
            when 'p' then 'PROCEDURE'
            when 'a' then 'AGGREGATE'
            when 'w' then 'WINDOW'
            end as kind,
       l.lanname as language,
       case when l.lanname = 'internal' then p.prosrc
            else pg_get_functiondef(p.oid)
            end as definition,
       pg_get_function_arguments(p.oid) as arguments,
       t.typname as return_type
from pg_proc p
left join pg_namespace n on p.pronamespace = n.oid
left join pg_language l on p.prolang = l.oid
left join pg_type t on t.oid = p.prorettype
where n.nspname not in ('pg_catalog', 'information_schema')
order by schema_name,
         specific_name;
"""
