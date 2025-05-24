from __future__ import annotations

import json
from threading import Event, Thread
from time import sleep
from typing import TYPE_CHECKING, Any
from unittest import TestCase
from uuid import uuid4

import pytest
from psycopg.sql import SQL, Identifier

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
from examples.dcb.postgres import (
    DCBPostgresFactory,
    PgDCBEvent,
    PostgresDCBEventStore,
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

        # Must atomically persist one or many events.
        position = eventstore.append(
            events=(DCBEvent(type="EventType1", data=b"data1"),),
        )
        self.assertEqual(1, position)

        # Query for all with one row, expect one result.
        result, head = eventstore.read()
        self.assertEqual(1, len(result))

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

        # Query for all with three rows, expect three results.
        result, head = eventstore.read()
        self.assertEqual(3, len(result))

        # Can query for type "EventType1".
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType1"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(1, result[0].position)
        self.assertEqual("EventType1", result[0].event.type)
        self.assertEqual(b"data1", result[0].event.data)
        self.assertEqual([], result[0].event.tags)

        # Can query for type "EventType2".
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual(b"data2", result[0].event.data)
        self.assertEqual(["tagA", "tagB"], result[0].event.tags)

        # Can query for type "EventType1" after position 1 - no events.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType1"])]), after=1
        )
        self.assertEqual(0, len(result))

        # Can query for type "EventType2" after position 1 - one event.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"])]), after=1
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tag "tagA" - two events with "tagA".
        result, head = eventstore.read(DCBQuery(items=[DCBQueryItem(tags=["tagA"])]))
        self.assertEqual(2, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual(3, result[1].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual("EventType3", result[1].event.type)

        # Can query for tag "tagB" - one event with "tagB".
        result, head = eventstore.read(DCBQuery(items=[DCBQueryItem(tags=["tagB"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tag "tagC" - one event with "tagC".
        result, head = eventstore.read(DCBQuery(items=[DCBQueryItem(tags=["tagC"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(3, result[0].position)
        self.assertEqual("EventType3", result[0].event.type)

        # Can query for tags "tagA" and tagB" - one event has both.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(tags=["tagA", "tagB"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tags "tagB" and tagC" - no events have both.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(tags=["tagB", "tagC"])])
        )
        self.assertEqual(0, len(result))

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

        # Can query for tags "tagB" or tagD" - only one event.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(tags=["tagB"]), DCBQueryItem(tags=["tagD"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tag "tagA" after position 2 - only one event.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(tags=["tagA"])]),
            after=2,
        )
        self.assertEqual(1, len(result))
        self.assertEqual(3, result[0].position)
        self.assertEqual("EventType3", result[0].event.type)

        # Can query for type "EventType1" and tag "tagA" - zero events.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType1"], tags=["tagA"])])
        )
        self.assertEqual(0, len(result))

        # Can query for type "EventType2" and tag "tagA" - only one event.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"], tags=["tagA"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for type "EventType2" and tag "tagA" after position 2 - no events.
        result, head = eventstore.read(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"], tags=["tagA"])]),
            after=2,
        )
        self.assertEqual(0, len(result))

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

        # Fail because event types match.
        with self.assertRaises(IntegrityError):
            eventstore.append(
                events=[
                    DCBEvent(type="EventType6", data=b"data6"),
                    DCBEvent(type="EventType7", data=b"data7"),
                ],
                condition=DCBAppendCondition(
                    fail_if_events_match=DCBQuery(
                        items=[DCBQueryItem(types=["EventType4", "EventType5"])],
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
                    items=[DCBQueryItem(types=["EventType6", "EventType7"])],
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

        # Can query with query items and limit.
        result, head = eventstore.read(
            DCBQuery(
                items=[DCBQueryItem(tags=["tagA"])],
            ),
            limit=2,
        )
        self.assertEqual(2, len(result))

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

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_registered.tags)],
            )
        )
        self.assertEqual(2, len(result))

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=course_registered.tags)],
            )
        )
        self.assertEqual(2, len(result))

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_joined_course.tags)],
            )
        )
        self.assertEqual(1, len(result))

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_registered.tags)],
            ),
            after=2,
        )
        self.assertEqual(2, len(result))

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=course_registered.tags)],
            ),
            after=2,
        )
        self.assertEqual(2, len(result))

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_joined_course.tags)],
            ),
            after=2,
        )
        self.assertEqual(1, len(result))

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_registered.tags)],
            ),
            after=2,
            limit=1,
        )
        self.assertEqual(1, len(result))

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=course_registered.tags)],
            ),
            after=2,
            limit=1,
        )
        self.assertEqual(1, len(result))

        result, head = eventstore.read(
            query=DCBQuery(
                items=[DCBQueryItem(tags=student_joined_course.tags)],
            ),
            after=2,
            limit=1,
        )
        self.assertEqual(1, len(result))

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


class TestInMemoryDCBEventStore(DCBEventStoreTestCase):
    def test_in_memory_event_store(self) -> None:
        self._test_event_store(InMemoryDCBEventStore())


class WithPostgres(TestCase):
    def setUp(self) -> None:
        self.datastore = PostgresDatastore(
            dbname="eventsourcing",
            host="127.0.0.1",
            port=5432,
            user="eventsourcing",
            password="eventsourcing",  # noqa:  S106
        )
        self.eventstore = PostgresDCBEventStore(self.datastore)
        self.eventstore.create_table()

    def tearDown(self) -> None:
        # Drop tables.
        drop_tables()


class TestPostgresDCBEventStore(DCBEventStoreTestCase, WithPostgres):
    def test_postgres_event_store(self) -> None:
        self._test_event_store(self.eventstore)

    def test_pg_composite_types(self) -> None:
        # Check "dcb_event_type" type.
        event = self.eventstore.construct_pg_dcb_event(
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

    def test_pg_functions(self) -> None:
        event1 = self.eventstore.construct_pg_dcb_event(
            type="EventTypeA",
            data=b"dataA",
            tags=["tagA", "tagB"],
        )
        event2 = self.eventstore.construct_pg_dcb_event(
            type="EventTypeB",
            data=b"dataB",
            tags=["tagC", "tagD"],
        )

        # Insert 20 events.
        events = [event1, event2] * 10
        positions = self.invoke_pg_insert_events_function(events)
        self.assertEqual(20, len(positions))
        self.assertEqual(1, positions[0])
        self.assertEqual(20, positions[19])

        # Limit 5.
        rows = list(self.eventstore.invoke_pg_function_select_events("", 0, 5))
        self.assertEqual(5, len(rows))

        # Limit 10.
        rows = list(self.eventstore.invoke_pg_function_select_events("", 0, 10))
        self.assertEqual(10, len(rows))

        # Default limit (=> NULL / unlimited).
        rows = list(self.eventstore.invoke_pg_function_select_events("", 0))
        self.assertEqual(20, len(rows))

        # After 5. Default limit (=> NULL / unlimited).
        rows = list(self.eventstore.invoke_pg_function_select_events("", 5))
        self.assertEqual(15, len(rows))
        self.assertEqual(6, rows[0]["sequence_position"])
        self.assertEqual(20, rows[14]["sequence_position"])

        # Select with one event type, expect rows, should be in asc order.
        text_query = self.eventstore.construct_text_query(
            [DCBQueryItem(types=["EventTypeA"])]
        )
        rows = list(self.eventstore.invoke_pg_function_select_events(text_query, 0))
        self.assertEqual(10, len(rows))
        self.assertEqual(1, rows[0]["sequence_position"])
        self.assertEqual(3, rows[1]["sequence_position"])
        self.assertEqual(19, rows[9]["sequence_position"])

        # Select with event types, expect rows, should be in asc order.
        text_query = self.eventstore.construct_text_query(
            [DCBQueryItem(types=["EventTypeA", "EventTypeB"])],
        )
        rows = list(self.eventstore.invoke_pg_function_select_events(text_query, 0))
        self.assertEqual(20, len(rows))
        self.assertEqual(1, rows[0]["sequence_position"])
        self.assertEqual(2, rows[1]["sequence_position"])
        self.assertEqual(19, rows[18]["sequence_position"])
        self.assertEqual(20, rows[19]["sequence_position"])

        # Select with query items and limit.
        rows = list(self.eventstore.invoke_pg_function_select_events(text_query, 0, 5))
        self.assertEqual(5, len(rows))

        # Select with tags.
        text_query = self.eventstore.construct_text_query(
            [DCBQueryItem(tags=["tagA", "tagB"])],
        )
        rows = list(self.eventstore.invoke_pg_function_select_events(text_query, 0))
        self.assertEqual(10, len(rows))

        # Select with tags - match, should be in asc order.
        text_query = self.eventstore.construct_text_query(
            [DCBQueryItem(tags=["tagA", "tagB"]), DCBQueryItem(tags=["tagC", "tagD"])],
        )
        rows = list(self.eventstore.invoke_pg_function_select_events(text_query, 0))
        self.assertEqual(20, len(rows))
        self.assertEqual(1, rows[0]["sequence_position"])
        self.assertEqual(["tagA", "tagB"], rows[0]["tags"])
        self.assertEqual(2, rows[1]["sequence_position"])
        self.assertEqual(["tagC", "tagD"], rows[1]["tags"])
        self.assertEqual(19, rows[18]["sequence_position"])
        self.assertEqual(["tagA", "tagB"], rows[18]["tags"])
        self.assertEqual(20, rows[19]["sequence_position"])
        self.assertEqual(["tagC", "tagD"], rows[19]["tags"])

        # Select with query item after position 10 - match.
        rows = list(self.eventstore.invoke_pg_function_select_events(text_query, 10))
        self.assertEqual(10, len(rows))
        self.assertEqual(11, rows[0]["sequence_position"])

        # Select with query item - no match, wrong types.
        text_query = self.eventstore.construct_text_query(
            [
                DCBQueryItem(
                    types=["EventType1", "EventType2"],
                    tags=["tag1", "tag2"],
                )
            ],
        )

        rows = list(self.eventstore.invoke_pg_function_select_events(text_query, 0))
        self.assertEqual(0, len(rows))

    def invoke_pg_insert_events_function(
        self,
        events: list[PgDCBEvent],
    ) -> list[int]:

        sql_invoke_pg_function_insert_events = SQL(
            "SELECT * FROM {insert_events}((%s))"
        ).format(
            insert_events=Identifier(self.eventstore.pg_function_name_insert_events)
        )

        with self.eventstore.datastore.get_connection() as conn:
            results = conn.execute(
                sql_invoke_pg_function_insert_events,
                (events,),
            ).fetchall()
            assert results is not None
            return [r["sequence_position"] for r in results]

    def test_select_events2(self) -> None:
        a, i = self.eventstore.invoke_pg_function_select_events2(
            "", 1, 1, unsorted=True
        )
        self.assertIsNone(a)
        self.assertIsNone(i)

        event1 = self.eventstore.construct_pg_dcb_event(
            type="EventTypeA",
            data=b"dataA",
            tags=["tagA", "tagB"],
        )
        event2 = self.eventstore.construct_pg_dcb_event(
            type="EventTypeB",
            data=b"dataB",
            tags=["tagC", "tagD"],
        )

        # Insert 20 events.
        events = [event1, event2] * 10
        self.invoke_pg_insert_events_function(events)

        # Select for "propagating state of application" (no query).
        a, i = self.eventstore.invoke_pg_function_select_events2("", after=0, limit=10)
        self.assertEqual(10, len(a))
        self.assertEqual(1, a[0].sequence_position)
        self.assertEqual(event1.type, a[0].type)
        self.assertEqual(event1.data, a[0].data)
        self.assertEqual(event1.tags, a[0].tags)
        self.assertEqual(event2.type, a[1].type)
        self.assertEqual(event2.data, a[1].data)
        self.assertEqual(event2.tags, a[1].tags)
        self.assertEqual(i, 20)

        a, i = self.eventstore.invoke_pg_function_select_events2(
            "", after=a[-1].sequence_position, limit=10
        )
        self.assertEqual(10, len(a))
        self.assertEqual(11, a[0].sequence_position)
        self.assertEqual(event1.type, a[0].type)
        self.assertEqual(event1.data, a[0].data)
        self.assertEqual(event1.tags, a[0].tags)
        self.assertEqual(event2.type, a[1].type)
        self.assertEqual(event2.data, a[1].data)
        self.assertEqual(event2.tags, a[1].tags)
        self.assertEqual(i, 20)

        # Select for decision model with query tags.
        query = self.eventstore.construct_text_query(
            [DCBQueryItem(tags=event1.tags)],
        )
        a, i = self.eventstore.invoke_pg_function_select_events2(query)
        self.assertEqual(10, len(a))
        for e in a:
            self.assertEqual(event1.type, e.type)
            self.assertEqual(event1.tags, e.tags)
            self.assertEqual(event1.data, e.data)

        # Select for decision model with query types.
        query = self.eventstore.construct_text_query(
            [DCBQueryItem(types=[event1.type])],
        )
        a, i = self.eventstore.invoke_pg_function_select_events2(query)
        self.assertEqual(10, len(a))
        for e in a:
            self.assertEqual(event1.type, e.type)
            self.assertEqual(event1.tags, e.tags)
            self.assertEqual(event1.data, e.data)
        self.assertEqual(i, 20)

        # Select for append condition with same query and last position.
        query = self.eventstore.construct_text_query(
            [DCBQueryItem(types=[event1.type])],
        )
        a, i = self.eventstore.invoke_pg_function_select_events2(query, after=i)
        self.assertIsNone(a)
        self.assertEqual(i, 20)

        # Select for append condition after more events written - returns an event.
        self.invoke_pg_insert_events_function([event1, event2])
        query = self.eventstore.construct_text_query(
            [DCBQueryItem(types=[event1.type])],
        )
        a, i = self.eventstore.invoke_pg_function_select_events2(
            query, after=i, limit=1, unsorted=True
        )
        self.assertEqual(1, len(a))
        self.assertEqual(i, 22)

        # Unusual select, with query and after, and not unsorted.
        self.invoke_pg_insert_events_function([event1, event2])
        query = self.eventstore.construct_text_query(
            [DCBQueryItem(types=[event1.type])],
        )
        a, i = self.eventstore.invoke_pg_function_select_events2(
            query, after=1, limit=100
        )
        self.assertEqual(11, len(a))
        self.assertEqual(i, 24)

        # Check we can do more realistic events.
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
        pg_dcb_student_registered = self.eventstore.construct_pg_dcb_event(
            type=student_registered.type,
            data=student_registered.data,
            tags=student_registered.tags,
        )
        pg_dcb_course_registered = self.eventstore.construct_pg_dcb_event(
            type=course_registered.type,
            data=course_registered.data,
            tags=course_registered.tags,
        )
        pg_dcb_student_joined_course = self.eventstore.construct_pg_dcb_event(
            type=student_joined_course.type,
            data=student_joined_course.data,
            tags=student_joined_course.tags,
        )
        self.invoke_pg_insert_events_function([pg_dcb_student_registered])
        query = self.eventstore.construct_text_query(
            [DCBQueryItem(types=[student_registered.type])],
        )
        for _ in range(10):
            a, i = self.eventstore.invoke_pg_function_select_events2(query)
            self.assertEqual(1, len(a))
            self.assertEqual(i, 25)

        self.invoke_pg_insert_events_function([pg_dcb_course_registered])
        query = self.eventstore.construct_text_query(
            [DCBQueryItem(types=[course_registered.type])],
        )
        for _ in range(10):
            a, i = self.eventstore.invoke_pg_function_select_events2(query)
            self.assertEqual(1, len(a))
            self.assertEqual(i, 26)

        self.invoke_pg_insert_events_function([pg_dcb_student_joined_course])
        query = self.eventstore.construct_text_query(
            [DCBQueryItem(types=[student_joined_course.type])],
        )
        for _ in range(10):
            a, i = self.eventstore.invoke_pg_function_select_events2(query)
            self.assertEqual(1, len(a))
            self.assertEqual(i, 27)

    def test_append_events_procedure(self) -> None:

        # Insert zero events.
        after = self.eventstore.invoke_pg_procedure_append_events([], "", 0)
        self.assertIsNone(after)

        # Insert one event with "select all" condition.
        event1 = self.eventstore.construct_pg_dcb_event(
            type="EventTypeA",
            data=b"dataA",
            tags=["tagA", "tagB"],
        )
        after = self.eventstore.invoke_pg_procedure_append_events([event1], "", 0)
        # Returned value should be 1 (=> position of last inserted event).
        self.assertEqual(1, after)

        # Try to insert another event with same "select all" fail condition.
        after = self.eventstore.invoke_pg_procedure_append_events([event1], "", 0)

        # Returned value should be -1 (=> FAIL / IntegrityError).
        self.assertEqual(-1, after)

        # Try again with no fail condition.
        after = self.eventstore.invoke_pg_procedure_append_events([event1], "", -1)

        # Returned value should be 2 (=> position of last inserted event).
        self.assertEqual(2, after)


class TestPostgresFactory(TestCase):
    def test(self) -> None:
        # For now, just cover the case of not creating a table.
        factory = DCBPostgresFactory(
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


class TestPostgresCommitOrderVsInsertOrder(ConcurrentAppendTestCase, WithPostgres):
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
    recorder = PostgresDCBEventStore(datastore)
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
#             after_connect=PostgresDCBEventStore.register_pg_composite_type_adapters,
#         )
#         self.eventstore = PostgresDCBEventStore(datastore)
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
