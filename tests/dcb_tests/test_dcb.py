from __future__ import annotations

from unittest import TestCase

from psycopg.sql import SQL, Identifier

from eventsourcing.persistence import IntegrityError, ProgrammingError
from eventsourcing.postgres import PostgresDatastore
from eventsourcing.tests.postgres_utils import drop_tables
from tests.dcb_tests.dcb import (
    DCBAppendCondition,
    DCBEvent,
    DCBEventStore,
    DCBQuery,
    DCBQueryItem,
    DCBSequencedEvent,
    InMemoryDCBEventStore,
    PostgresDCBEventStore,
)

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

    def _test(self, eventstore: DCBEventStore) -> None:
        # Query for all with zero rows, expect no results.
        result = eventstore.get(DCBQuery())
        self.assertEqual(0, len(list(result)))

        # Must atomically persist one or many events.
        position = eventstore.append(
            events=(DCBEvent(type="EventType1", data=b"data1"),),
        )
        self.assertEqual(1, position)

        # Query for all with one row, expect one result.
        result = eventstore.get(DCBQuery())
        self.assertEqual(1, len(result))

        # Append more than one...
        position = eventstore.append(
            events=[
                DCBEvent(type="EventType2", data=b"data2", tags=["tagA", "tagB"]),
                DCBEvent(type="EventType3", data=b"data3", tags=["tagA", "tagC"]),
            ],
        )
        self.assertEqual(3, position)
        result = eventstore.get(DCBQuery())
        self.assertEqual(3, len(result))

        # Query for all with three rows, expect three results.
        result = eventstore.get(DCBQuery())
        self.assertEqual(3, len(result))

        # Can query for type "EventType1".
        result = eventstore.get(DCBQuery(items=[DCBQueryItem(types=["EventType1"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(1, result[0].position)
        self.assertEqual("EventType1", result[0].event.type)
        self.assertEqual(b"data1", result[0].event.data)
        self.assertEqual([], result[0].event.tags)

        # Can query for type "EventType2".
        result = eventstore.get(DCBQuery(items=[DCBQueryItem(types=["EventType2"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual(b"data2", result[0].event.data)
        self.assertEqual(["tagA", "tagB"], result[0].event.tags)

        # Can query for type "EventType1" after position 1 - no events.
        result = eventstore.get(
            DCBQuery(items=[DCBQueryItem(types=["EventType1"])]), after=1
        )
        self.assertEqual(0, len(result))

        # Can query for type "EventType2" after position 1 - one event.
        result = eventstore.get(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"])]), after=1
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tag "tagA" - two events with "tagA".
        result = eventstore.get(DCBQuery(items=[DCBQueryItem(tags=["tagA"])]))
        self.assertEqual(2, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual(3, result[1].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual("EventType3", result[1].event.type)

        # Can query for tag "tagB" - one event with "tagB".
        result = eventstore.get(DCBQuery(items=[DCBQueryItem(tags=["tagB"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tag "tagC" - one event with "tagC".
        result = eventstore.get(DCBQuery(items=[DCBQueryItem(tags=["tagC"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(3, result[0].position)
        self.assertEqual("EventType3", result[0].event.type)

        # Can query for tags "tagA" and tagB" - one event has both.
        result = eventstore.get(DCBQuery(items=[DCBQueryItem(tags=["tagA", "tagB"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tags "tagB" and tagC" - no events have both.
        result = eventstore.get(DCBQuery(items=[DCBQueryItem(tags=["tagB", "tagC"])]))
        self.assertEqual(0, len(result))

        # Can query for tags "tagB" or tagC" - two events have one or the other.
        result = eventstore.get(
            DCBQuery(items=(DCBQueryItem(tags=["tagB"]), DCBQueryItem(tags=["tagC"])))
        )
        self.assertEqual(2, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual(3, result[1].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual("EventType3", result[1].event.type)

        # Can query for tags "tagB" or tagD" - only one event.
        result = eventstore.get(
            DCBQuery(items=[DCBQueryItem(tags=["tagB"]), DCBQueryItem(tags=["tagD"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tag "tagA" after position 2 - only one event.
        result = eventstore.get(
            DCBQuery(items=[DCBQueryItem(tags=["tagA"])]),
            after=2,
        )
        self.assertEqual(1, len(result))
        self.assertEqual(3, result[0].position)
        self.assertEqual("EventType3", result[0].event.type)

        # Can query for type "EventType1" and tag "tagA" - zero events.
        result = eventstore.get(
            DCBQuery(items=[DCBQueryItem(types=["EventType1"], tags=["tagA"])])
        )
        self.assertEqual(0, len(result))

        # Can query for type "EventType2" and tag "tagA" - only one event.
        result = eventstore.get(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"], tags=["tagA"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for type "EventType2" and tag "tagA" after position 2 - no events.
        result = eventstore.get(
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
                condition=DCBAppendCondition(
                    fail_if_events_match=DCBQuery(),
                    after=None,
                ),
            )

        result = eventstore.get(DCBQuery())
        self.assertEqual(3, len(result))

        # Okay, because append condition after last position.
        position = eventstore.append(
            events=(
                DCBEvent(type="EventType4", data=b"data4"),
                DCBEvent(type="EventType5", data=b"data5"),
            ),
            condition=DCBAppendCondition(
                fail_if_events_match=DCBQuery(),
                after=3,
            ),
        )
        self.assertEqual(5, position)

        result = eventstore.get(DCBQuery())
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
        result = eventstore.get(DCBQuery(), limit=2)
        self.assertEqual(2, len(result))

        # Can query with query items and limit.
        result = eventstore.get(
            DCBQuery(
                items=[DCBQueryItem(tags=["tagA"])],
            ),
            limit=2,
        )
        self.assertEqual(2, len(result))


class TestInMemoryDCBEventStore(DCBEventStoreTestCase):
    def test_in_memory(self) -> None:
        self._test(InMemoryDCBEventStore())


class TestPostgresDCBEventStore(DCBEventStoreTestCase):
    def setUp(self) -> None:
        self.datastore = PostgresDatastore(
            dbname="eventsourcing",
            host="127.0.0.1",
            port=5432,
            user="eventsourcing",
            password="eventsourcing",  # noqa:  S106
            after_connect=PostgresDCBEventStore.register_type_adapters,
        )
        self.eventstore = PostgresDCBEventStore(self.datastore)
        self.eventstore.create_table()

    def tearDown(self) -> None:
        # Drop functions and types.
        with self.datastore.get_connection() as conn:
            conn.execute(
                SQL("DROP PROCEDURE IF EXISTS {0}").format(
                    Identifier(self.eventstore.dcb_append_events_procedure_name)
                )
            )
            conn.execute(
                SQL("DROP FUNCTION IF EXISTS {0}").format(
                    Identifier(self.eventstore.dcb_insert_events_function_name)
                )
            )
            conn.execute(
                SQL("DROP FUNCTION IF EXISTS {0}").format(
                    Identifier(self.eventstore.dcb_select_events_function_name)
                )
            )
            conn.execute(
                SQL("DROP TYPE IF EXISTS {0} CASCADE").format(
                    Identifier(self.eventstore.dcb_append_condition_type_name)
                )
            )
            conn.execute(
                SQL("DROP TYPE IF EXISTS {0} CASCADE").format(
                    Identifier(self.eventstore.dcb_query_item_type_name)
                )
            )
            conn.execute(
                SQL("DROP TYPE IF EXISTS {0} CASCADE").format(
                    Identifier(self.eventstore.dcb_event_type_name)
                )
            )

        # Drop tables.
        drop_tables()

    def test_postgres(self) -> None:
        self._test(self.eventstore)

    def test_pg_composite_types(self) -> None:
        # Check "dcb_event_type" type.
        dcb_event = self.eventstore.pg_dcb_event(
            type="EventType1",
            data=b"data",
            tags=["tag1", "tag2"],
        )
        self.assertEqual("EventType1", dcb_event.type)
        self.assertEqual(b"data", dcb_event.data)
        self.assertEqual(["tag1", "tag2"], dcb_event.tags)

        with self.datastore.get_connection() as conn:
            result = conn.execute(
                (
                    "SELECT pg_typeof(%(dcb_event)s), "
                    "(%(dcb_event)s).type, "
                    "(%(dcb_event)s).data, "
                    "(%(dcb_event)s).tags"
                ),
                {"dcb_event": dcb_event},
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
            result = conn.execute(
                (
                    "SELECT pg_typeof(%(dcb_event)s), "
                    "(%(dcb_event)s).typeyyyyyyyyyyy, "
                    "(%(dcb_event)s).data, "
                    "(%(dcb_event)s).tags"
                ),
                {"dcb_event": dcb_event},
            ).fetchone()

        self.assertIn(
            'column "typeyyyyyyyyyyy" not found in data type dcb_event',
            str(cm.exception),
        )

        # Check "dcb_query_item" type.
        dcb_query_item = self.eventstore.pg_query_item(
            types=["EventType1", "EventType2"],
            tags=["tag1", "tag2"],
        )
        self.assertEqual(["EventType1", "EventType2"], dcb_query_item.types)
        self.assertEqual(["tag1", "tag2"], dcb_query_item.tags)

        with self.datastore.get_connection() as conn:
            result = conn.execute(
                "SELECT pg_typeof(%(query_item)s), "
                "(%(query_item)s).types, "
                "(%(query_item)s).tags",
                {"query_item": dcb_query_item},
            ).fetchone()
        assert result is not None
        self.assertEqual("dcb_query_item", result["pg_typeof"])
        self.assertEqual(["EventType1", "EventType2"], result["types"])
        self.assertEqual(["tag1", "tag2"], result["tags"])

        with (
            self.assertRaises(ProgrammingError) as cm,
            self.datastore.get_connection() as conn,
        ):
            result = conn.execute(
                "SELECT pg_typeof(%(query_item)s), "
                "(%(query_item)s).typeyyyyyyyyyys, "
                "(%(query_item)s).tags",
                {"query_item": dcb_query_item},
            ).fetchone()

        self.assertIn(
            'column "typeyyyyyyyyyys" not found in data type dcb_query_item',
            str(cm.exception),
        )

        # Check "dcb_append_condition" type.
        dcb_append_condition = self.eventstore.pg_append_condition(
            query_items=[dcb_query_item],
            after=12,
        )
        self.assertEqual(dcb_query_item, dcb_append_condition.query_items[0])
        self.assertEqual(12, dcb_append_condition.after)

        with self.datastore.get_connection() as conn:
            result = conn.execute(
                (
                    "SELECT pg_typeof(%(dcb_append_condition)s), "
                    "(%(dcb_append_condition)s).query_items, "
                    "(%(dcb_append_condition)s).after"
                ),
                {"dcb_append_condition": dcb_append_condition},
            ).fetchone()
        assert result is not None
        self.assertEqual("dcb_append_condition", result["pg_typeof"])
        self.assertEqual([dcb_query_item], result["query_items"])
        self.assertEqual(12, result["after"])

        with (
            self.assertRaises(ProgrammingError) as cm,
            self.datastore.get_connection() as conn,
        ):
            conn.execute(
                "SELECT pg_typeof(%(append_condition)s), "
                "(%(append_condition)s).query_itemsyyy, "
                "(%(append_condition)s).after",
                {"append_condition": dcb_append_condition},
            ).fetchone()

        self.assertIn(
            'column "query_itemsyyy" not found in data type dcb_append_condition',
            str(cm.exception),
        )

    def test_pg_functions(self) -> None:
        dcb_event1 = self.eventstore.pg_dcb_event(
            type="EventTypeA",
            data=b"dataA",
            tags=["tagA", "tagB"],
        )
        dcb_event2 = self.eventstore.pg_dcb_event(
            type="EventTypeB",
            data=b"dataB",
            tags=["tagC", "tagD"],
        )

        with self.datastore.get_connection() as conn:
            results = conn.execute(
                SQL("SELECT * FROM dcb_insert_events((%s))"),
                ([dcb_event1, dcb_event2] * 10,),
            ).fetchall()
            assert results is not None
            self.assertEqual(20, len(results))
            self.assertEqual(1, results[0]["posn"])
            self.assertEqual(20, results[19]["posn"])

        # Limit 5.
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s), (%s))"),
                    ([], 0, 5),
                ).fetchall()
            )
            self.assertEqual(5, len(results))

        # Limit 10.
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s), (%s))"),
                    ([], 0, 10),
                ).fetchall()
            )
            self.assertEqual(10, len(results))

        # Default limit (=> NULL / unlimited).
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s))"),
                    ([], 0),
                ).fetchall()
            )
            self.assertEqual(20, len(results))

        # After 5. Default limit (=> NULL / unlimited).
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s))"),
                    ([], 5),
                ).fetchall()
            )
            self.assertEqual(15, len(results))
            self.assertEqual(6, results[0]["posn"])
            self.assertEqual(20, results[14]["posn"])

        # Select with event types - match, should be in asc order.
        dcb_query_item = self.eventstore.pg_query_item(
            types=["EventTypeA", "EventTypeB"],
            tags=[],
        )
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s), (%s))"),
                    ([dcb_query_item], 0, None),
                ).fetchall()
            )
            self.assertEqual(20, len(results))
            self.assertEqual(1, results[0]["posn"])
            self.assertEqual(2, results[1]["posn"])
            self.assertEqual(19, results[18]["posn"])
            self.assertEqual(20, results[19]["posn"])

        # Select with query items and limit.
        dcb_query_item = self.eventstore.pg_query_item(
            types=["EventTypeA", "EventTypeB"],
            tags=[],
        )
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s), (%s))"),
                    ([dcb_query_item], 0, 5),
                ).fetchall()
            )
            self.assertEqual(5, len(results))

        # Select with tags - match, should be in asc order.
        dcb_query_items = [
            self.eventstore.pg_query_item(
                types=[],
                tags=["tagA", "tagB"],
            ),
            self.eventstore.pg_query_item(
                types=[],
                tags=["tagC", "tagD"],
            ),
        ]
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s), (%s))"),
                    (dcb_query_items, 0, None),
                ).fetchall()
            )
            self.assertEqual(20, len(results))
            self.assertEqual(1, results[0]["posn"])
            self.assertEqual(["tagA", "tagB"], results[0]["tags"])
            self.assertEqual(2, results[1]["posn"])
            self.assertEqual(["tagC", "tagD"], results[1]["tags"])
            self.assertEqual(19, results[18]["posn"])
            self.assertEqual(["tagA", "tagB"], results[18]["tags"])
            self.assertEqual(20, results[19]["posn"])
            self.assertEqual(["tagC", "tagD"], results[19]["tags"])

        # Select with query item after position 10 - match.
        dcb_query_item = self.eventstore.pg_query_item(
            types=["EventTypeA", "EventTypeB"],
            tags=[],
            # tags=["tag1", "tag2"],
        )
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s), (%s))"),
                    ([dcb_query_item], 10, 10),
                ).fetchall()
            )
            self.assertEqual(10, len(results))
            self.assertEqual(11, results[0]["posn"])

        # Select with query item - no match, wrong types.
        dcb_query_item = self.eventstore.pg_query_item(
            types=["EventType1", "EventType2"],
            tags=["tag1", "tag2"],
        )
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s), (%s))"),
                    ([dcb_query_item], 0, 10),
                ).fetchall()
            )
            self.assertEqual(0, len(results))

        # Fast fail option.
        dcb_query_item = self.eventstore.pg_query_item(
            types=["EventType1", "EventType2"],
            tags=["tag1", "tag2"],
        )
        with self.datastore.get_connection() as conn:
            results = list(
                conn.execute(
                    SQL("SELECT * FROM dcb_select_events((%s), (%s), (%s), (%s))"),
                    ([dcb_query_item], 0, 1, True),
                ).fetchall()
            )
            self.assertEqual(0, len(results))

    def test_append_events_procedure(self) -> None:

        # Insert zero events.
        with self.datastore.get_connection() as conn:
            result = conn.execute(
                SQL("CALL dcb_append_events((%s), (%s), (%s))"), ([], [], 0)
            )
            returned_value = result.fetchall()[-1]["after"]

        # Returned value should be NULL (=> None / 0 records).
        self.assertIsNone(returned_value)

        # Insert one event with "select all" condition.
        dcb_event1 = self.eventstore.pg_dcb_event(
            type="EventTypeA",
            data=b"dataA",
            tags=["tagA", "tagB"],
        )
        with self.datastore.get_connection() as conn:
            result = conn.execute(
                SQL("CALL dcb_append_events((%s), (%s), (%s))"),
                (
                    [dcb_event1],
                    [],
                    0,
                ),
            )

            returned_value = result.fetchall()[-1]["after"]

        # Returned value should be 1 (=> position of last inserted event).
        self.assertEqual(1, returned_value)

        # Try to insert another event with same "select all" fail condition.
        dcb_event1 = self.eventstore.pg_dcb_event(
            type="EventTypeA",
            data=b"dataA",
            tags=["tagA", "tagB"],
        )
        with self.datastore.get_connection() as conn:
            result = conn.execute(
                SQL("CALL dcb_append_events((%s), (%s), (%s))"),
                (
                    [dcb_event1],
                    [],
                    0,
                ),
            )
            returned_value = result.fetchall()[-1]["after"]

        # Returned value should be -1 (=> FAIL / IntegrityError).
        self.assertEqual(-1, returned_value)

        # Try again with no fail condition.
        dcb_event1 = self.eventstore.pg_dcb_event(
            type="EventTypeA",
            data=b"dataA",
            tags=["tagA", "tagB"],
        )

        with self.datastore.get_connection() as conn:
            result = conn.execute(
                SQL("CALL dcb_append_events((%s), (%s), (%s))"),
                (
                    [dcb_event1],
                    [],
                    -1,
                ),
            )
            returned_value = result.fetchall()[-1]["after"]

        # Returned value should be 2 (=> position of last inserted event).
        self.assertEqual(2, returned_value)
        #
        #     # dcb_event1 = self.eventstore.pg_dcb_event(
        #     #     type="EventTypeA",
        #     #     data=b"dataA",
        #     #     tags=["tagA", "tagB"],
        #     # )
        #     #
        #     # result = conn.execute(SQL(
        #     #     "CALL dcb_append_events((%s), (%s), (%s))"
        #     # ), ([dcb_event1], [], 10,))
        #     # self.assertEqual(2, result.fetchall()[-1]["after"])
        #


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
