from __future__ import annotations

from unittest import TestCase

from eventsourcing.persistence import IntegrityError
from tests.dcb_tests.dcb import (
    DCBAppendCondition,
    DCBEvent,
    DCBEventStore,
    DCBQuery,
    DCBQueryItem,
    DCBSequencedEvent,
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


class TestDCBEventStore(TestCase):
    def test_position_sequence(self) -> None:
        db = DCBEventStore()
        position = next(db.position_sequence)
        self.assertEqual(1, position)
        position = next(db.position_sequence)
        self.assertEqual(2, position)
        position = next(db.position_sequence)
        self.assertEqual(3, position)

    def test_append_and_query(self) -> None:
        db = DCBEventStore()
        result = db.get(DCBQuery())
        self.assertEqual(0, len(list(result)))

        # Must atomically persist one or many events.
        position = db.append(
            events=(DCBEvent(type="EventType1", data=b"data1"),),
        )
        self.assertEqual(1, position)

        result = db.get(DCBQuery())
        self.assertEqual(1, len(result))

        # Append more than one...
        position = db.append(
            events=[
                DCBEvent(type="EventType2", data=b"data2", tags=["tagA", "tagB"]),
                DCBEvent(type="EventType3", data=b"data3", tags=["tagA", "tagC"]),
            ],
        )
        self.assertEqual(3, position)
        result = db.get(DCBQuery())
        self.assertEqual(3, len(result))

        # Can query for type "EventType1".
        result = db.get(DCBQuery(items=[DCBQueryItem(types=["EventType1"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(1, result[0].position)
        self.assertEqual("EventType1", result[0].event.type)

        # Can query for type "EventType2".
        result = db.get(DCBQuery(items=[DCBQueryItem(types=["EventType2"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for type "EventType1" after position 1 - no events.
        result = db.get(DCBQuery(items=[DCBQueryItem(types=["EventType1"])]), after=1)
        self.assertEqual(0, len(result))

        # Can query for type "EventType2" after position 1 - one event.
        result = db.get(DCBQuery(items=[DCBQueryItem(types=["EventType2"])]), after=1)
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tag "tagA" - two events with "tagA".
        result = db.get(DCBQuery(items=[DCBQueryItem(tags=["tagA"])]))
        self.assertEqual(2, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual(3, result[1].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual("EventType3", result[1].event.type)

        # Can query for tag "tagB" - one event with "tagB".
        result = db.get(DCBQuery(items=[DCBQueryItem(tags=["tagB"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tag "tagC" - one event with "tagC".
        result = db.get(DCBQuery(items=[DCBQueryItem(tags=["tagC"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(3, result[0].position)
        self.assertEqual("EventType3", result[0].event.type)

        # Can query for tags "tagA" and tagB" - one event has both.
        result = db.get(DCBQuery(items=[DCBQueryItem(tags=["tagA", "tagB"])]))
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tags "tagB" and tagC" - no events have both.
        result = db.get(DCBQuery(items=[DCBQueryItem(tags=["tagB", "tagC"])]))
        self.assertEqual(0, len(result))

        # Can query for tags "tagB" or tagC" - two events have one or the other.
        result = db.get(
            DCBQuery(items=(DCBQueryItem(tags=["tagB"]), DCBQueryItem(tags=["tagC"])))
        )
        self.assertEqual(2, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual(3, result[1].position)
        self.assertEqual("EventType2", result[0].event.type)
        self.assertEqual("EventType3", result[1].event.type)

        # Can query for tags "tagB" or tagD" - only one event.
        result = db.get(
            DCBQuery(items=[DCBQueryItem(tags=["tagB"]), DCBQueryItem(tags=["tagD"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for tag "tagA" after position 2 - only one event.
        result = db.get(
            DCBQuery(items=[DCBQueryItem(tags=["tagA"])]),
            after=2,
        )
        self.assertEqual(1, len(result))
        self.assertEqual(3, result[0].position)
        self.assertEqual("EventType3", result[0].event.type)

        # Can query for type "EventType1" and tag "tagA" - zero events.
        result = db.get(
            DCBQuery(items=[DCBQueryItem(types=["EventType1"], tags=["tagA"])])
        )
        self.assertEqual(0, len(result))

        # Can query for type "EventType2" and tag "tagA" - only one event.
        result = db.get(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"], tags=["tagA"])])
        )
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0].position)
        self.assertEqual("EventType2", result[0].event.type)

        # Can query for type "EventType2" and tag "tagA" after position 2 - no events.
        result = db.get(
            DCBQuery(items=[DCBQueryItem(types=["EventType2"], tags=["tagA"])]),
            after=2,
        )
        self.assertEqual(0, len(result))

        # Append must fail if event store has events matching append condition.

        # Fail because append condition matches all events.
        with self.assertRaises(IntegrityError):
            db.append(
                events=(
                    DCBEvent(type="EventType4", data=b"data4"),
                    DCBEvent(type="EventType5", data=b"data5"),
                ),
                condition=DCBAppendCondition(
                    fail_if_events_match=DCBQuery(),
                    after=None,
                ),
            )

        result = db.get(DCBQuery())
        self.assertEqual(3, len(result))

        # Okay, because append condition after last position.
        position = db.append(
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

        result = db.get(DCBQuery())
        self.assertEqual(5, len(result))

        # Fail because event types match.
        with self.assertRaises(IntegrityError):
            db.append(
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
        position = db.append(
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
            db.append(
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
