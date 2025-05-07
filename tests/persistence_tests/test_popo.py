from __future__ import annotations

from concurrent.futures.thread import ThreadPoolExecutor
from unittest import TestCase
from unittest.mock import Mock
from uuid import uuid4

from eventsourcing.domain import datetime_now_with_tzinfo
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    Notification,
    ProcessRecorder,
    ProgrammingError,
    StoredEvent,
    TrackingRecorder,
)
from eventsourcing.popo import (
    POPOAggregateRecorder,
    POPOApplicationRecorder,
    POPOFactory,
    POPOProcessRecorder,
    POPOSubscription,
    POPOTrackingRecorder,
)
from eventsourcing.tests.persistence import (
    AggregateRecorderTestCase,
    ApplicationRecorderTestCase,
    InfrastructureFactoryTestCase,
    ProcessRecorderTestCase,
    TrackingRecorderTestCase,
)
from eventsourcing.utils import Environment


class TestPOPOAggregateRecorder(AggregateRecorderTestCase):
    def create_recorder(self) -> AggregateRecorder:
        return POPOAggregateRecorder()


class TestPOPOApplicationRecorder(ApplicationRecorderTestCase[POPOApplicationRecorder]):
    def create_recorder(self) -> POPOApplicationRecorder:
        return POPOApplicationRecorder()

    def test_insert_select(self) -> None:
        super().test_insert_select()

        # Check select_notifications() does not use negative indexes.

        # Construct the recorder.
        recorder = self.create_recorder()

        # Write two stored events.
        stored_event1 = StoredEvent(
            originator_id=uuid4(),
            originator_version=self.INITIAL_VERSION,
            topic="topic1",
            state=b"state1",
        )
        stored_event2 = StoredEvent(
            originator_id=uuid4(),
            originator_version=self.INITIAL_VERSION,
            topic="topic2",
            state=b"state2",
        )
        recorder.insert_events([stored_event1, stored_event2])

        # This was returning 3.
        self.assertEqual(len(recorder.select_notifications(0, 10)), 2)

        # This was returning 4.
        self.assertEqual(len(recorder.select_notifications(-1, 10)), 2)

    def test_insert_subscribe(self) -> None:
        super().optional_test_insert_subscribe()

    def test_subscribe_concurrent_reading_and_writing(self) -> None:
        recorder = self.create_recorder()

        num_batches = 20
        batch_size = 100
        num_events = num_batches * batch_size

        def read(last_notification_id: int | None) -> None:
            start = datetime_now_with_tzinfo()
            with recorder.subscribe(last_notification_id) as subscription:
                for i, notification in enumerate(subscription):
                    # print("Read", i+1, "notifications")
                    last_notification_id = notification.id
                    if i + 1 == num_events:
                        break
            duration = datetime_now_with_tzinfo() - start
            print(
                "Finished reading",
                num_events,
                "events in",
                duration.total_seconds(),
                "seconds",
            )

        def write() -> None:
            start = datetime_now_with_tzinfo()
            for _ in range(num_batches):
                events = []
                for _ in range(batch_size):
                    stored_event = StoredEvent(
                        originator_id=uuid4(),
                        originator_version=self.INITIAL_VERSION,
                        topic="topic1",
                        state=b"state1",
                    )
                    events.append(stored_event)
                recorder.insert_events(events)
                # print("Wrote", i + 1, "notifications")
            duration = datetime_now_with_tzinfo() - start
            print(
                "Finished writing",
                num_events,
                "events in",
                duration.total_seconds(),
                "seconds",
            )

        thread_pool = ThreadPoolExecutor(max_workers=2)

        print("Concurrent...")
        # Get the max notification ID (for the subscription).
        last_notification_id = recorder.max_notification_id()
        write_job = thread_pool.submit(write)
        read_job = thread_pool.submit(read, last_notification_id)
        write_job.result()
        read_job.result()

        print("Sequential...")
        last_notification_id = recorder.max_notification_id()
        assert isinstance(last_notification_id, int)  # Should be int by now.
        write_job = thread_pool.submit(write)
        write_job.result()
        read_job = thread_pool.submit(read, last_notification_id)
        read_job.result()

        thread_pool.shutdown()

    def test_concurrent_throughput(self) -> None:
        super().test_concurrent_throughput()


class TestPOPOTrackingRecorder(TrackingRecorderTestCase):
    def create_recorder(self) -> TrackingRecorder:
        return POPOTrackingRecorder()

    def test_wait(self) -> None:
        super().test_wait()

    def test_insert_tracking(self) -> None:
        super().test_insert_tracking()


class TestPOPOProcessRecorder(ProcessRecorderTestCase):
    def create_recorder(self) -> ProcessRecorder:
        return POPOProcessRecorder()

    def test_performance(self) -> None:
        super().test_performance()


class TestPOPOInfrastructureFactory(InfrastructureFactoryTestCase[POPOFactory]):
    def setUp(self) -> None:
        self.env = Environment("TestCase")
        super().setUp()

    def expected_factory_class(self) -> type[POPOFactory]:
        return POPOFactory

    def expected_aggregate_recorder_class(self) -> type[AggregateRecorder]:
        return POPOAggregateRecorder

    def expected_application_recorder_class(self) -> type[ApplicationRecorder]:
        return POPOApplicationRecorder

    def expected_tracking_recorder_class(self) -> type[TrackingRecorder]:
        return POPOTrackingRecorder

    class POPOTrackingRecorderSubclass(POPOTrackingRecorder):
        pass

    def tracking_recorder_subclass(self) -> type[TrackingRecorder]:
        return self.POPOTrackingRecorderSubclass

    def expected_process_recorder_class(self) -> type[ProcessRecorder]:
        return POPOProcessRecorder


class TestPOPOSubscription(TestCase):
    def test_listen_catches_error(self) -> None:

        mock_recorder = Mock(spec=POPOApplicationRecorder)

        subscription = POPOSubscription(mock_recorder, 0)

        # self.assertIsInstance(subscription._thread_error, TypeError)

        with self.assertRaises(TypeError):
            next(subscription)

        with self.assertRaises(TypeError):
            next(subscription)

        subscription._thread_error = None

        with self.assertRaises(StopIteration):
            next(subscription)

        subscription._notifications = [
            Notification(
                id=1, originator_id=uuid4(), originator_version=1, topic="", state=b""
            ),
            Notification(
                id=2, originator_id=uuid4(), originator_version=1, topic="", state=b""
            ),
            Notification(
                id=3, originator_id=uuid4(), originator_version=1, topic="", state=b""
            ),
        ]
        subscription._notifications_index = 0

        subscription._has_been_stopped = False
        self.assertEqual(1, next(subscription).id)
        self.assertEqual(2, next(subscription).id)
        self.assertEqual(3, next(subscription).id)

        subscription._notifications_queue.put([])

        with self.assertRaises(StopIteration):
            next(subscription)

        subscription._notifications = [
            Notification(
                id=4, originator_id=uuid4(), originator_version=1, topic="", state=b""
            ),
            Notification(
                id=5, originator_id=uuid4(), originator_version=1, topic="", state=b""
            ),
            Notification(
                id=6, originator_id=uuid4(), originator_version=1, topic="", state=b""
            ),
        ]
        subscription._notifications_index = 0
        subscription._thread_error = ValueError()
        subscription._has_been_stopped = True

        with self.assertRaises(ValueError):
            next(subscription)

        with self.assertRaises(ProgrammingError):
            subscription.__exit__(None, None, None)

        subscription.__enter__()
        with self.assertRaises(ProgrammingError):
            subscription.__enter__()

        subscription._has_been_stopped = False
        subscription._thread_error = None
        subscription._loop_on_pull()
        self.assertIsInstance(subscription._thread_error, TypeError)
        subscription._has_been_stopped = False
        subscription._thread_error = ValueError()
        subscription._loop_on_pull()
        self.assertIsInstance(subscription._thread_error, ValueError)


del AggregateRecorderTestCase
del ApplicationRecorderTestCase
del TrackingRecorderTestCase
del ProcessRecorderTestCase
del InfrastructureFactoryTestCase
