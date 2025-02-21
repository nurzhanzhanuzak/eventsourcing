from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock
from uuid import uuid4

from eventsourcing.persistence import ProgrammingError, StoredEvent, Tracking
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
    def create_recorder(self):
        return POPOAggregateRecorder()


class TestPOPOApplicationRecorder(ApplicationRecorderTestCase):
    def create_recorder(self):
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

    def test_insert_subscribe(self):
        super().optional_test_insert_subscribe()

    def test_subscribe_concurrent_reading_and_writing(self) -> None:
        recorder = self.create_recorder()

        num_batches = 20
        batch_size = 100
        num_events = num_batches * batch_size

        def read(last_notification_id: int):
            start = datetime.now()
            with recorder.subscribe(last_notification_id) as subscription:
                for i, notification in enumerate(subscription):
                    # print("Read", i+1, "notifications")
                    last_notification_id = notification.id
                    if i + 1 == num_events:
                        break
            duration = datetime.now() - start
            print(
                "Finished reading",
                num_events,
                "events in",
                duration.total_seconds(),
                "seconds",
            )

        def write():
            start = datetime.now()
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
            duration = datetime.now() - start
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
        write_job = thread_pool.submit(write)
        write_job.result()
        read_job = thread_pool.submit(read, last_notification_id)
        read_job.result()

        thread_pool.shutdown()


class TestPOPOTrackingRecorder(TrackingRecorderTestCase):
    def create_recorder(self):
        return POPOTrackingRecorder()

    def test_wait(self):
        super().test_wait()


class TestPOPOProcessRecorder(ProcessRecorderTestCase):
    def create_recorder(self):
        return POPOProcessRecorder()

    def test_performance(self):
        super().test_performance()

    def test_max_doesnt_increase_when_lower_inserted_later(self) -> None:
        # Construct the recorder.
        recorder = self.create_recorder()

        tracking1 = Tracking(
            application_name="upstream_app",
            notification_id=1,
        )
        tracking2 = Tracking(
            application_name="upstream_app",
            notification_id=2,
        )

        # Insert tracking info.
        recorder.insert_events(
            stored_events=[],
            tracking=tracking2,
        )

        # Get current position.
        self.assertEqual(
            recorder.max_tracking_id("upstream_app"),
            2,
        )

        # Insert tracking info.
        recorder.insert_events(
            stored_events=[],
            tracking=tracking1,
        )

        # Get current position.
        self.assertEqual(
            recorder.max_tracking_id("upstream_app"),
            2,
        )


class TestPOPOInfrastructureFactory(InfrastructureFactoryTestCase):
    def setUp(self) -> None:
        self.env = Environment("TestCase")
        super().setUp()

    def expected_factory_class(self):
        return POPOFactory

    def expected_aggregate_recorder_class(self):
        return POPOAggregateRecorder

    def expected_application_recorder_class(self):
        return POPOApplicationRecorder

    def expected_tracking_recorder_class(self):
        return POPOTrackingRecorder

    class POPOTrackingRecorderSubclass(POPOTrackingRecorder):
        pass

    def tracking_recorder_subclass(self):
        return self.POPOTrackingRecorderSubclass

    def expected_process_recorder_class(self):
        return POPOProcessRecorder


class TestPOPOSubscription(TestCase):
    def test_listen_catches_error(self):

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

        subscription._notifications = [1, 2, 3]
        subscription._notifications_index = 0

        subscription._has_been_stopped = False
        self.assertEqual(1, next(subscription))
        self.assertEqual(2, next(subscription))
        self.assertEqual(3, next(subscription))

        subscription._notifications_queue.put([])

        with self.assertRaises(StopIteration):
            next(subscription)

        subscription._notifications = [1, 2, 3]
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
