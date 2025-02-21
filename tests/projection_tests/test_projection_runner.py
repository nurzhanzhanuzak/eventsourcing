from __future__ import annotations

from unittest import TestCase

from eventsourcing.application import Application
from eventsourcing.domain import Aggregate
from eventsourcing.projection import ProjectionRunner
from tests.projection_tests.test_projection import (
    CountProjection,
    POPOCountRecorder,
    SpannerThrown,
    SpannerThrownError,
)


class TestProjectionRunner(TestCase):
    def test(self):
        runner = ProjectionRunner(
            application_class=Application,
            projection_class=CountProjection,
            tracking_recorder_class=POPOCountRecorder,
        )

        app = runner.app
        aggregate = Aggregate()
        aggregate.trigger_event(event_class=Aggregate.Event)
        aggregate.trigger_event(event_class=Aggregate.Event)
        recordings = app.save(aggregate)

        runner.wait(recordings[-1].notification.id)
        self.assertEqual(
            runner.projection.tracking_recorder.get_all_events_counter(), 3
        )
        self.assertEqual(
            runner.projection.tracking_recorder.get_created_events_counter(), 1
        )
        self.assertEqual(
            runner.projection.tracking_recorder.get_subsequent_events_counter(), 2
        )

        aggregate = Aggregate()
        aggregate.trigger_event(event_class=Aggregate.Event)
        aggregate.trigger_event(event_class=Aggregate.Event)
        recordings = app.save(aggregate)

        runner.wait(recordings[-1].notification.id)
        self.assertEqual(
            runner.projection.tracking_recorder.get_all_events_counter(), 6
        )
        self.assertEqual(
            runner.projection.tracking_recorder.get_created_events_counter(), 2
        )
        self.assertEqual(
            runner.projection.tracking_recorder.get_subsequent_events_counter(), 4
        )

        runner.run_forever(timeout=0.1)

        aggregate.trigger_event(event_class=SpannerThrown)
        app.save(aggregate)

        with self.assertRaises(SpannerThrownError):
            runner.run_forever()

        with self.assertRaises(SpannerThrownError):
            runner.wait("application", app.recorder.max_notification_id())

        with runner, self.assertRaises(SpannerThrownError):
            runner.run_forever()

        #
        # # Resume....
        # application_sequence = app.catchup_subscription(
        #     tracking_recorder.max_tracking_id(app.name)
        # )
        #
        # self.assertEqual(
        #     runner.projection.tracking_recorder.get_all_events_counter(), 6
        # )
        # self.assertEqual(
        #     runner.projection.tracking_recorder.get_created_events_counter(), 2
        # )
        # self.assertEqual(
        #     runner.projection.tracking_recorder.get_subsequent_events_counter(), 4
        # )
        #
        # runner.projection.wait(recordings[-1].notification.id)
        # self.assertEqual(
        #     runner.projection.tracking_recorder.get_all_events_counter(), 6
        # )
        # self.assertEqual(
        #     runner.projection.tracking_recorder.get_created_events_counter(), 2
        # )
        # self.assertEqual(
        #     runner.projection.tracking_recorder.get_subsequent_events_counter(), 4
        # )
        #
        # aggregate = Aggregate()
        # aggregate.trigger_event(event_class=Aggregate.Event)
        # aggregate.trigger_event(event_class=Aggregate.Event)
        # recordings = app.save(aggregate)
        #
        # runner.projection.wait(recordings[-1].notification.id)
        # self.assertEqual(
        #     runner.projection.tracking_recorder.get_all_events_counter(), 9
        # )
        # self.assertEqual(
        #     runner.projection.tracking_recorder.get_created_events_counter(), 3
        # )
        # self.assertEqual(
        #     runner.projection.tracking_recorder.get_subsequent_events_counter(), 6
        # )
        #


#
# class TestCountProjectWithPOPO(CountProjectionTestCase):
#     def construct_tracking_recorder(self):
#         return POPOCountRecorder()
#
# class TestCountProjectWithPostgres(CountProjectionTestCase):
#     tracking_table_name = "notification_tracking"
#     counter_table_name = "events_counter"
#     all_table_names = (
#         "notification_tracking",
#         "events_counter",
#     )
#
#     def setUp(self) -> None:
#         super().setUp()
#         self.drop_tables()
#
#     def tearDown(self) -> None:
#         super().tearDown()
#         self.drop_tables()
#
#     def drop_tables(self):
#         datastore = PostgresDatastore(
#             "eventsourcing",
#             "127.0.0.1",
#             "5432",
#             "eventsourcing",
#             "eventsourcing",
#         )
#         for table_name in self.all_table_names:
#             drop_postgres_table(datastore, table_name)
#
#     def construct_tracking_recorder(self) -> CountRecorder:
#         datastore = PostgresDatastore(
#             "eventsourcing",
#             "127.0.0.1",
#             "5432",
#             "eventsourcing",
#             "eventsourcing",
#         )
#         recorder = PostgresCountRecorder(
#             datastore,
#             tracking_table_name=self.tracking_table_name,
#             counter_table_name=self.counter_table_name,
#         )
#         recorder.create_table()
#         return recorder
