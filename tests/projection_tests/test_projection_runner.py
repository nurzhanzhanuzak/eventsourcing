from __future__ import annotations

import warnings
import weakref
from threading import Event
from typing import TYPE_CHECKING
from unittest import TestCase

from eventsourcing.application import Application
from eventsourcing.domain import Aggregate
from eventsourcing.projection import Projection, ProjectionRunner
from tests.projection_tests.test_projection import (
    CountProjection,
    POPOCountRecorder,
    SpannerThrown,
    SpannerThrownError,
)

if TYPE_CHECKING:  # pragma: no cover
    from eventsourcing.domain import DomainEventProtocol
    from eventsourcing.persistence import Tracking


class TestProjectionRunner(TestCase):
    def test_runner(self):
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

    def test_runner_as_context_manager(self):
        with ProjectionRunner(
            application_class=Application,
            projection_class=CountProjection,
            tracking_recorder_class=POPOCountRecorder,
        ) as runner:

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

    def test_warning_error_not_assigned_to_deleted_runner(self):

        # Define a project that raises an exception.
        class BrokenProjection(Projection):
            def process_event(
                self, domain_event: DomainEventProtocol, tracking: Tracking
            ) -> None:
                raise ValueError

        # Construct a runner.
        runner = ProjectionRunner(
            application_class=Application,
            projection_class=BrokenProjection,
            tracking_recorder_class=POPOCountRecorder,
        )

        # Write an event.
        runner.app.save(Aggregate())

        # Error should be assigned to the runner.
        with self.assertRaises(ValueError):
            runner.run_forever()

        # Write another event.
        runner.app.save(Aggregate())

        # Get another application sequence
        application_sequence = runner.app.get_application_sequence()

        # Get a reference to the projection.
        projection = runner.projection

        # Construct another threading.Event.
        has_error = Event()

        # Get another weakref to the runner.
        ref = weakref.ref(runner)

        # Delete the runner object.
        del runner

        # Check the weakref returns None.
        self.assertIsNone(ref())

        # Call _process_events_loop and catch warning.
        with warnings.catch_warnings(record=True) as w:
            ProjectionRunner._process_events_loop(
                application_sequence,
                projection,
                has_error,
                ref,
            )

            self.assertEqual(1, len(w))
            self.assertIs(w[-1].category, RuntimeWarning)
            self.assertEqual(
                "ProjectionRunner was deleted before error could be assigned:",
                w[-1].message.args[0].split("\n")[0],
            )

        # Check the event was set anyway.
        self.assertTrue(has_error.is_set())
