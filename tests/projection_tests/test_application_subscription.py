from unittest import TestCase

from eventsourcing.application import Application
from eventsourcing.domain import Aggregate
from eventsourcing.persistence import Tracking
from eventsourcing.projection import ApplicationSubscription


class TestApplicationSubscription(TestCase):
    def test(self):
        app = Application()

        max_notification_id = app.recorder.max_notification_id()

        aggregate = Aggregate()
        aggregate.trigger_event(Aggregate.Event)
        aggregate.trigger_event(Aggregate.Event)
        aggregate.trigger_event(Aggregate.Event)
        app.save(aggregate)

        subscription = ApplicationSubscription(app=app, gt=max_notification_id)

        # Catch up.
        for domain_event, tracking in subscription:
            self.assertIsInstance(domain_event, Aggregate.Event)
            self.assertIsInstance(tracking, Tracking)
            self.assertEqual(tracking.application_name, app.name)
            if max_notification_id is not None:
                self.assertGreater(tracking.notification_id, max_notification_id)
            if tracking.notification_id == app.recorder.max_notification_id():
                break

        max_notification_id = app.recorder.max_notification_id()

        aggregate.trigger_event(Aggregate.Event)
        aggregate.trigger_event(Aggregate.Event)
        aggregate.trigger_event(Aggregate.Event)
        app.save(aggregate)

        # Continue.
        for domain_event, tracking in subscription:
            self.assertIsInstance(domain_event, Aggregate.Event)
            self.assertIsInstance(tracking, Tracking)
            self.assertEqual(tracking.application_name, app.name)
            if max_notification_id is not None:
                self.assertGreater(tracking.notification_id, max_notification_id)
            if tracking.notification_id == app.recorder.max_notification_id():
                break
