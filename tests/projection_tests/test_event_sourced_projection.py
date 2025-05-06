from unittest import TestCase
from uuid import NAMESPACE_URL, UUID, uuid5

from eventsourcing.application import (
    AggregateNotFoundError,
    Application,
    ProcessingEvent,
)
from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.domain import Aggregate, DomainEventProtocol, event
from eventsourcing.projection import (
    EventSourcedProjection,
    EventSourcedProjectionRunner,
)
from eventsourcing.utils import get_topic


class Counter(Aggregate):
    def __init__(self, name: str) -> None:
        self.name = name
        self.count = 0

    @classmethod
    def create_id(cls, name: str) -> UUID:
        return uuid5(NAMESPACE_URL, f"/counters/{name}")

    @event("Incremented")
    def increment(self) -> None:
        self.count += 1


class Counters(EventSourcedProjection):
    @singledispatchmethod
    def policy(
        self,
        domain_event: DomainEventProtocol,
        processing_event: ProcessingEvent,
    ) -> None:
        topic = get_topic(type(domain_event))
        try:
            counter_id = Counter.create_id(topic)
            counter: Counter = self.repository.get(counter_id)
        except AggregateNotFoundError:
            counter = Counter(topic)
        counter.increment()
        processing_event.collect_events(counter)

    def get_count(self, domain_event_class: type[DomainEventProtocol]) -> int:
        topic = get_topic(domain_event_class)
        counter_id = Counter.create_id(topic)
        try:
            counter: Counter = self.repository.get(counter_id)
        except AggregateNotFoundError:
            return 0
        return counter.count


class TestEventSourcedProjection(TestCase):

    def test_event_sourced_projection(self) -> None:
        with EventSourcedProjectionRunner(
            application_class=Application,
            projection_class=Counters,
            env={
                "PERSISTENCE_MODULE": "eventsourcing.popo",
                "COUNTERS_PERSISTENCE_MODULE": "eventsourcing.popo",
            },
        ) as runner:
            recordings = runner.app.save(Aggregate())
            runner.wait(recordings[-1].notification.id)
            self.assertEqual(1, runner.projection.get_count(Aggregate.Created))
            self.assertEqual(0, runner.projection.get_count(Aggregate.Event))

            recordings = runner.app.save(Aggregate())
            runner.wait(recordings[-1].notification.id)
            self.assertEqual(2, runner.projection.get_count(Aggregate.Created))
            self.assertEqual(0, runner.projection.get_count(Aggregate.Event))

            recordings = runner.app.save(Aggregate())
            runner.wait(recordings[-1].notification.id)
            self.assertEqual(3, runner.projection.get_count(Aggregate.Created))
            self.assertEqual(0, runner.projection.get_count(Aggregate.Event))

            aggregate = Aggregate()
            aggregate.trigger_event(Aggregate.Event)
            recordings = runner.app.save(aggregate)
            runner.wait(recordings[-1].notification.id)
            self.assertEqual(4, runner.projection.get_count(Aggregate.Created))
            self.assertEqual(1, runner.projection.get_count(Aggregate.Event))
