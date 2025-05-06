from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar
from unittest.case import TestCase
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from eventsourcing.application import Application, ProcessingEvent, ProgrammingError
from eventsourcing.dispatch import singledispatchmethod
from eventsourcing.domain import Aggregate, DomainEventProtocol
from eventsourcing.persistence import IntegrityError, Notification, Tracking
from eventsourcing.system import (
    Follower,
    Leader,
    ProcessApplication,
    RecordingEvent,
    RecordingEventReceiver,
    System,
)
from eventsourcing.tests.application import BankAccounts
from eventsourcing.tests.domain import BankAccount
from eventsourcing.utils import get_topic, resolve_topic
from tests.application_tests.test_processapplication import EmailProcess

if TYPE_CHECKING:
    from collections.abc import Sequence

system_defined_as_global = System(
    pipes=[
        [
            BankAccounts,
            EmailProcess,
        ],
        [Application],
    ]
)


class TestSystem(TestCase):
    def test_graph_nodes_and_edges(self) -> None:
        system = System(
            pipes=[
                [
                    BankAccounts,
                    EmailProcess,
                ],
                [Application],
            ]
        )
        self.assertEqual(len(system.nodes), 3)
        self.assertEqual(system.nodes["BankAccounts"], get_topic(BankAccounts))
        self.assertEqual(system.nodes["EmailProcess"], get_topic(EmailProcess))
        self.assertEqual(system.nodes["Application"], get_topic(Application))

        self.assertEqual(system.leaders, ["BankAccounts"])
        self.assertEqual(system.followers, ["EmailProcess"])
        self.assertEqual(system.singles, ["Application"])

        self.assertEqual(len(system.edges), 1)
        self.assertIn(
            (
                "BankAccounts",
                "EmailProcess",
            ),
            system.edges,
        )

        self.assertEqual(len(system.singles), 1)

    def test_duplicate_edges_are_eliminated(self) -> None:
        system = System(
            pipes=[
                [
                    BankAccounts,
                    EmailProcess,
                ],
                [
                    BankAccounts,
                    EmailProcess,
                ],
                [Application],
            ]
        )
        self.assertEqual(len(system.nodes), 3)
        self.assertEqual(system.nodes["BankAccounts"], get_topic(BankAccounts))
        self.assertEqual(system.nodes["EmailProcess"], get_topic(EmailProcess))
        self.assertEqual(system.nodes["Application"], get_topic(Application))

        self.assertEqual(system.leaders, ["BankAccounts"])
        self.assertEqual(system.followers, ["EmailProcess"])
        self.assertEqual(system.singles, ["Application"])

        self.assertEqual(len(system.edges), 1)
        self.assertIn(
            (
                "BankAccounts",
                "EmailProcess",
            ),
            system.edges,
        )

        self.assertEqual(len(system.singles), 1)

    def test_raises_type_error_not_a_follower(self) -> None:
        with self.assertRaises(TypeError) as cm:
            System(
                pipes=[
                    [
                        BankAccounts,
                        Leader,
                    ],
                ]
            )
        exception = cm.exception
        self.assertEqual(
            exception.args[0],
            "Not a follower class: <class 'eventsourcing.system.Leader'>",
        )

    def test_raises_type_error_not_a_processor(self) -> None:
        with self.assertRaises(TypeError) as cm:
            System(
                pipes=[
                    [
                        BankAccounts,
                        Follower,
                        EmailProcess,
                    ],
                ]
            )
        exception = cm.exception
        self.assertEqual(
            exception.args[0],
            "Not a process application class: <class 'eventsourcing.system.Follower'>",
        )

    def test_is_leaders_only(self) -> None:
        system = System(
            pipes=[
                [
                    Leader,
                    ProcessApplication,
                    ProcessApplication,
                ],
            ]
        )
        self.assertEqual(list(system.leaders_only), ["Leader"])

    def test_leader_class(self) -> None:
        system = System(
            pipes=[
                [
                    Application,
                    ProcessApplication,
                    ProcessApplication,
                ],
            ]
        )
        self.assertTrue(issubclass(system.leader_cls("Application"), Leader))
        self.assertTrue(issubclass(system.leader_cls("ProcessApplication"), Leader))

    def test_system_has_topic_if_defined_as_module_attribute(self) -> None:
        system_topic = system_defined_as_global.topic
        self.assertTrue(system_topic.endswith("test_system:system_defined_as_global"))
        self.assertEqual(resolve_topic(system_topic), system_defined_as_global)

    def test_system_topic_is_none_if_defined_in_function_body(self) -> None:
        system = System([[]])
        with self.assertRaises(ProgrammingError):
            system.topic  # noqa: B018


class TestLeader(TestCase):
    def test(self) -> None:
        # Define fixture that receives prompts.
        class FollowerFixture(RecordingEventReceiver):
            def __init__(self) -> None:
                self.num_received = 0

            def receive_recording_event(
                self, new_recording_event: RecordingEvent
            ) -> None:
                self.num_received += 1

        # Test fixture is working.
        follower = FollowerFixture()
        follower.receive_recording_event(RecordingEvent("Leader", [], 1))
        self.assertEqual(follower.num_received, 1)

        # Construct leader.
        leader = Leader()
        leader.lead(follower)

        # Check follower receives a prompt when there are new events.
        leader.save(Aggregate())
        self.assertEqual(follower.num_received, 2)

        # Check follower doesn't receive prompt when no new events.
        leader.save()
        self.assertEqual(follower.num_received, 2)

        # Check follower doesn't receive prompt when recordings are filtered out.
        leader.notify_topics = ["topic1"]
        leader.save(Aggregate())
        self.assertEqual(follower.num_received, 2)


class TestFollower(TestCase):
    def test_process_event(self) -> None:
        class UUID5EmailNotification(Aggregate):
            def __init__(self, to: str, subject: str, message: str) -> None:
                self.to = to
                self.subject = subject
                self.message = message

            @staticmethod
            def create_id(to: str) -> UUID:
                return uuid5(NAMESPACE_URL, f"/emails/{to}")

        class UUID5EmailProcess(EmailProcess):
            @singledispatchmethod
            def policy(
                self,
                domain_event: DomainEventProtocol,
                processing_event: ProcessingEvent,
            ) -> None:
                if isinstance(domain_event, BankAccount.Opened):
                    notification = UUID5EmailNotification(
                        to=domain_event.email_address,
                        subject="Your New Account",
                        message=f"Dear {domain_event.full_name}, ...",
                    )
                    processing_event.collect_events(notification)

        bank_accounts = BankAccounts()
        email_process = UUID5EmailProcess()

        account = BankAccount.open(
            full_name="Alice",
            email_address="alice@example.com",
        )

        recordings = bank_accounts.save(account)

        self.assertEqual(len(recordings), 1)

        aggregate_event = recordings[0].domain_event
        notification = recordings[0].notification
        tracking = Tracking(bank_accounts.name, notification.id)

        # Process the event.
        email_process.process_event(aggregate_event, tracking)
        self.assertEqual(
            email_process.recorder.max_tracking_id(bank_accounts.name), notification.id
        )

        # Process the event again, ignore tracking integrity error.
        email_process.process_event(aggregate_event, tracking)
        self.assertEqual(
            email_process.recorder.max_tracking_id(bank_accounts.name), notification.id
        )

        # Create another event that will cause conflict with email processing.
        account = BankAccount.open(
            full_name="Alice",
            email_address="alice@example.com",
        )
        recordings = bank_accounts.save(account)

        # Process the event and expect an integrity error.
        aggregate_event = recordings[0].domain_event
        notification = recordings[0].notification
        tracking = Tracking(bank_accounts.name, notification.id)
        with self.assertRaises(IntegrityError):
            email_process.process_event(aggregate_event, tracking)

    def test_filter_received_notifications(self) -> None:
        class MyFollower(Follower):
            topics: ClassVar[Sequence[str]] = []

            @singledispatchmethod
            def policy(self, *args: Any, **kwargs: Any) -> None:
                pass

        follower = MyFollower()
        notifications = [
            Notification(
                id=1,
                originator_id=uuid4(),
                originator_version=1,
                state=b"",
                topic="topic1",
            )
        ]
        self.assertEqual(len(follower.filter_received_notifications(notifications)), 1)
        type(follower).topics = ["topic1"]
        self.assertEqual(len(follower.filter_received_notifications(notifications)), 1)
        type(follower).topics = ["topic2"]
        self.assertEqual(len(follower.filter_received_notifications(notifications)), 0)
