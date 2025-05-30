from __future__ import annotations

from typing import TYPE_CHECKING

from examples.coursebooking.enrolment_testcase import EnrolmentTestCase
from examples.coursebookingdcb.application import EnrolmentWithDCB

if TYPE_CHECKING:
    from examples.coursebooking.interface import EnrolmentInterface


class TestEnrolmentWithDCB(EnrolmentTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.env["PERSISTENCE_MODULE"] = "eventsourcing.dcb.popo"

    def construct_app(self) -> EnrolmentInterface:
        return EnrolmentWithDCB(self.env)

    def test_enrolment_with_postgres(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "examples.coursebookingdcb.postgres_ts"
        super().test_enrolment_with_postgres()


del EnrolmentTestCase
