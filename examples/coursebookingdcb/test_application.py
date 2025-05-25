from __future__ import annotations

from typing import TYPE_CHECKING

from examples.coursebooking.test_application import TestEnrolment
from examples.coursebookingdcb.application import EnrolmentWithDCB

if TYPE_CHECKING:
    from examples.coursebooking.interface import Enrolment


class TestEnrolmentWithDCB(TestEnrolment):
    def setUp(self) -> None:
        super().setUp()
        self.env["PERSISTENCE_MODULE"] = "examples.dcb.popo"

    def construct_app(self) -> Enrolment:
        return EnrolmentWithDCB(self.env)

    def test_enrolment_with_postgres(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "examples.dcb.postgres_tt"
        super().test_enrolment_with_postgres()


del TestEnrolment
