from __future__ import annotations

from typing import TYPE_CHECKING

from examples.coursebooking.test_application import TestEnrolment
from examples.coursebookingdcbrefactored.application import EnrolmentWithDCBRefactored

if TYPE_CHECKING:
    from examples.coursebooking.interface import Enrolment


class TestEnrolmentWithDCBRefactored(TestEnrolment):
    def setUp(self) -> None:
        super().setUp()
        self.env["PERSISTENCE_MODULE"] = "examples.dcb.popo"

    def construct_app(self) -> Enrolment:
        return EnrolmentWithDCBRefactored(self.env)

    def test_enrolment_with_postgres(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "examples.dcb.postgres_tt"
        super().test_enrolment_with_postgres()


del TestEnrolment
