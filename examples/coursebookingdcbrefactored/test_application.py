from __future__ import annotations

from typing import TYPE_CHECKING

from examples.coursebookingdcb.test_application import TestEnrolmentWithDCB
from examples.coursebookingdcbrefactored.application import EnrolmentWithDCBRefactored

if TYPE_CHECKING:
    from examples.coursebooking.interface import Enrolment


class TestEnrolmentWithDCBRefactored(TestEnrolmentWithDCB):
    def construct_app(self) -> Enrolment:
        return EnrolmentWithDCBRefactored(self.env)


del TestEnrolmentWithDCB
