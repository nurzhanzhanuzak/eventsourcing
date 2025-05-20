from __future__ import annotations

from typing import TYPE_CHECKING

from examples.coursebookingdcb.test_application import TestEnrolmentWithDCB
from examples.coursebookingdcbrefactored.application import EnrolmentWithDCBRefactored

if TYPE_CHECKING:
    from examples.coursebooking.interface import EnrolmentProtocol


class TestEnrolmentWithDCBRefactored(TestEnrolmentWithDCB):
    def construct_app(self) -> EnrolmentProtocol:
        return EnrolmentWithDCBRefactored(self.env)


del TestEnrolmentWithDCB
