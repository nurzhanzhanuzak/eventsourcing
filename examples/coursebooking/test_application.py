from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import TestLoader, TestSuite

from eventsourcing.persistence import IntegrityError
from examples.coursebooking.application import (
    EnrolmentWithAggregates,
)
from examples.coursebooking.enrolment_testcase import EnrolmentTestCase

if TYPE_CHECKING:
    from examples.coursebooking.interface import EnrolmentInterface


class TestEnrolmentWithAggregates(EnrolmentTestCase):
    def construct_app(self) -> EnrolmentInterface:
        return EnrolmentWithAggregates(self.env)

    def test_enrolment_with_postgres(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "eventsourcing.postgres"
        super().test_enrolment_with_postgres()


class TestEnrolmentConsistency(TestEnrolmentWithAggregates):
    def test_enrolment(self) -> None:
        # Construct application object.
        app = self.construct_app()

        # Register courses.
        french = app.register_course("French", places=5)

        # Register students.
        sara = app.register_student("Sara", max_courses=3)
        bastian = app.register_student("Bastian", max_courses=3)

        # Try to break recorded consistency with concurrent operation.
        assert isinstance(app, EnrolmentWithAggregates)
        student = app.get_student(sara)
        course = app.get_course(french)
        student.join_course(course.id)
        course.accept_student(student.id)

        # During this operation, Bastian joins French.
        app.join_course(bastian, french)

        # Can't proceed with concurrent operation because course changed.
        with self.assertRaises(IntegrityError):
            app.save(student, course)

        # Check Sara doesn't have French, and French doesn't have Sara.
        self.assertNotIn("Sara", app.list_students_for_course(french))
        self.assertNotIn("French", app.list_courses_for_student(sara))


test_cases = (TestEnrolmentWithAggregates, TestEnrolmentConsistency)


def load_tests(loader: TestLoader, _: TestSuite, __: str | None) -> TestSuite:
    suite = TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
