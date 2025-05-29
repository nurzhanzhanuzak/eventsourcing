from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import TestCase, TestLoader, TestSuite

from eventsourcing.persistence import IntegrityError
from eventsourcing.tests.postgres_utils import drop_tables
from examples.coursebooking.application import (
    EnrolmentWithAggregates,
)
from examples.coursebooking.interface import (
    AlreadyJoinedError,
    CourseNotFoundError,
    FullyBookedError,
    StudentNotFoundError,
    TooManyCoursesError,
)

if TYPE_CHECKING:
    from examples.coursebooking.interface import EnrolmentInterface


class TestEnrolment(TestCase):
    def setUp(self) -> None:
        self.env: dict[str, str] = {}

    def construct_app(self) -> EnrolmentInterface:
        raise NotImplementedError

    def test_enrolment(self) -> None:
        # Construct application object.
        app = self.construct_app()

        # Register courses.
        dcb = app.register_course("Dynamic Consistency Boundaries", places=5)
        maths = app.register_course("Maths", places=5)
        biology = app.register_course("Biology", places=5)
        french = app.register_course("French", places=5)
        spanish = app.register_course("Spanish", places=5)

        # Register students.
        sara = app.register_student("Sara", max_courses=3)
        mollie = app.register_student("Mollie", max_courses=3)
        allard = app.register_student("Allard", max_courses=3)
        grace = app.register_student("Grace", max_courses=3)
        bastian = app.register_student("Bastian", max_courses=3)
        greg = app.register_student("Greg", max_courses=3)
        katherine = app.register_student("Katherine", max_courses=3)

        # Fill 'Dynamic Consistency Boundaries' course.
        app.join_course(sara, dcb)
        app.join_course(mollie, dcb)
        app.join_course(allard, dcb)
        app.join_course(grace, dcb)
        app.join_course(bastian, dcb)

        # Greg can't join because the course is full.
        with self.assertRaises(FullyBookedError):
            app.join_course(greg, dcb)

        # Greg joins other courses instead.
        app.join_course(greg, french)
        app.join_course(greg, spanish)
        app.join_course(greg, maths)

        # Greg has enough to do already.
        with self.assertRaises(TooManyCoursesError):
            app.join_course(greg, biology)

        # Katherine also does French.
        app.join_course(katherine, french)

        # Katherine already does French.
        with self.assertRaises(AlreadyJoinedError):
            app.join_course(katherine, french)

        # Course not found.
        with self.assertRaises(CourseNotFoundError):
            app.join_course(grace, "not-a-course")

        # Student not found.
        with self.assertRaises(StudentNotFoundError):
            app.join_course("not-a-student", dcb)

        # List students for Dynamic Consistency Boundaries.
        students = app.list_students_for_course(dcb)
        self.assertEqual(students, ["Sara", "Mollie", "Allard", "Grace", "Bastian"])

        # List students for French.
        students = app.list_students_for_course(french)
        self.assertEqual(students, ["Greg", "Katherine"])

        # List courses for Sara.
        courses = app.list_courses_for_student(sara)
        self.assertEqual(courses, ["Dynamic Consistency Boundaries"])

        # List courses for Greg.
        courses = app.list_courses_for_student(greg)
        self.assertEqual(courses, ["French", "Spanish", "Maths"])

    def test_enrolment_with_postgres(self) -> None:
        self.env["POSTGRES_DBNAME"] = "eventsourcing"
        self.env["POSTGRES_HOST"] = "127.0.0.1"
        self.env["POSTGRES_PORT"] = "5432"
        self.env["POSTGRES_USER"] = "eventsourcing"
        self.env["POSTGRES_PASSWORD"] = "eventsourcing"  # noqa: S105
        try:
            self.test_enrolment()
        finally:
            drop_tables()


class TestEnrolmentWithAggregates(TestEnrolment):
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
