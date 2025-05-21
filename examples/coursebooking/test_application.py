from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import TestCase

from eventsourcing.persistence import IntegrityError
from eventsourcing.tests.postgres_utils import drop_tables
from examples.coursebooking.application import (
    Enrolment,
)
from examples.coursebooking.interface import (
    AlreadyJoinedError,
    CourseNotFoundError,
    FullyBookedError,
    StudentNotFoundError,
    TooManyCoursesError,
)

if TYPE_CHECKING:
    from examples.coursebooking.interface import EnrolmentProtocol


class TestEnrolment(TestCase):
    def setUp(self) -> None:
        self.env: dict[str, str] = {}

    def construct_app(self) -> EnrolmentProtocol:
        return Enrolment(self.env)

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
        app.join_course(dcb, sara)
        app.join_course(dcb, mollie)
        app.join_course(dcb, allard)
        app.join_course(dcb, grace)
        app.join_course(dcb, bastian)

        # Greg can't join because the course is full.
        with self.assertRaises(FullyBookedError):
            app.join_course(dcb, greg)

        # Greg joins other courses instead.
        app.join_course(french, greg)
        app.join_course(spanish, greg)
        app.join_course(maths, greg)

        # Greg has enough to do already.
        with self.assertRaises(TooManyCoursesError):
            app.join_course(biology, greg)

        # Katherine also does French.
        app.join_course(french, katherine)

        # Katherine already does French.
        with self.assertRaises(AlreadyJoinedError):
            app.join_course(french, katherine)

        # Course not found.
        with self.assertRaises(CourseNotFoundError):
            app.join_course("not-a-course", grace)

        # Student not found.
        with self.assertRaises(StudentNotFoundError):
            app.join_course(dcb, "not-a-student")

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
        if "PERSISTENCE_MODULE" not in self.env:
            self.env["PERSISTENCE_MODULE"] = "eventsourcing.postgres"
        self.env["POSTGRES_DBNAME"] = "eventsourcing"
        self.env["POSTGRES_HOST"] = "127.0.0.1"
        self.env["POSTGRES_PORT"] = "5432"
        self.env["POSTGRES_USER"] = "eventsourcing"
        self.env["POSTGRES_PASSWORD"] = "eventsourcing"  # noqa: S105
        self.env["POSTGRES_ORIGINATOR_ID_TYPE"] = "text"
        try:
            self.test_enrolment()
        finally:
            drop_tables()


class TestEnrolmentConsistency(TestEnrolment):
    def test_enrolment(self) -> None:
        # Construct application object.
        app = self.construct_app()

        # Register courses.
        french = app.register_course("French", places=5)

        # Register students.
        sara = app.register_student("Sara", max_courses=3)
        bastian = app.register_student("Bastian", max_courses=3)

        # Try to break recorded consistency with concurrent operation.
        assert isinstance(app, Enrolment)
        student = app.get_student(sara)
        course = app.get_course(french)
        student.join_course(course.id)
        course.accept_student(student.id)

        # During this operation, Bastian joins French.
        app.join_course(french, bastian)

        # Can't proceed with concurrent operation because course changed.
        with self.assertRaises(IntegrityError):
            app.save(student, course)

        # Check Sara doesn't have French, and French doesn't have Sara.
        self.assertNotIn("Sara", app.list_students_for_course(french))
        self.assertNotIn("French", app.list_courses_for_student(sara))
