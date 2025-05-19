from __future__ import annotations

from unittest import TestCase
from uuid import uuid4

from eventsourcing.postgres import PostgresDatastore
from eventsourcing.tests.postgres_utils import drop_tables
from examples.coursebookingdcb.application import (
    AlreadyJoinedError,
    CourseNotFoundError,
    Enrolment,
    FullyBookedError,
    StudentNotFoundError,
    TooManyCoursesError,
)
from tests.dcb_tests.postgres import PostgresDCBEventStore
from tests.dcb_tests.test_dcb import drop_functions_and_types


class TestEnrolment(TestCase):
    def setUp(self) -> None:
        self.env: dict[str, str] = {}

    def test_enrolment(self) -> None:
        # Construct application object.
        app = Enrolment(env=self.env)

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
            app.join_course(f"course-{uuid4()}", grace)

        # Student not found.
        with self.assertRaises(StudentNotFoundError):
            app.join_course(dcb, f"student-{uuid4()}")

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

    def test_enrollment_with_postgres(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "eventsourcing.postgres"
        self.env["POSTGRES_DBNAME"] = "eventsourcing"
        self.env["POSTGRES_HOST"] = "127.0.0.1"
        self.env["POSTGRES_PORT"] = "5432"
        self.env["POSTGRES_USER"] = "eventsourcing"
        self.env["POSTGRES_PASSWORD"] = "eventsourcing"  # noqa: S105

        try:
            self.test_enrolment()
        finally:
            drop_tables()
            eventstore = PostgresDCBEventStore(
                datastore=PostgresDatastore(
                    dbname=self.env["POSTGRES_DBNAME"],
                    host=self.env["POSTGRES_HOST"],
                    port=self.env["POSTGRES_PORT"],
                    user=self.env["POSTGRES_USER"],
                    password=self.env["POSTGRES_PASSWORD"],
                )
            )
            drop_functions_and_types(eventstore)
