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

    def test_extra(self) -> None:
        app = EnrolmentWithDCBRefactored(self.env)

        # Register student.
        student_id = app.register_student(name="Max", max_courses=4)
        self.assertTrue(student_id.startswith("student-"))

        # Get student.
        student = app.get_student(student_id)
        self.assertEqual(student_id, student.id)
        self.assertEqual("Max", student.name)
        self.assertEqual(4, student.max_courses)

        # Update name.
        app.update_student_name(student_id, "Maxine")
        student = app.get_student(student_id)
        self.assertEqual("Maxine", student.name)

        # Update max_courses.
        app.update_student_max_courses(student_id, 10)
        student = app.get_student(student_id)
        self.assertEqual(10, student.max_courses)

        # Test course.
        course_id = app.register_course(name="Max", places=3)
        self.assertTrue(course_id.startswith("course-"))
        course = app.get_course(course_id)
        self.assertEqual(course_id, course.id)
        self.assertEqual(3, course.places)

        # Join course.
        # course_id = app.join_course(course_id=)


del TestEnrolment
