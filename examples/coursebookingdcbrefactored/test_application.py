from __future__ import annotations

from typing import TYPE_CHECKING

from eventsourcing.domain import ProgrammingError
from eventsourcing.persistence import IntegrityError
from examples.coursebooking.enrolment_testcase import EnrolmentTestCase
from examples.coursebookingdcbrefactored.application import (
    EnrolmentWithDCBRefactored,
    Student,
    StudentAndCourse,
)

if TYPE_CHECKING:
    from examples.coursebooking.interface import EnrolmentInterface


class TestEnrolmentWithDCBRefactored(EnrolmentTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.env["PERSISTENCE_MODULE"] = "eventsourcing.dcb.popo"

    def construct_app(self) -> EnrolmentInterface:
        return EnrolmentWithDCBRefactored(self.env)

    def test_enrolment_with_postgres(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "eventsourcing.dcb.postgres_tt"
        super().test_enrolment_with_postgres()

    def test_enrolment(self) -> None:
        super().test_enrolment()

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
        app.update_max_courses(student_id, 10)
        student = app.get_student(student_id)
        self.assertEqual(10, student.max_courses)

        # Register course.
        course_id = app.register_course(name="Bio", places=3)
        self.assertTrue(course_id.startswith("course-"))
        course = app.get_course(course_id)
        self.assertEqual(course_id, course.id)
        self.assertEqual(3, course.places)

        # Update name.
        app.update_course_name(course_id, "Biology")
        course = app.get_course(course_id)
        self.assertEqual("Biology", course.name)

        # Update places.
        app.update_course_places(course_id, 10)
        course = app.get_course(course_id)
        self.assertEqual(10, course.places)

        # Join course.
        self.assertEqual(student.course_ids, [])
        self.assertEqual(course.student_ids, [])
        app.join_course(student_id=student_id, course_id=course_id)
        student = app.get_student(student_id)
        course = app.get_course(course_id)
        self.assertEqual(student.course_ids, [course_id])
        self.assertEqual(course.student_ids, [student_id])

        # Leave course.
        app.leave_course(student_id=student_id, course_id=course_id)
        student = app.get_student(student_id)
        course = app.get_course(course_id)
        self.assertEqual(student.course_ids, [])
        self.assertEqual(course.student_ids, [])

        # Multi-get.
        objs = app.repository.get_many(course_id, student_id)
        self.assertEqual(2, len(objs))

        # Check order is preserved.
        self.assertEqual([course_id, student_id], [o.id for o in objs if o])
        objs = app.repository.get_many(student_id, course_id)
        self.assertEqual([student_id, course_id], [o.id for o in objs if o])

        # Can't call non-command underscore methods.
        with self.assertRaisesRegex(ProgrammingError, "cannot be used"):
            student._(course_id=course_id)

        # Check joining a course doesn't conflict with concurrent student or course
        # name changes (because event types are not in the consistency boundary).
        group = app.repository.get_group(StudentAndCourse, student_id, course_id)
        group.student_joins_course()
        app.update_course_name(course_id, "Bio-science")
        app.update_student_name(student_id, "Bob")
        app.repository.save(group)

        # Check changing max courses does conflict.
        group = app.repository.get_group(StudentAndCourse, student_id, course_id)
        group.student_leaves_course()
        app.update_max_courses(student_id, 1)
        with self.assertRaises(IntegrityError):
            app.repository.save(group)

        # Can get limited perspective on an enduring object.
        student_name = app.get_student(
            student_id, types=[Student.Registered, Student.NameUpdated]
        )
        self.assertEqual(student_name.name, "Bob")
        self.assertEqual(student_name.max_courses, 4)  # Was changed to 1.

        # Can get limited perspective on an enduring object.
        student_max_courses = app.get_student(
            student_id, types=[Student.Registered, Student.MaxCoursesUpdated]
        )
        self.assertEqual(student_max_courses.name, "Max")  # Was changed to Maxine.
        self.assertEqual(student_max_courses.max_courses, 1)

        # Can't update name - type not in cb_types.
        with self.assertRaises(IntegrityError):
            student_max_courses.update_name(name="Jac")

        # Can't operate on enduring objects in a group...
        group = app.repository.get_group(StudentAndCourse, student_id, course_id)
        with self.assertRaises(IntegrityError):
            group.student.update_name("Jac")

        # ...unless the event type falls within the consistency boundary.
        group.student.update_max_courses(100)
        app.repository.save(group)
        student = app.get_student(student_id)
        self.assertEqual("Bob", student.name)
        self.assertEqual(100, student.max_courses)


del EnrolmentTestCase
