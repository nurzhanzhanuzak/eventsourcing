from __future__ import annotations

from eventsourcing.domain import ProgrammingError
from eventsourcing.persistence import IntegrityError
from examples.coursebooking.enrolment_testcase import EnrolmentTestCase
from examples.coursebookingdcbrefactored.application import (
    Course,
    EnrolmentWithDCBRefactored,
    Student,
    StudentAndCourse,
    StudentJoinedCourse,
    StudentLeftCourse,
)


class TestEnrolmentWithDCBRefactored(EnrolmentTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.env["PERSISTENCE_MODULE"] = "eventsourcing.dcb.popo"

    def construct_app(self) -> EnrolmentWithDCBRefactored:
        return EnrolmentWithDCBRefactored(self.env)

    def test_enrolment_with_postgres(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "eventsourcing.dcb.postgres_tt"
        super().test_enrolment_with_postgres()

    def test_enrolment(self) -> None:
        super().test_enrolment()

        with self.construct_app() as app:

            # Register student.
            student_id = app.register_student(name="Max", max_courses=4)

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

            # Update name.
            app.update_course_name(course_id, "Biology")
            course = app.get_course(course_id)
            self.assertEqual("Biology", course.name)

            # Update places.
            app.update_places(course_id, 10)
            course = app.get_course(course_id)
            self.assertEqual(10, course.places)

            # Join course.
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

            # Check get_many() preserves order.
            objs = app.repository.get_many(course_id, student_id)
            self.assertEqual([course_id, student_id], [o.id for o in objs if o])
            objs = app.repository.get_many(student_id, course_id)
            self.assertEqual([student_id, course_id], [o.id for o in objs if o])

            # Can't call non-command underscore methods.
            with self.assertRaisesRegex(ProgrammingError, "cannot be used"):
                student._(course_id=course_id)

            # Define a group with a more limited context boundary.
            class StudentAndCourseLimitedCB(StudentAndCourse):
                cb_types = (
                    Student.Registered,
                    Course.Registered,
                    Student.MaxCoursesUpdated,
                    Course.PlacesUpdated,
                    StudentJoinedCourse,
                    StudentLeftCourse,
                )

            # Check joining a course doesn't conflict with concurrent name changes.
            group = app.repository.get_group(
                StudentAndCourseLimitedCB, student_id, course_id
            )
            group.student_joins_course()
            app.update_course_name(course_id, "Bio-science")
            app.update_student_name(student_id, "Bob")
            app.repository.save(group)

            # Check conflicting with concurrent changes raises IntegrityError.
            group = app.repository.get_group(
                StudentAndCourseLimitedCB, student_id, course_id
            )
            group.student_leaves_course()
            app.update_max_courses(student_id, 1)
            with self.assertRaises(IntegrityError):
                app.repository.save(group)

            # Check conflicting with concurrent changes raises IntegrityError.
            group = app.repository.get_group(
                StudentAndCourseLimitedCB, student_id, course_id
            )
            group.student_leaves_course()
            app.update_places(course_id, 1)
            with self.assertRaises(IntegrityError):
                app.repository.save(group)

            # Can get limited perspective on an enduring object - named changed.
            student = app.get_student(
                student_id, types=[Student.Registered, Student.NameUpdated]
            )
            self.assertEqual(student.name, "Bob")

            # Slightly dodgy, have old values, this was changed to 1.
            self.assertEqual(student.max_courses, 4)

            # Can update name.
            student.update_name("Bobby")

            # Can't update max_courses - decision type not in cb.
            with self.assertRaises(IntegrityError):
                student.update_max_courses(max_courses=1000)

            # Can get limited perspective on an enduring object - max courses.
            student = app.get_student(
                student_id, types=[Student.Registered, Student.MaxCoursesUpdated]
            )
            self.assertEqual(student.max_courses, 1)

            # Slightly dodgy, have old values, this was changed to 'Bobby'.
            self.assertEqual(student.name, "Max")

            # Can update max_courses.
            student.update_max_courses(max_courses=1000)

            # Can't update name - decision type not in cb.
            with self.assertRaises(IntegrityError):
                student.update_name(name="Jac")

            # Can operate on enduring objects in group if decision type in cb.
            group = app.repository.get_group(
                StudentAndCourseLimitedCB, student_id, course_id
            )
            group.student.update_max_courses(100)
            app.repository.save(group)
            student = app.get_student(student_id)
            self.assertEqual(100, student.max_courses)

            # Can't operate on enduring objects in group if decision type not in cb.
            group = app.repository.get_group(
                StudentAndCourseLimitedCB, student_id, course_id
            )
            with self.assertRaises(IntegrityError):
                group.student.update_name("Jac")


del EnrolmentTestCase
