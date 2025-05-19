from __future__ import annotations

from unittest import TestCase

from eventsourcing.persistence import IntegrityError
from examples.coursebooking.application import Enrolment
from examples.coursebooking.domainmodel import (
    FullyBookedError,
    TooManyCoursesError,
)


class TestEnrolment(TestCase):
    def test_enrolment(self) -> None:
        # Construct application object.
        app = Enrolment()

        # Register courses.
        dcb = app.register_course("Dynamic Consistency Boundaries", places=5)
        maths = app.register_course("Maths", places=5)
        biology = app.register_course("Biology", places=5)
        french = app.register_course("French", places=5)
        spanish = app.register_course("Spanish", places=5)

        # Register student.
        sara = app.register_student("Sara", max_courses=3)
        mollie = app.register_student("Mollie", max_courses=3)
        allard = app.register_student("Allard", max_courses=3)
        bastian = app.register_student("Bastian", max_courses=3)
        grace = app.register_student("Grace", max_courses=3)
        john = app.register_student("John", max_courses=3)
        katherine = app.register_student("Katherine", max_courses=3)

        # Fill Dynamic Consistency Boundaries course.
        app.join_course(dcb, sara)
        app.join_course(dcb, mollie)
        app.join_course(dcb, allard)
        app.join_course(dcb, bastian)
        app.join_course(dcb, grace)

        # John can't join because the course is full.
        with self.assertRaises(FullyBookedError):
            app.join_course(dcb, john)

        # John joins other courses instead.
        app.join_course(french, john)
        app.join_course(spanish, john)
        app.join_course(maths, john)

        # John has enough to do already.
        with self.assertRaises(TooManyCoursesError):
            app.join_course(biology, john)

        # Katherine also does French.
        app.join_course(french, katherine)

        # List students for Dynamic Consistency Boundaries.
        students = app.list_students_for_course(dcb)
        self.assertEqual(students, ["Sara", "Mollie", "Allard", "Bastian", "Grace"])

        # List students for French.
        students = app.list_students_for_course(french)
        self.assertEqual(students, ["John", "Katherine"])

        # List courses for Sara.
        courses = app.list_courses_for_student(sara)
        self.assertEqual(courses, ["Dynamic Consistency Boundaries"])

        # List courses for John.
        courses = app.list_courses_for_student(john)
        self.assertEqual(courses, ["French", "Spanish", "Maths"])

        # Try to break recorded consistency with concurrent operation.
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
