from __future__ import annotations

from typing import cast
from unittest import TestCase

from eventsourcing.dcb.domain import Selector
from examples.coursebookingdcbrefactored.application import Course, Student


class TestEnduringObjects(TestCase):
    def test_student(self) -> None:
        # Construct a student by calling the class.
        student = Student(name="Max", max_courses=3)

        # Check student id.
        self.assertTrue(student.id.startswith("student-"), student.id)
        self.assertGreater(len(student.id), len("student-"))

        # Check student attributes.
        self.assertEqual("Max", student.name)
        self.assertEqual(3, student.max_courses)

        # Collect events.
        new_events = student.collect_events()
        self.assertEqual(len(new_events), 1)

        # Check the event type and attributes.
        new_event1 = cast(Student.Registered, new_events[0])
        self.assertIsInstance(new_event1, Student.Registered)
        self.assertEqual(new_event1.student_id, student.id)

        # Check the event has tags.
        self.assertTrue(student.id in new_event1.tags)

        # Check init event can reconstruct enduring object.
        copy1 = cast(Student, new_event1.mutate(None))
        self.assertEqual(copy1.id, student.id)
        self.assertEqual(copy1.name, student.name)
        self.assertEqual(copy1.max_courses, student.max_courses)

        # Check the enduring object's consistency boundary.
        self.assertEqual(student.cb, [Selector(tags=[student.id])])

        # Check the name can be changed.
        student.update_name(name="Maxine")
        self.assertEqual(student.name, "Maxine")

        # Collect events.
        new_events = student.collect_events()
        self.assertEqual(len(new_events), 1)

        # Check the event type and attributes.
        new_event2 = cast(Student.NameUpdated, new_events[0])
        self.assertIsInstance(new_event2, Student.NameUpdated)
        self.assertEqual(new_event2.name, "Maxine")

        # Check the event has tags.
        self.assertTrue(student.id in new_event2.tags)

        # Check event can evolve enduring object.
        copy2 = cast(Student, new_event2.mutate(copy1))
        self.assertEqual(copy2.id, student.id)
        self.assertEqual(copy2.name, student.name)
        self.assertEqual(copy2.max_courses, student.max_courses)

    def test_course(self) -> None:
        # Construct a course by by calling the class.
        course = Course(name="Biology", places=4)

        # Check course id.
        self.assertTrue(course.id.startswith("course-"), course.id)
        self.assertGreater(len(course.id), len("course-"))

        # Check student attributes.
        self.assertEqual("Biology", course.name)
        self.assertEqual(4, course.places)

        # Collect events.
        new_events = course.collect_events()
        self.assertEqual(len(new_events), 1)

        # Check the event type and attributes.
        new_event = cast(Course.Registered, new_events[0])
        self.assertIsInstance(new_event, Course.Registered)
        self.assertEqual(new_event.course_id, course.id)

        # Check the event has tags.
        self.assertTrue(1, len(new_event.tags))
        self.assertTrue(course.id in new_event.tags)

        # Check init event can reconstruct enduring object.
        copy = cast(Course, new_event.mutate(None))
        self.assertEqual(copy.id, course.id)
        self.assertEqual(copy.name, course.name)
        self.assertEqual(copy.places, course.places)

        # Check the enduring object's consistency boundary.
        self.assertEqual(course.cb, [Selector(tags=[course.id])])
