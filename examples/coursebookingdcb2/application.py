from __future__ import annotations

from uuid import uuid4

from examples.coursebookingdcb2.mapper import DomainEvent, EventStore, Mapper, Selector
from tests.dcb_tests.application import (
    DCBApplication,
)


class StudentRegistered(DomainEvent):
    name: str
    max_courses: int


class CourseRegistered(DomainEvent):
    name: str
    places: int


class StudentJoinedCourse(DomainEvent):
    pass


class Enrolment(DCBApplication):
    def __init__(self, env: dict[str, str]):
        super().__init__(env=env)
        self.events = EventStore(Mapper(), self.recorder)

    def register_student(self, name: str, max_courses: int) -> str:
        student_id = f"student-{uuid4()}"
        event = StudentRegistered(tags=[student_id], name=name, max_courses=max_courses)
        self.events.put(event)
        return student_id

    def register_course(self, name: str, places: int) -> str:
        course_id = f"course-{uuid4()}"
        event = CourseRegistered(tags=[course_id], name=name, places=places)
        self.events.put(event)
        return course_id

    def join_course(self, course_id: str, student_id: str) -> None:
        # Decide the consistency boundary.
        cb = [
            Selector(types=[StudentRegistered, StudentJoinedCourse], tags=[student_id]),
            Selector(types=[CourseRegistered, StudentJoinedCourse], tags=[course_id]),
        ]

        # Select relevant events.
        sequence, last_position = self.events.get(cb=cb, with_last_position=True)

        # Project the events so we can make a joining decision.
        max_courses: int | None = None
        places: int | None = None
        count_courses: int = 0
        count_students: int = 0
        for event in sequence:
            if isinstance(event, CourseRegistered):
                places = event.places
            elif isinstance(event, StudentRegistered):
                max_courses = event.max_courses
            elif isinstance(event, StudentJoinedCourse):
                if student_id in event.tags and course_id in event.tags:
                    raise AlreadyJoinedError
                if student_id in event.tags:
                    count_courses += 1
                if course_id in event.tags:
                    count_students += 1

        # Check we have a student and a course, and the
        # course isn't full and the student isn't too busy.
        if max_courses is None:
            raise StudentNotFoundError
        if places is None:
            raise CourseNotFoundError
        if count_courses >= max_courses:
            raise TooManyCoursesError
        if count_students >= places:
            raise FullyBookedError

        # The DCB magic: one event for "one fact".
        student_joined_course = StudentJoinedCourse(
            tags=[student_id, course_id],
        )

        # Append using the same consistency boundary as the fail condition.
        self.events.put(student_joined_course, cb=cb, after=last_position)

    def list_students_for_course(self, course_id: str) -> list[str]:
        # Get events relevant to identify students for course.
        sequence = self.events.get(
            Selector(types=[StudentJoinedCourse], tags=[course_id])
        )

        # Project the events into a list of student IDs.
        ids: list[str] = []
        for event in sequence:
            if isinstance(event, StudentJoinedCourse):
                ids.extend([t for t in event.tags if t.startswith("student-")])

        # Get events relevant for the student names.
        sequence = self.events.get(
            [
                Selector(types=[StudentRegistered], tags=[student_id])
                for student_id in ids
            ]
        )

        # Project the events into a mapping of student IDs to names.
        names: dict[str, str] = dict.fromkeys(ids, "")
        for event in sequence:
            if isinstance(event, StudentRegistered):
                names[event.tags[0]] = event.name

        # Return the names.
        return [name for name in names.values() if name]

    def list_courses_for_student(self, student_id: str) -> list[str]:
        # Get events relevant to identify courses for student.
        sequence = self.events.get(
            Selector(types=[StudentJoinedCourse], tags=[student_id])
        )

        # Project the events into a list of course IDs.
        ids: list[str] = []
        for event in sequence:
            if isinstance(event, StudentJoinedCourse):
                ids.extend([t for t in event.tags if t.startswith("course-")])

        # Get events relevant for the course names.
        sequence = self.events.get(
            [Selector(types=[CourseRegistered], tags=[course_id]) for course_id in ids]
        )

        # Project the events into a mapping of course IDs to names.
        names: dict[str, str] = dict.fromkeys(ids, "")
        for event in sequence:
            if isinstance(event, CourseRegistered):
                names[event.tags[0]] = event.name

        # Return the names.
        return [name for name in names.values() if name]


class AlreadyJoinedError(Exception):
    pass


class TooManyCoursesError(Exception):
    pass


class FullyBookedError(Exception):
    pass


class StudentNotFoundError(Exception):
    pass


class CourseNotFoundError(Exception):
    pass
