from __future__ import annotations

from uuid import uuid4

from eventsourcing.domain import event
from examples.aggregate11.domainmodel import Aggregate
from examples.coursebooking.interface import (
    AlreadyJoinedError,
    FullyBookedError,
    TooManyCoursesError,
)


class Student(Aggregate):
    @staticmethod
    def create_id() -> str:
        return "student-" + str(uuid4())

    def __init__(self, name: str, max_courses: int) -> None:
        self.name = name
        self.max_courses = max_courses
        self.course_ids: list[str] = []

    @event("CourseJoined")
    def join_course(self, course_id: str) -> None:
        if len(self.course_ids) >= self.max_courses:
            raise TooManyCoursesError
        self.course_ids.append(course_id)


class Course(Aggregate):
    @staticmethod
    def create_id() -> str:
        return "course-" + str(uuid4())

    def __init__(self, name: str, places: int) -> None:
        self.name = name
        self.places = places
        self.student_ids: list[str] = []

    @event("StudentAccepted")
    def accept_student(self, student_id: str) -> None:
        if len(self.student_ids) >= self.places:
            raise FullyBookedError
        if student_id in self.student_ids:
            raise AlreadyJoinedError
        self.student_ids.append(student_id)
