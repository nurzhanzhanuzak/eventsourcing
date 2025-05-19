from __future__ import annotations

from typing import TYPE_CHECKING

from eventsourcing.domain import Aggregate, event

if TYPE_CHECKING:
    from uuid import UUID


class Student(Aggregate):
    def __init__(self, name: str, max_courses: int) -> None:
        self.name = name
        self.max_courses = max_courses
        self.courses: list[UUID] = []

    @event("CourseJoined")
    def join_course(self, course_id: UUID) -> None:
        if len(self.courses) >= self.max_courses:
            raise TooManyCoursesError
        self.courses.append(course_id)


class Course(Aggregate):
    def __init__(self, name: str, places: int) -> None:
        self.name = name
        self.places = places
        self.students: list[UUID] = []

    @event("StudentAccepted")
    def accept_student(self, student_id: UUID) -> None:
        if len(self.students) >= self.places:
            raise FullyBookedError
        if student_id in self.students:
            raise AlreadyJoinedError
        self.students.append(student_id)


class TooManyCoursesError(Exception):
    pass


class FullyBookedError(Exception):
    pass


class AlreadyJoinedError(Exception):
    pass
