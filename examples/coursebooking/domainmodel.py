from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Any
from uuid import uuid4

import msgspec
from typing_extensions import TypeVar

from eventsourcing.domain import (
    BaseAggregate,
    CanInitAggregate,
    CanMutateAggregate,
    event,
)
from examples.coursebooking.interface import (
    AlreadyJoinedError,
    CourseID,
    FullyBookedError,
    StudentID,
    TooManyCoursesError,
)


class DomainEvent(msgspec.Struct, frozen=True):
    originator_id: str
    originator_version: int
    timestamp: datetime


class MsgspecStringIDEvent(DomainEvent, CanMutateAggregate[str], frozen=True):
    def _as_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self.__struct_fields__}


class MsgspecStringIDCreatedEvent(DomainEvent, CanInitAggregate[str], frozen=True):
    originator_topic: str


TID = TypeVar("TID", bound=str, default=str)


class MsgspecStringIDAggregate(BaseAggregate[TID]):
    class Event(MsgspecStringIDEvent, frozen=True):
        pass

    class Created(Event, MsgspecStringIDCreatedEvent, frozen=True):
        pass


class Aggregate(MsgspecStringIDAggregate[TID]):
    pass


class Student(Aggregate[StudentID]):
    @staticmethod
    def create_id() -> StudentID:
        return StudentID("student-" + str(uuid4()))

    def __init__(self, name: str, max_courses: int) -> None:
        self.name = name
        self.max_courses = max_courses
        self.course_ids: list[CourseID] = []

    @event("CourseJoined")
    def join_course(self, course_id: CourseID) -> None:
        if len(self.course_ids) >= self.max_courses:
            raise TooManyCoursesError
        self.course_ids.append(course_id)


class Course(Aggregate[CourseID]):
    @staticmethod
    def create_id() -> CourseID:
        return CourseID("course-" + str(uuid4()))

    def __init__(self, name: str, places: int) -> None:
        self.name = name
        self.places = places
        self.student_ids: list[StudentID] = []

    @event("StudentAccepted")
    def accept_student(self, student_id: StudentID) -> None:
        if len(self.student_ids) >= self.places:
            raise FullyBookedError
        if student_id in self.student_ids:
            raise AlreadyJoinedError
        self.student_ids.append(student_id)
