from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Any
from uuid import uuid4

import msgspec

from eventsourcing.domain import (
    BaseAggregate,
    CanInitAggregate,
    CanMutateAggregate,
    event,
)
from examples.coursebooking.interface import (
    AlreadyJoinedError,
    FullyBookedError,
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


class MsgspecStringIDAggregate(BaseAggregate[str]):
    class Event(MsgspecStringIDEvent, frozen=True):
        pass

    class Created(Event, MsgspecStringIDCreatedEvent, frozen=True):
        pass


class Aggregate(MsgspecStringIDAggregate):
    pass


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
