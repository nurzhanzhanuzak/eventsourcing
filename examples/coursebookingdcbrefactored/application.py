from __future__ import annotations

from typing import Any, cast

import msgspec

from eventsourcing.dcb.api import DCBEvent
from eventsourcing.dcb.application import (
    DCBApplication,
)
from eventsourcing.dcb.domain import (
    CanInitialiseEnduringObject,
    CanMutateEnduringObject,
    EnduringObject,
    Group,
)
from eventsourcing.dcb.persistence import (
    DCBEventStore,
    DCBMapper,
    DCBRepository,
)
from eventsourcing.domain import event
from eventsourcing.utils import get_topic, resolve_topic
from examples.coursebooking.interface import (
    AlreadyJoinedError,
    CourseNotFoundError,
    Enrolment,
    FullyBookedError,
    NotAlreadyJoinedError,
    StudentNotFoundError,
    TooManyCoursesError,
)


class Decision(msgspec.Struct, CanMutateEnduringObject):
    tags: list[str]

    def _as_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self.__struct_fields__}


class MsgspecStructMapper(DCBMapper):
    def to_dcb_event(self, event: CanMutateEnduringObject) -> DCBEvent:
        return DCBEvent(
            type=get_topic(type(event)),
            data=msgspec.msgpack.encode(event),
            tags=event.tags,
        )

    def to_domain_event(self, event: DCBEvent) -> CanMutateEnduringObject:
        return msgspec.msgpack.decode(
            event.data,
            type=resolve_topic(event.type),
        )


class InitialDecision(Decision, CanInitialiseEnduringObject):
    originator_topic: str


class StudentJoinedCourse(Decision):
    student_id: str
    course_id: str


class StudentLeftCourse(Decision):
    student_id: str
    course_id: str


class Student(EnduringObject):
    class Registered(InitialDecision):
        student_id: str
        name: str
        max_courses: int

    class NameUpdated(Decision):
        name: str

    class MaxCoursesUpdated(Decision):
        max_courses: int

    def __init__(self, name: str, max_courses: int) -> None:
        self.name = name
        self.max_courses = max_courses
        self.course_ids: list[str] = []

    @event(NameUpdated)
    def update_name(self, name: str) -> None:
        self.name = name

    @event(MaxCoursesUpdated)
    def update_max_courses(self, max_courses: int) -> None:
        self.max_courses = max_courses

    @event(StudentJoinedCourse)
    def _(self, course_id: str) -> None:
        if len(self.course_ids) >= self.max_courses:
            raise TooManyCoursesError
        self.course_ids.append(course_id)

    @event(StudentLeftCourse)
    def _(self, course_id: str) -> None:
        self.course_ids.remove(course_id)


class Course(EnduringObject):
    class Registered(InitialDecision):
        course_id: str
        name: str
        places: int

    class NameUpdated(Decision):
        name: str

    class PlacesUpdated(Decision):
        places: int

    def __init__(self, name: str, places: int) -> None:
        self.name = name
        self.places = places
        self.student_ids: list[str] = []

    @event(NameUpdated)
    def update_name(self, name: str) -> None:
        self.name = name

    @event(PlacesUpdated)
    def update_places(self, places: int) -> None:
        self.places = places

    @event(StudentJoinedCourse)
    def _(self, student_id: str) -> None:
        if student_id in self.student_ids:
            raise AlreadyJoinedError
        if len(self.student_ids) >= self.places:
            raise FullyBookedError
        self.student_ids.append(student_id)

    @event(StudentLeftCourse)
    def _(self, student_id: str) -> None:
        if student_id not in self.student_ids:
            raise NotAlreadyJoinedError
        self.student_ids.remove(student_id)


class StudentAndCourse(Group):
    def __init__(
        self,
        student: Student | None,
        course: Course | None,
    ) -> None:
        self.student = student
        self.course = course

    def student_joins_course(self) -> None:
        if self.course is None:
            raise CourseNotFoundError
        if self.student is None:
            raise StudentNotFoundError

        # The DCB magic: one event for "one fact".
        self.trigger_event(
            StudentJoinedCourse,
            student_id=self.student.id,
            course_id=self.course.id,
        )

    def student_leaves_course(self) -> None:
        if self.course is None:
            raise CourseNotFoundError
        if self.student is None:
            raise StudentNotFoundError

        # The DCB magic: one event for "one fact".
        self.trigger_event(
            StudentLeftCourse,
            student_id=self.student.id,
            course_id=self.course.id,
        )


class EnrolmentWithDCBRefactored(DCBApplication, Enrolment):
    def __init__(self, env: dict[str, str]):
        super().__init__(env=env)
        self.events = DCBEventStore(MsgspecStructMapper(), self.recorder)
        self.repository = DCBRepository(self.events)

    def register_student(self, name: str, max_courses: int) -> str:
        student = Student(name=name, max_courses=max_courses)
        self.repository.save(student)
        return student.id

    def register_course(self, name: str, places: int) -> str:
        course = Course(name=name, places=places)
        self.repository.save(course)
        return course.id

    def join_course(self, student_id: str, course_id: str) -> None:
        group = self.repository.get_group(StudentAndCourse, student_id, course_id)
        group.student_joins_course()
        self.repository.save(group)

    def leave_course(self, student_id: str, course_id: str) -> None:
        group = self.repository.get_group(StudentAndCourse, student_id, course_id)
        group.student_leaves_course()
        self.repository.save(group)

    def list_students_for_course(self, course_id: str) -> list[str]:
        course = self.get_course(course_id)
        students = self.repository.get_many(*course.student_ids)
        return [cast(Student, c).name for c in students if c is not None]

    def update_student_name(self, student_id: str, name: str) -> None:
        student = self.get_student(student_id)
        student.update_name(name)
        self.repository.save(student)

    def update_course_places(self, course_id: str, max_courses: int) -> None:
        course = self.get_course(course_id)
        course.update_places(max_courses)
        self.repository.save(course)

    def list_courses_for_student(self, student_id: str) -> list[str]:
        student = self.get_student(student_id)
        courses = self.repository.get_many(*student.course_ids)
        return [cast(Course, c).name for c in courses if c is not None]

    def update_course_name(self, course_id: str, name: str) -> None:
        course = self.get_course(course_id)
        course.update_name(name)
        self.repository.save(course)

    def update_student_max_courses(self, student_id: str, max_courses: int) -> None:
        student = self.get_student(student_id)
        student.update_max_courses(max_courses)
        self.repository.save(student)

    def get_student(self, student_id: str) -> Student:
        return cast(Student, self.repository.get(student_id))

    def get_course(self, course_id: str) -> Course:
        return cast(Course, self.repository.get(course_id))
