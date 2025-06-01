from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

import msgspec

from eventsourcing.dcb.api import DCBEvent
from eventsourcing.dcb.application import (
    DCBApplication,
)
from eventsourcing.dcb.domain import (
    EnduringObject,
    Group,
    Initialises,
    Mutates,
)
from eventsourcing.dcb.persistence import (
    DCBMapper,
)
from eventsourcing.domain import event
from eventsourcing.utils import get_topic, resolve_topic
from examples.coursebooking.interface import (
    AlreadyJoinedError,
    CourseID,
    CourseNotFoundError,
    EnrolmentInterface,
    FullyBookedError,
    NotAlreadyJoinedError,
    StudentID,
    StudentNotFoundError,
    TooManyCoursesError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


class Decision(msgspec.Struct, Mutates):
    tags: list[str]

    def _as_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self.__struct_fields__}


class MsgspecStructMapper(DCBMapper):
    def to_dcb_event(self, event: Mutates) -> DCBEvent:
        return DCBEvent(
            type=get_topic(type(event)),
            data=msgspec.msgpack.encode(event),
            tags=event.tags,
        )

    def to_domain_event(self, event: DCBEvent) -> Mutates:
        return msgspec.msgpack.decode(
            event.data,
            type=resolve_topic(event.type),
        )


class InitialDecision(Decision, Initialises):
    originator_topic: str


class StudentJoinedCourse(Decision):
    student_id: StudentID
    course_id: CourseID


class StudentLeftCourse(Decision):
    student_id: StudentID
    course_id: CourseID


class Student(EnduringObject[StudentID]):
    class Registered(InitialDecision):
        student_id: StudentID
        name: str
        max_courses: int

    class NameUpdated(Decision):
        name: str

    class MaxCoursesUpdated(Decision):
        max_courses: int

    def __init__(self, name: str, max_courses: int) -> None:
        self.name = name
        self.max_courses = max_courses
        self.course_ids: list[CourseID] = []

    @event(NameUpdated)
    def update_name(self, name: str) -> None:
        self.name = name

    @event(MaxCoursesUpdated)
    def update_max_courses(self, max_courses: int) -> None:
        self.max_courses = max_courses

    @event(StudentJoinedCourse)
    def _(self, course_id: CourseID) -> None:
        if len(self.course_ids) >= self.max_courses:
            raise TooManyCoursesError
        self.course_ids.append(course_id)

    @event(StudentLeftCourse)
    def _(self, course_id: CourseID) -> None:
        self.course_ids.remove(course_id)


class Course(EnduringObject[CourseID]):
    class Registered(InitialDecision):
        course_id: CourseID
        name: str
        places: int

    class NameUpdated(Decision):
        name: str

    class PlacesUpdated(Decision):
        places: int

    def __init__(self, name: str, places: int) -> None:
        self.name = name
        self.places = places
        self.student_ids: list[StudentID] = []

    @event(NameUpdated)
    def update_name(self, name: str) -> None:
        self.name = name

    @event(PlacesUpdated)
    def update_places(self, places: int) -> None:
        self.places = places

    @event(StudentJoinedCourse)
    def _(self, student_id: StudentID) -> None:
        if student_id in self.student_ids:
            raise AlreadyJoinedError
        if len(self.student_ids) >= self.places:
            raise FullyBookedError
        self.student_ids.append(student_id)

    @event(StudentLeftCourse)
    def _(self, student_id: StudentID) -> None:
        if student_id not in self.student_ids:
            raise NotAlreadyJoinedError
        self.student_ids.remove(student_id)


class StudentAndCourse(Group):
    cb_types = (
        Student.Registered,
        Course.Registered,
        Student.MaxCoursesUpdated,
        Course.PlacesUpdated,
        StudentJoinedCourse,
        StudentLeftCourse,
    )

    def __init__(
        self,
        student: Student | None,
        course: Course | None,
    ) -> None:
        if course is None:
            raise CourseNotFoundError
        if student is None:
            raise StudentNotFoundError
        self.student = student
        self.course = course

    def student_joins_course(self) -> None:
        # The DCB magic: one event for "one fact".
        self.trigger_event(
            StudentJoinedCourse,
            student_id=self.student.id,
            course_id=self.course.id,
        )

    def student_leaves_course(self) -> None:
        # The DCB magic: one event for "one fact".
        self.trigger_event(
            StudentLeftCourse,
            student_id=self.student.id,
            course_id=self.course.id,
        )


class EnrolmentWithDCBRefactored(DCBApplication, EnrolmentInterface):
    env: Mapping[str, str] = {"MAPPER_TOPIC": get_topic(MsgspecStructMapper)}

    def register_student(self, name: str, max_courses: int) -> StudentID:
        student = Student(name=name, max_courses=max_courses)
        self.repository.save(student)
        return student.id

    def register_course(self, name: str, places: int) -> CourseID:
        course = Course(name=name, places=places)
        self.repository.save(course)
        return course.id

    def join_course(self, student_id: StudentID, course_id: CourseID) -> None:
        group = self.repository.get_group(StudentAndCourse, student_id, course_id)
        group.student_joins_course()
        self.repository.save(group)

    def leave_course(self, student_id: StudentID, course_id: CourseID) -> None:
        group = self.repository.get_group(StudentAndCourse, student_id, course_id)
        group.student_leaves_course()
        self.repository.save(group)

    def list_students_for_course(self, course_id: CourseID) -> list[str]:
        course = self.get_course(course_id)
        students = self.repository.get_many(*course.student_ids)
        return [cast(Student, c).name for c in students if c is not None]

    def list_courses_for_student(self, student_id: StudentID) -> list[str]:
        student = self.get_student(student_id)
        courses = self.repository.get_many(*student.course_ids)
        return [cast(Course, c).name for c in courses if c is not None]

    def update_student_name(self, student_id: StudentID, name: str) -> None:
        student = self.get_student(student_id)
        student.update_name(name)
        self.repository.save(student)

    def update_max_courses(self, student_id: StudentID, max_courses: int) -> None:
        student = self.get_student(student_id)
        student.update_max_courses(max_courses)
        self.repository.save(student)

    def update_course_name(self, course_id: CourseID, name: str) -> None:
        course = self.get_course(course_id)
        course.update_name(name)
        self.repository.save(course)

    def update_places(self, course_id: CourseID, max_courses: int) -> None:
        course = self.get_course(course_id)
        course.update_places(max_courses)
        self.repository.save(course)

    def get_student(self, tag: StudentID, types: DecisionTypes = ()) -> Student:
        return cast(Student, self.repository.get(tag, types))

    def get_course(self, tag: CourseID, types: DecisionTypes = ()) -> Course:
        return cast(Course, self.repository.get(tag, types))


DecisionTypes = Sequence[type[Decision]]
