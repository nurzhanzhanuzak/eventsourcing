from __future__ import annotations

from typing import cast, Union

from examples.coursebooking.interface import (
    AlreadyJoinedError,
    CourseNotFoundError,
    Enrolment,
    FullyBookedError,
    StudentNotFoundError,
    TooManyCoursesError,
)
from examples.coursebookingdcbrefactored.eventstore import (
    DomainEvent,
    EnduringObject,
    EventStore,
    InitEvent,
    Mapper,
    Repository,
    Selector,
)
from examples.dcb.application import (
    DCBApplication,
)


class Student(EnduringObject):
    def __init__(self, name: str, max_courses: int) -> None:
        self.name = name
        self.max_courses = max_courses
        self.course_ids: list[str] = []

    class Registered(InitEvent):
        student_id: str
        name: str
        max_courses: int

    class NameUpdated(DomainEvent):
        name: str

        def apply(self, obj: Student) -> None:
            obj.name = self.name

    class MaxCoursesUpdated(DomainEvent):
        max_courses: int

        def apply(self, obj: Student) -> None:
            obj.max_courses = self.max_courses

    def update_name(self, name: str) -> None:
        self.trigger_event(self.NameUpdated, name=name)

    def update_max_courses(self, max_courses: int) -> None:
        self.trigger_event(self.MaxCoursesUpdated, max_courses=max_courses)


# class Student(EnduringObject):
#     name: str
#     max_courses: int
#
#     class Registered(InitEvent):
#         student_id: str
#         name: str
#         max_courses: int
#
#     class NameUpdated(DomainEvent):
#         name: str
#
#     class MaxCoursesUpdated(DomainEvent):
#         max_courses: int
#
#     @event(NameUpdated)
#     def update_name(self, name: str) -> None:
#         self.name = name
#
#     @event(MaxCoursesUpdated)
#     def update_max_courses(self, max_courses: int) -> None:
#         self.max_courses = max_courses


class Course(EnduringObject):
    def __init__(self, name: str, places: int) -> None:
        self.name = name
        self.places = places
        self.student_ids: list[str] = []


    class Registered(InitEvent):
        course_id: str
        name: str
        places: int


class StudentJoinedCourse(DomainEvent):
    student_id: str
    course_id: str

    def apply(self, obj: Course | Student) -> None:
        if isinstance(obj, Student):
            obj.course_ids.append(self.course_id)
        elif isinstance(obj, Course):
            obj.student_ids.append(self.student_id)


class EnrolmentWithDCBRefactored(DCBApplication, Enrolment):
    def __init__(self, env: dict[str, str]):
        super().__init__(env=env)
        self.events = EventStore(Mapper(), self.recorder)
        self.repository = Repository(self.events)

    def register_student(self, name: str, max_courses: int) -> str:
        student = Student(name=name, max_courses=max_courses)
        self.repository.save(student)
        return student.id

    def update_student_name(self, student_id: str, name: str) -> None:
        student = self.get_student(student_id)
        student.update_name(name)
        self.repository.save(student)

    def update_student_max_courses(self, student_id: str, max_courses: int) -> None:
        student = self.get_student(student_id)
        student.update_max_courses(max_courses)
        self.repository.save(student)

    def register_course(self, name: str, places: int) -> str:
        course = Course(name=name, places=places)
        self.repository.save(course)
        return course.id

    def join_course(self, course_id: str, student_id: str) -> None:
        course, student = cast(
            tuple[Union[Course, None], Union[Student, None]],
            self.repository.get_many(course_id, student_id)
        )
        if course is None:
            raise CourseNotFoundError
        if student is None:
            raise StudentNotFoundError
        if student.id in course.student_ids:
            raise AlreadyJoinedError
        if len(student.course_ids) >= student.max_courses:
            raise TooManyCoursesError
        if len(course.student_ids) >= course.places:
            raise FullyBookedError

        # The DCB magic: one event for "one fact".
        student_joined_course = StudentJoinedCourse(
            student_id=student_id,
            course_id=course_id,
            tags=[student_id, course_id],
        )

        self.events.put(
            student_joined_course,
            cb=[*student.cb, *course.cb],
            after=course.last_known_position,
        )

    def list_students_for_course(self, course_id: str) -> list[str]:
        course = self.get_course(course_id)
        students = self.repository.get_many(*course.student_ids)
        return [cast(Student, c).name for c in students if c is not None]

    def list_courses_for_student(self, student_id: str) -> list[str]:
        student = self.get_student(student_id)
        courses = self.repository.get_many(*student.course_ids)
        return [cast(Course, c).name for c in courses if c is not None]

    def get_student(self, student_id: str) -> Student:
        return cast(Student, self.repository.get(student_id))

    def get_course(self, course_id: str) -> Course:
        return cast(Course, self.repository.get(course_id))
