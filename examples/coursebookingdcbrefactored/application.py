from __future__ import annotations

from typing import cast

from eventsourcing.domain import event
from examples.coursebooking.interface import (
    AlreadyJoinedError,
    CourseNotFoundError,
    Enrolment,
    FullyBookedError,
    NotAlreadyJoinedError,
    StudentNotFoundError,
    TooManyCoursesError,
)
from examples.coursebookingdcbrefactored.eventstore import (
    EnduringObject,
    EventStore,
    Group,
    Repository,
    StructDecision,
    StructInitialised,
    StructMapper,
)
from examples.dcb.application import (
    DCBApplication,
)


class Decision(StructDecision):
    pass


class Initialised(StructInitialised):
    pass


class Student(EnduringObject):
    def __init__(self, name: str, max_courses: int) -> None:
        self.name = name
        self.max_courses = max_courses
        self.course_ids: list[str] = []

    class Registered(Initialised):
        student_id: str
        name: str
        max_courses: int

    class NameUpdated(Decision):
        name: str

    class MaxCoursesUpdated(Decision):
        max_courses: int

    @event(NameUpdated)
    def update_name(self, name: str) -> None:
        self.name = name

    @event(MaxCoursesUpdated)
    def update_max_courses(self, max_courses: int) -> None:
        self.max_courses = max_courses


class Course(EnduringObject):
    def __init__(self, name: str, places: int) -> None:
        self.name = name
        self.places = places
        self.student_ids: list[str] = []

    class Registered(Initialised):
        course_id: str
        name: str
        places: int


class StudentJoinedCourse(Decision):
    student_id: str
    course_id: str

    def apply(self, obj: Course | Student) -> None:
        if isinstance(obj, Student):
            obj.course_ids.append(self.course_id)
        elif isinstance(obj, Course):
            obj.student_ids.append(self.student_id)


class StudentLeftCourse(Decision):
    student_id: str
    course_id: str

    def apply(self, obj: Course | Student) -> None:
        if isinstance(obj, Student):
            obj.course_ids.remove(self.course_id)
        elif isinstance(obj, Course):
            obj.student_ids.remove(self.student_id)


class StudentAndCourse(Group):
    def __init__(self, student: Student | None, course: Course | None) -> None:
        self.student = student
        self.course = course

    def student_joins_course(self) -> None:
        if self.course is None:
            raise CourseNotFoundError
        if self.student is None:
            raise StudentNotFoundError
        if self.student.id in self.course.student_ids:
            raise AlreadyJoinedError
        if len(self.student.course_ids) >= self.student.max_courses:
            raise TooManyCoursesError
        if len(self.course.student_ids) >= self.course.places:
            raise FullyBookedError

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
        if self.student.id not in self.course.student_ids:
            raise NotAlreadyJoinedError

        # The DCB magic: one event for "one fact".
        self.trigger_event(
            StudentLeftCourse,
            student_id=self.student.id,
            course_id=self.course.id,
        )


class EnrolmentWithDCBRefactored(DCBApplication, Enrolment):
    def __init__(self, env: dict[str, str]):
        super().__init__(env=env)
        self.events = EventStore(StructMapper(), self.recorder)
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

    def join_course(self, student_id: str, course_id: str) -> None:
        group = self.get_student_and_course(student_id, course_id)
        group.student_joins_course()
        self.repository.save(group)

    def leave_course(self, student_id: str, course_id: str) -> None:
        group = self.get_student_and_course(student_id, course_id)
        group.student_leaves_course()
        self.repository.save(group)

    def get_student_and_course(
        self, student_id: str, course_id: str
    ) -> StudentAndCourse:
        return self.repository.get_group(student_id, course_id, cls=StudentAndCourse)

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
