from __future__ import annotations

from typing import cast

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

    class Registered(InitEvent):
        course_id: str
        name: str
        places: int


class StudentJoinedCourse(DomainEvent):
    pass


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
        # Decide the consistency boundary.
        cb = [
            Selector(
                types=[Student.Registered, StudentJoinedCourse], tags=[student_id]
            ),
            Selector(types=[Course.Registered, StudentJoinedCourse], tags=[course_id]),
        ]

        # Select relevant events.
        sequence, last_position = self.events.get(cb=cb, with_last_position=True)

        # Project the events so we can make a joining decision.
        max_courses: int | None = None
        places: int | None = None
        count_courses: int = 0
        count_students: int = 0
        for event in sequence:
            if isinstance(event, Course.Registered):
                places = event.places
            elif isinstance(event, Student.Registered):
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
                Selector(types=[Student.Registered], tags=[student_id])
                for student_id in ids
            ]
        )

        # Project the events into a mapping of student IDs to names.
        names: dict[str, str] = dict.fromkeys(ids, "")
        for event in sequence:
            if isinstance(event, Student.Registered):
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
            [Selector(types=[Course.Registered], tags=[course_id]) for course_id in ids]
        )

        # Project the events into a mapping of course IDs to names.
        names: dict[str, str] = dict.fromkeys(ids, "")
        for event in sequence:
            if isinstance(event, Course.Registered):
                names[event.tags[0]] = event.name

        # Return the names.
        return [name for name in names.values() if name]

    def get_student(self, student_id: str) -> Student:
        return cast(Student, self.repository.get(student_id))

    def get_course(self, course_id: str) -> Course:
        return cast(Course, self.repository.get(course_id))
