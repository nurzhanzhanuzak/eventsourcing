from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from eventsourcing.dcb.application import (
    DCBApplication,
)
from eventsourcing.dcb.domain import (
    Selector,
    Slice,
    TSlice,
)
from eventsourcing.dcb.msgspecstruct import Decision, MsgspecStructMapper
from eventsourcing.domain import event
from eventsourcing.utils import get_topic
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


class StudentJoinedCourse(Decision):
    student_id: StudentID
    course_id: CourseID


class StudentLeftCourse(Decision):
    student_id: StudentID
    course_id: CourseID


class StudentRegistered(Decision):
    student_id: StudentID
    name: str
    max_courses: int


class StudentNameUpdated(Decision):
    name: str


class StudentMaxCoursesUpdated(Decision):
    max_courses: int


class CourseRegistered(Decision):
    course_id: CourseID
    name: str
    places: int


class CourseNameUpdated(Decision):
    name: str


class CoursePlacesUpdated(Decision):
    places: int


class RegisterStudent(Slice):
    def __init__(self, name: str, max_courses: int):
        self.student_id = StudentID(f"student-{uuid4()}")
        self.name = name
        self.max_courses = max_courses

    @property
    def cb(self) -> list[Selector]:
        return [Selector(types=[StudentRegistered], tags=[self.student_id])]

    def execute(self) -> None:
        decision = StudentRegistered(
            student_id=self.student_id,
            name=self.name,
            max_courses=self.max_courses,
            tags=[self.student_id],
        )
        self.append(decision)


class UpdateStudentName(Slice):
    def __init__(self, student_id: StudentID, name: str) -> None:
        self.id = student_id
        self.name = name
        self.student_was_registered: bool = False

    @property
    def cb(self) -> list[Selector]:
        return [Selector(types=[StudentRegistered, StudentNameUpdated], tags=[self.id])]

    @event(StudentRegistered)
    def _(self) -> None:
        self.student_was_registered = True

    def execute(self) -> None:
        assert self.student_was_registered
        decision = StudentNameUpdated(tags=[self.id], name=self.name)
        self.append(decision)


class UpdateMaxCourses(Slice):
    def __init__(self, student_id: StudentID, max_courses: int) -> None:
        self.student_was_registered: bool = False
        self.id = student_id
        self.max_courses = max_courses

    @property
    def cb(self) -> list[Selector]:
        return [
            Selector(
                types=[StudentRegistered, StudentMaxCoursesUpdated], tags=[self.id]
            )
        ]

    @event(StudentRegistered)
    def _(self) -> None:
        self.student_was_registered = True

    def execute(self) -> None:
        assert self.student_was_registered
        decision = StudentMaxCoursesUpdated(
            tags=[self.id], max_courses=self.max_courses
        )
        self.append(decision)


class RegisterCourse(Slice):
    def __init__(self, name: str, places: int):
        self.course_id = CourseID(f"course-{uuid4()}")
        self.name = name
        self.places = places

    @property
    def cb(self) -> list[Selector]:
        return [Selector(types=[CourseRegistered], tags=[self.course_id])]

    def execute(self) -> None:
        decision = CourseRegistered(
            course_id=self.course_id,
            name=self.name,
            places=self.places,
            tags=[self.course_id],
        )
        self.append(decision)


class UpdateCourseName(Slice):
    def __init__(self, course_id: CourseID, name: str) -> None:
        self.id = course_id
        self.name = name
        self.course_was_registered: bool = False

    @property
    def cb(self) -> list[Selector]:
        return [Selector(types=[CourseRegistered, CourseNameUpdated], tags=[self.id])]

    @event(CourseRegistered)
    def _(self) -> None:
        self.course_was_registered = True

    def execute(self) -> None:
        assert self.course_was_registered
        decision = CourseNameUpdated(tags=[self.id], name=self.name)
        self.append(decision)


class UpdatePlaces(Slice):
    def __init__(self, course_id: CourseID, places: int) -> None:
        self.id = course_id
        self.places = places
        self.course_was_registered: bool = False

    @property
    def cb(self) -> list[Selector]:
        return [Selector(types=[CourseRegistered, CoursePlacesUpdated], tags=[self.id])]

    @event(CourseRegistered)
    def _(self) -> None:
        self.course_was_registered = True

    def execute(self) -> None:
        assert self.course_was_registered
        decision = CoursePlacesUpdated(tags=[self.id], places=self.places)
        self.append(decision)


class StudentJoinsCourse(Slice):
    def __init__(self, student_id: StudentID, course_id: CourseID) -> None:
        self.student_id = student_id
        self.course_id = course_id
        self.course_was_registered = False
        self.student_was_registered = False
        self.student_max_courses = 0
        self.course_places = 0
        self.students_on_course: list[StudentID] = []
        self.courses_for_student: list[CourseID] = []

    @property
    def cb(self) -> list[Selector]:
        return [
            Selector(
                types=[
                    StudentRegistered,
                    StudentMaxCoursesUpdated,
                    StudentJoinedCourse,
                    StudentLeftCourse,
                ],
                tags=[self.student_id],
            ),
            Selector(
                types=[
                    CourseRegistered,
                    CoursePlacesUpdated,
                    StudentJoinedCourse,
                    StudentLeftCourse,
                ],
                tags=[self.course_id],
            ),
        ]

    @event(StudentRegistered)
    def _(self, max_courses: int) -> None:
        self.student_was_registered = True
        self.student_max_courses = max_courses

    @event(CourseRegistered)
    def _(self, places: int) -> None:
        self.course_was_registered = True
        self.course_places = places

    @event(StudentJoinedCourse)
    def _(self, student_id: StudentID, course_id: CourseID) -> None:
        if student_id == self.student_id:
            self.courses_for_student.append(course_id)
        if course_id == self.course_id:
            self.students_on_course.append(student_id)

    @event(StudentLeftCourse)
    def _(self, student_id: StudentID, course_id: CourseID) -> None:
        if student_id == self.student_id:
            self.courses_for_student.remove(course_id)
        if course_id == self.course_id:
            self.students_on_course.remove(student_id)

    @event(StudentMaxCoursesUpdated)
    def _(self, max_courses: int) -> None:
        self.student_max_courses = max_courses

    @event(CoursePlacesUpdated)
    def _(self, places: int) -> None:
        self.course_places = places

    def execute(self) -> None:
        if not self.course_was_registered:
            raise CourseNotFoundError
        if not self.student_was_registered:
            raise StudentNotFoundError
        if len(self.students_on_course) >= self.course_places:
            raise FullyBookedError
        if len(self.courses_for_student) >= self.student_max_courses:
            raise TooManyCoursesError
        if self.student_id in self.students_on_course:
            raise AlreadyJoinedError
        decision = StudentJoinedCourse(
            tags=[self.student_id, self.course_id],
            student_id=self.student_id,
            course_id=self.course_id,
        )
        self.append(decision)


class StudentLeavesCourse(Slice):
    def __init__(self, student_id: StudentID, course_id: CourseID) -> None:
        self.student_id = student_id
        self.course_id = course_id
        self.course_was_registered = False
        self.student_was_registered = False
        self.students_on_course: list[StudentID] = []
        self.courses_for_student: list[CourseID] = []

    @property
    def cb(self) -> list[Selector]:
        # return [
        #     Selector(
        #         types=[StudentRegistered, StudentJoinedCourse, StudentLeftCourse],
        #         tags=[self.student_id],
        #     ),
        #     Selector(
        #         types=[CourseRegistered, StudentJoinedCourse, StudentLeftCourse],
        #         tags=[self.course_id],
        #     ),
        # ]
        return [
            Selector(types=type(self).projected_types, tags=[tag])
            for tag in [self.student_id, self.course_id]
        ]

    @event(StudentRegistered)
    def _(self) -> None:
        self.student_was_registered = True

    @event(CourseRegistered)
    def _(self) -> None:
        self.course_was_registered = True

    @event(StudentJoinedCourse)
    def _(self, student_id: StudentID, course_id: CourseID) -> None:
        if student_id == self.student_id:
            self.courses_for_student.append(course_id)
        if course_id == self.course_id:
            self.students_on_course.append(student_id)

    @event(StudentLeftCourse)
    def _(self, student_id: StudentID, course_id: CourseID) -> None:
        if student_id == self.student_id:
            self.courses_for_student.remove(course_id)
        if course_id == self.course_id:
            self.students_on_course.remove(student_id)

    def execute(self) -> None:
        if not self.course_was_registered:
            raise CourseNotFoundError
        if not self.student_was_registered:
            raise StudentNotFoundError
        if self.student_id not in self.students_on_course:
            raise NotAlreadyJoinedError
        decision = StudentLeftCourse(
            tags=[self.student_id, self.course_id],
            student_id=self.student_id,
            course_id=self.course_id,
        )
        self.append(decision)


class StudentsIDs(Slice):
    def __init__(self, course_id: CourseID) -> None:
        self.course_id = course_id
        self.student_ids: list[StudentID] = []

    @property
    def cb(self) -> list[Selector]:
        return [Selector(types=type(self).projected_types, tags=[self.course_id])]

    @event(StudentJoinedCourse)
    def _(self, student_id: StudentID) -> None:
        self.student_ids.append(student_id)


class StudentNames(Slice):
    def __init__(self, student_ids: list[StudentID]) -> None:
        self.student_id_names: dict[StudentID, str | None] = dict.fromkeys(
            student_ids, None
        )

    @property
    def cb(self) -> list[Selector]:
        return [
            # Selector(types=[StudentRegistered, StudentNameUpdated], tags=[student_id])
            Selector(types=type(self).projected_types, tags=[student_id])
            for student_id in self.student_id_names
        ]

    @event(StudentRegistered)
    def _(self, student_id: StudentID, name: str) -> None:
        self.student_id_names[student_id] = name

    @event(StudentNameUpdated)
    def _(self, tags: Sequence[str], name: str) -> None:
        for tag in tags:
            if tag in self.student_id_names:
                self.student_id_names[cast(StudentID, tag)] = name

    @property
    def names(self) -> list[str]:
        return [n for n in self.student_id_names.values() if n]


class CourseIDs(Slice):
    def __init__(self, student_id: StudentID) -> None:
        self.student_id = student_id
        self.course_ids: list[CourseID] = []

    @property
    def cb(self) -> list[Selector]:
        # return [Selector(types=[StudentJoinedCourse], tags=[self.student_id])]
        return [Selector(types=type(self).projected_types, tags=[self.student_id])]

    @event(StudentJoinedCourse)
    def _(self, course_id: CourseID) -> None:
        self.course_ids.append(course_id)


class CourseNames(Slice):
    def __init__(self, course_ids: list[CourseID]) -> None:
        self.course_id_names: dict[CourseID, str | None] = dict.fromkeys(
            course_ids, None
        )

    @property
    def cb(self) -> list[Selector]:
        return [
            # Selector(types=[CourseRegistered, CourseNameUpdated], tags=[student_id])
            Selector(types=type(self).projected_types, tags=[student_id])
            for student_id in self.course_id_names
        ]

    @event(CourseRegistered)
    def _(self, course_id: CourseID, name: str) -> None:
        self.course_id_names[course_id] = name

    @event(CourseNameUpdated)
    def _(self, tags: Sequence[str], name: str) -> None:
        for tag in tags:
            if tag in self.course_id_names:
                self.course_id_names[cast(CourseID, tag)] = name

    @property
    def names(self) -> list[str]:
        return [n for n in self.course_id_names.values() if n]


class Student(Slice):
    def __init__(self, student_id: StudentID) -> None:
        self.student_was_registered: bool = False
        self.id = student_id
        self.name: str = ""
        self.max_courses: int = 0
        self.course_ids: list[CourseID] = []

    @property
    def cb(self) -> list[Selector]:
        return [Selector(tags=[self.id])]

    @event(StudentRegistered)
    def _(self, name: str, max_courses: int) -> None:
        self.student_was_registered = True
        self.name = name
        self.max_courses = max_courses

    @event(StudentNameUpdated)
    def _(self, name: str) -> None:
        self.name = name

    @event(StudentMaxCoursesUpdated)
    def _(self, max_courses: int) -> None:
        self.max_courses = max_courses

    @event(StudentJoinedCourse)
    def _(self, course_id: CourseID) -> None:
        self.course_ids.append(course_id)

    @event(StudentLeftCourse)
    def _(self, course_id: CourseID) -> None:
        self.course_ids.remove(course_id)


class Course(Slice):
    def __init__(self, course_id: CourseID) -> None:
        self.course_was_registered: bool = False
        self.id = course_id
        self.name: str = ""
        self.places = 0
        self.student_ids: list[StudentID] = []

    @property
    def cb(self) -> list[Selector]:
        return [Selector(tags=[self.id])]

    @event(CourseRegistered)
    def _(self, name: str, places: int) -> None:
        self.student_was_registered = True
        self.name = name
        self.places = places

    @event(CourseNameUpdated)
    def _(self, name: str) -> None:
        self.name = name

    @event(CoursePlacesUpdated)
    def _(self, places: int) -> None:
        self.places = places

    @event(StudentJoinedCourse)
    def _(self, student_id: StudentID) -> None:
        self.student_ids.append(student_id)

    @event(StudentLeftCourse)
    def _(self, student_id: StudentID) -> None:
        self.student_ids.remove(student_id)


class EnrolmentWithDCBSlices(DCBApplication, EnrolmentInterface):
    env: Mapping[str, str] = {"MAPPER_TOPIC": get_topic(MsgspecStructMapper)}

    def register_student(self, name: str, max_courses: int) -> StudentID:
        return self.do(RegisterStudent(name, max_courses)).student_id

    def register_course(self, name: str, places: int) -> CourseID:
        return self.do(RegisterCourse(name, places)).course_id

    def join_course(self, student_id: StudentID, course_id: CourseID) -> None:
        self.do(StudentJoinsCourse(student_id, course_id))

    def leave_course(self, student_id: StudentID, course_id: CourseID) -> None:
        self.do(StudentLeavesCourse(student_id, course_id))

    def list_students_for_course(self, course_id: CourseID) -> list[str]:
        return self.do(StudentNames(self.do(StudentsIDs(course_id)).student_ids)).names

    def list_courses_for_student(self, student_id: StudentID) -> list[str]:
        return self.do(CourseNames(self.do(CourseIDs(student_id)).course_ids)).names

    def update_student_name(self, student_id: StudentID, name: str) -> None:
        self.do(UpdateStudentName(student_id, name))

    def update_max_courses(self, student_id: StudentID, max_courses: int) -> None:
        self.do(UpdateMaxCourses(student_id, max_courses))

    def update_course_name(self, course_id: CourseID, name: str) -> None:
        self.do(UpdateCourseName(course_id, name))

    def update_places(self, course_id: CourseID, places: int) -> None:
        self.do(UpdatePlaces(course_id, places))

    def get_student(self, student_id: StudentID) -> Student:
        return self.do(Student(student_id=student_id))

    def get_course(self, course_id: CourseID) -> Course:
        return self.do(Course(course_id=course_id))

    def do(self, s: TSlice) -> TSlice:
        s = self.repository.project_perspective(s)
        s.execute()
        if s.new_decisions:
            self.repository.save(s)
        return s


DecisionTypes = Sequence[type[Decision]]
