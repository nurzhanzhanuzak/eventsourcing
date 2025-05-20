from __future__ import annotations

from typing import Protocol


class EnrolmentProtocol(Protocol):
    def register_student(self, name: str, max_courses: int) -> str:
        raise NotImplementedError

    def register_course(self, name: str, places: int) -> str:
        raise NotImplementedError

    def join_course(self, course_id: str, student_id: str) -> None:
        raise NotImplementedError

    def list_students_for_course(self, course_id: str) -> list[str]:
        raise NotImplementedError

    def list_courses_for_student(self, student_id: str) -> list[str]:
        raise NotImplementedError


class TooManyCoursesError(Exception):
    pass


class FullyBookedError(Exception):
    pass


class AlreadyJoinedError(Exception):
    pass


class StudentNotFoundError(Exception):
    pass


class CourseNotFoundError(Exception):
    pass
