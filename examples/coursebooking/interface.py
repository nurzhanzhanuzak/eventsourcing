from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager


class EnrolmentInterface(AbstractContextManager["EnrolmentInterface"], ABC):
    def __init__(self, env: dict[str, str] | None = None) -> None:
        super().__init__()

    @abstractmethod
    def register_student(self, name: str, max_courses: int) -> str: ...

    @abstractmethod
    def register_course(self, name: str, places: int) -> str: ...

    @abstractmethod
    def join_course(self, student_id: str, course_id: str) -> None: ...

    @abstractmethod
    def list_students_for_course(self, course_id: str) -> list[str]: ...

    @abstractmethod
    def list_courses_for_student(self, student_id: str) -> list[str]: ...


class TooManyCoursesError(Exception):
    pass


class FullyBookedError(Exception):
    pass


class AlreadyJoinedError(Exception):
    pass


class NotAlreadyJoinedError(Exception):
    pass


class StudentNotFoundError(Exception):
    pass


class CourseNotFoundError(Exception):
    pass
