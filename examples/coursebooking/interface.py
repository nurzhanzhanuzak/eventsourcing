from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager


class Enrolment(AbstractContextManager["Enrolment"], ABC):
    def __init__(self, env: dict[str, str] | None = None) -> None:
        super().__init__()

    @abstractmethod
    def register_student(self, name: str, max_courses: int) -> str:
        raise NotImplementedError

    @abstractmethod
    def register_course(self, name: str, places: int) -> str:
        raise NotImplementedError

    @abstractmethod
    def join_course(self, course_id: str, student_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def list_students_for_course(self, course_id: str) -> list[str]:
        raise NotImplementedError

    @abstractmethod
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
