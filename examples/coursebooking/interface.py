from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing_extensions import Self


class EnrolmentInterface(ABC):
    def __init__(self, env: dict[str, str] | None = None) -> None:
        super().__init__()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        return None  # pragma: no cover

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
