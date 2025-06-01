from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, NewType

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Self


class EnrolmentInterface(ABC):
    def __init__(self, env: dict[str, str] | None = None) -> None:
        super().__init__()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return None  # pragma: no cover

    @abstractmethod
    def register_student(self, name: str, max_courses: int) -> StudentID: ...

    @abstractmethod
    def register_course(self, name: str, places: int) -> CourseID: ...

    @abstractmethod
    def join_course(self, student_id: StudentID, course_id: CourseID) -> None: ...

    @abstractmethod
    def list_students_for_course(self, course_id: CourseID) -> list[str]: ...

    @abstractmethod
    def list_courses_for_student(self, student_id: StudentID) -> list[str]: ...


StudentID = NewType("StudentID", str)

CourseID = NewType("CourseID", str)


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


class NotAlreadyJoinedError(Exception):
    pass
