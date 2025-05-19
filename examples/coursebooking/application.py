from __future__ import annotations

from uuid import UUID

from eventsourcing.application import AggregateNotFoundError, Application
from examples.coursebooking.domainmodel import Course, Student


class Enrolment(Application[UUID]):
    def register_student(self, name: str, max_courses: int) -> UUID:
        student = Student(name, max_courses=max_courses)
        self.save(student)
        return student.id

    def register_course(self, name: str, places: int) -> UUID:
        course = Course(name, places=places)
        self.save(course)
        return course.id

    def join_course(self, course_id: UUID, student_id: UUID) -> None:
        course = self.get_course(course_id)
        student = self.get_student(student_id)
        course.accept_student(student_id)
        student.join_course(course_id)
        self.save(course, student)

    def list_students_for_course(self, course_id: UUID) -> list[str]:
        return [self.get_student(s).name for s in self.get_course(course_id).students]

    def list_courses_for_student(self, student_id: UUID) -> list[str]:
        return [self.get_course(s).name for s in self.get_student(student_id).courses]

    def get_student(self, student_id: UUID) -> Student:
        try:
            return self.repository.get(student_id)
        except AggregateNotFoundError:
            raise StudentNotFoundError from None

    def get_course(self, course_id: UUID) -> Course:
        try:
            return self.repository.get(course_id)
        except AggregateNotFoundError:
            raise CourseNotFoundError from None


class StudentNotFoundError(AggregateNotFoundError):
    pass


class CourseNotFoundError(AggregateNotFoundError):
    pass
