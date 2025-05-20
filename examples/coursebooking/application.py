from __future__ import annotations

from eventsourcing.application import AggregateNotFoundError, Application
from examples.coursebooking.domainmodel import Course, Student
from examples.coursebooking.interface import CourseNotFoundError, StudentNotFoundError


class Enrolment(Application[str]):
    def register_student(self, name: str, max_courses: int) -> str:
        student = Student(name, max_courses=max_courses)
        self.save(student)
        return student.id

    def register_course(self, name: str, places: int) -> str:
        course = Course(name, places=places)
        self.save(course)
        return course.id

    def join_course(self, course_id: str, student_id: str) -> None:
        course = self.get_course(course_id)
        student = self.get_student(student_id)
        course.accept_student(student_id)
        student.join_course(course_id)
        self.save(course, student)

    def list_students_for_course(self, course_id: str) -> list[str]:
        return [
            self.get_student(s).name for s in self.get_course(course_id).student_ids
        ]

    def list_courses_for_student(self, student_id: str) -> list[str]:
        return [
            self.get_course(s).name for s in self.get_student(student_id).course_ids
        ]

    def get_student(self, student_id: str) -> Student:
        try:
            return self.repository.get(student_id)
        except AggregateNotFoundError:
            raise StudentNotFoundError from None

    def get_course(self, course_id: str) -> Course:
        try:
            return self.repository.get(course_id)
        except AggregateNotFoundError:
            raise CourseNotFoundError from None
