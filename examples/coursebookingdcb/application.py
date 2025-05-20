from __future__ import annotations

import json
from typing import cast
from uuid import uuid4

from examples.coursebooking.interface import (
    AlreadyJoinedError,
    CourseNotFoundError,
    FullyBookedError,
    StudentNotFoundError,
    TooManyCoursesError,
)
from tests.dcb_tests.api import DCBAppendCondition, DCBEvent, DCBQuery, DCBQueryItem
from tests.dcb_tests.application import DCBApplication


class EnrolmentWithDCB(DCBApplication):
    def register_student(self, name: str, max_courses: int) -> str:
        student_id = f"student-{uuid4()}"
        consistency_boundary = DCBQuery(
            items=[DCBQueryItem(tags=[student_id])],
        )
        student_registered = DCBEvent(
            type="StudentRegistered",
            data=json.dumps({"name": name, "max_courses": max_courses}).encode(),
            tags=[student_id],
        )
        self.recorder.append(
            events=[student_registered],
            condition=DCBAppendCondition(
                fail_if_events_match=consistency_boundary,
            ),
        )
        return student_id

    def register_course(self, name: str, places: int) -> str:
        course_id = f"course-{uuid4()}"
        course_registered = DCBEvent(
            type="CourseRegistered",
            data=json.dumps({"name": name, "places": places}).encode(),
            tags=[course_id],
        )
        consistency_boundary = DCBQuery(
            items=[DCBQueryItem(tags=[course_id])],
        )
        self.recorder.append(
            events=[course_registered],
            condition=DCBAppendCondition(
                fail_if_events_match=consistency_boundary,
            ),
        )
        return course_id

    def join_course(self, course_id: str, student_id: str) -> None:
        # Decide the consistency boundary.
        consistency_boundary = DCBQuery(
            items=[
                DCBQueryItem(
                    types=["StudentRegistered", "StudentJoinedCourse"],
                    tags=[student_id],
                ),
                DCBQueryItem(
                    types=["CourseRegistered", "StudentJoinedCourse"],
                    tags=[course_id],
                ),
            ]
        )

        # Select relevant events.
        sequence = self.recorder.get(query=consistency_boundary)

        # Project the events so we can make a joining decision.
        max_courses: int | None = None
        places: int | None = None
        count_courses: int = 0
        count_students: int = 0
        last_known_position = max(s.position for s in sequence) if sequence else None
        for sequenced in sequence:
            if sequenced.event.type == "CourseRegistered":
                data = json.loads(sequenced.event.data.decode())
                places = cast(int, data["places"])
            elif sequenced.event.type == "StudentRegistered":
                data = json.loads(sequenced.event.data.decode())
                max_courses = cast(int, data["max_courses"])
            elif sequenced.event.type == "StudentJoinedCourse":
                if (
                    student_id in sequenced.event.tags
                    and course_id in sequenced.event.tags
                ):
                    raise AlreadyJoinedError
                if student_id in sequenced.event.tags:
                    count_courses += 1
                if course_id in sequenced.event.tags:
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
        student_joined_course = DCBEvent(
            type="StudentJoinedCourse",
            data=b"",
            tags=[student_id, course_id],
        )

        # Append using the same consistency boundary as the fail condition.
        self.recorder.append(
            events=[student_joined_course],
            condition=DCBAppendCondition(
                fail_if_events_match=consistency_boundary,
                after=last_known_position,
            ),
        )

    def list_students_for_course(self, course_id: str) -> list[str]:
        # Get events relevant for a list of course student IDs.
        course_students_consistency_boundary = DCBQuery(
            items=[
                DCBQueryItem(
                    types=["StudentJoinedCourse"],
                    tags=[course_id],
                ),
            ]
        )
        sequence = self.recorder.get(query=course_students_consistency_boundary)

        # Project the events into a mapping of student IDs to names.
        student_names: dict[str, str] = {}
        for sequenced in sequence:
            if sequenced.event.type == "StudentJoinedCourse":
                for tag in sequenced.event.tags:
                    if tag.startswith("student-"):
                        student_names[tag] = ""

        # Get events relevant for the student names.
        student_names_consistency_boundary = DCBQuery(
            items=[
                DCBQueryItem(
                    types=["StudentRegistered"],
                    tags=[student_id],
                )
                for student_id in student_names
            ]
        )

        # Project the events into the mapping of IDs to names.
        for s in self.recorder.get(query=student_names_consistency_boundary):
            if s.event.type == "StudentRegistered":
                name = cast(str, json.loads(s.event.data.decode())["name"])
                student_names[s.event.tags[0]] = name

        # Return the names.
        return list(student_names.values())

    def list_courses_for_student(self, student_id: str) -> list[str]:
        # Get events relevant for a list of course student IDs.
        student_courses_consistency_boundary = DCBQuery(
            items=[
                DCBQueryItem(
                    types=["StudentJoinedCourse"],
                    tags=[student_id],
                ),
            ]
        )
        sequence = self.recorder.get(query=student_courses_consistency_boundary)

        # Project the events into a mapping of course IDs to names.
        course_names: dict[str, str] = {}
        for sequenced in sequence:
            if sequenced.event.type == "StudentJoinedCourse":
                for tag in sequenced.event.tags:
                    if tag.startswith("course-"):
                        course_names[tag] = ""

        # Get events relevant for the course names.
        course_names_consistency_boundary = DCBQuery(
            items=[
                DCBQueryItem(
                    types=["CourseRegistered"],
                    tags=[course_id],
                )
                for course_id in course_names
            ]
        )

        # Project the events into the mapping of IDs to names.
        for s in self.recorder.get(query=course_names_consistency_boundary):
            if s.event.type == "CourseRegistered":
                name = cast(str, json.loads(s.event.data.decode())["name"])
                course_names[s.event.tags[0]] = name

        # Return the names.
        return list(course_names.values())
