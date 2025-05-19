from __future__ import annotations

from typing import cast
from uuid import uuid4

from tests.dcb_tests.api import DCBAppendCondition, DCBEvent, DCBQuery, DCBQueryItem
from tests.dcb_tests.application import DCBApplication


class Enrolment(DCBApplication):
    def register_student(self, name: str, max_courses: int) -> str:
        student_id = f"student-{uuid4()}"
        consistency_boundary = DCBAppendCondition(
            fail_if_events_match=DCBQuery(
                items=[DCBQueryItem(tags=[student_id])],
            )
        )
        student_registered = DCBEvent(
            type="StudentRegistered",
            data=self.transcoder.encode({"name": name, "max_courses": max_courses}),
            tags=[student_id],
        )
        self.events.append(
            events=[student_registered],
            condition=consistency_boundary,
        )
        return student_id

    def register_course(self, name: str, places: int) -> str:
        course_id = f"course-{uuid4()}"
        course_registered = DCBEvent(
            type="CourseRegistered",
            data=self.transcoder.encode({"name": name, "places": places}),
            tags=[course_id],
        )
        consistency_boundary = DCBAppendCondition(
            fail_if_events_match=DCBQuery(
                items=[DCBQueryItem(tags=[course_id])],
            )
        )
        self.events.append(
            events=[course_registered],
            condition=consistency_boundary,
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
        sequence = self.events.get(query=consistency_boundary)

        # Project the events so we can make a joining decision.
        max_courses: int | None = None
        places: int | None = None
        count_courses: int = 0
        count_students: int = 0
        last_known_position = max(s.position for s in sequence) if sequence else None
        for sequenced in sequence:
            if sequenced.event.type == "CourseRegistered":
                data = self.transcoder.decode(sequenced.event.data)
                places = cast(int, data["places"])
            elif sequenced.event.type == "StudentRegistered":
                data = self.transcoder.decode(sequenced.event.data)
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
        self.events.append(
            events=[student_joined_course],
            condition=DCBAppendCondition(
                fail_if_events_match=consistency_boundary,
                after=last_known_position,
            ),
        )

    def list_students_for_course(self, course_id: str) -> list[str]:
        consistency_boundary = DCBQuery(
            items=[
                DCBQueryItem(
                    types=["StudentJoinedCourse"],
                    tags=[course_id],
                ),
            ]
        )
        sequence = self.events.get(query=consistency_boundary)
        student_names: list[str] = []
        for sequenced in sequence:
            tags = sequenced.event.tags
            tags.remove(course_id)
            query = DCBQuery(
                items=[
                    DCBQueryItem(
                        types=["StudentRegistered"],
                        tags=tags,
                    ),
                ]
            )
            for s in self.events.get(query=query):
                name = self.transcoder.decode(s.event.data)["name"]
                student_names.append(name)
        return student_names

    def list_courses_for_student(self, student_id: str) -> list[str]:
        consistency_boundary = DCBQuery(
            items=[
                DCBQueryItem(
                    types=["StudentJoinedCourse"],
                    tags=[student_id],
                ),
            ]
        )
        sequence = self.events.get(query=consistency_boundary)
        course_names: list[str] = []
        for sequenced in sequence:
            tags = sequenced.event.tags
            tags.remove(student_id)
            query = DCBQuery(
                items=[
                    DCBQueryItem(
                        types=["CourseRegistered"],
                        tags=tags,
                    ),
                ]
            )
            for s in self.events.get(query=query):
                name = self.transcoder.decode(s.event.data)["name"]
                course_names.append(name)
        return course_names


class AlreadyJoinedError(Exception):
    pass


class TooManyCoursesError(Exception):
    pass


class FullyBookedError(Exception):
    pass


class StudentNotFoundError(Exception):
    pass


class CourseNotFoundError(Exception):
    pass
