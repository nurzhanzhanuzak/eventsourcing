import sys
from collections.abc import Iterator

from eventsourcing.domain import datetime_now_with_tzinfo
from examples.coursebooking.application import EnrolmentWithAggregates
from examples.coursebooking.interface import Enrolment
from examples.coursebookingdcbrefactored.application import EnrolmentWithDCBRefactored

# CREATE DATABASE large_test_dcb_db;
# ALTER DATABASE large_test_dcb_db OWNER TO eventsourcing;

env = {}
env["POSTGRES_DBNAME"] = "large_test_dcb_db"
env["POSTGRES_HOST"] = "127.0.0.1"
env["POSTGRES_PORT"] = "5432"
env["POSTGRES_USER"] = "eventsourcing"
env["POSTGRES_PASSWORD"] = "eventsourcing"  # noqa: S105
env["POSTGRES_ORIGINATOR_ID_TYPE"] = "text"


def inf_range() -> Iterator[int]:
    i = 1
    while True:
        yield i
        i += 1


if __name__ == "__main__":
    if "dcb" in sys.argv:
        env["PERSISTENCE_MODULE"] = "examples.dcb.postgres"
        app: Enrolment = EnrolmentWithDCBRefactored(env)
    elif "agg" in sys.argv:
        env["PERSISTENCE_MODULE"] = "eventsourcing.postgres"
        app = EnrolmentWithAggregates(env)
    else:
        print(f"Usage: {__file__} dcb | agg")  # noqa: T201
        sys.exit(1)

    # app = Enrolment(env=env)
    r_students = inf_range()
    r_courses = inf_range()

    started_script = datetime_now_with_tzinfo()
    started_loop = datetime_now_with_tzinfo()
    for i in inf_range():
        num_courses = 10
        num_students = 10
        students_per_course = 10
        course_ids = [
            app.register_course(f"course-{next(r_courses)}", students_per_course)
            for _ in range(num_courses)
        ]
        student_ids = [
            app.register_student(f"student-{next(r_students)}", num_courses)
            for _ in range(num_students)
        ]

        for course_id in course_ids:
            for student_id in student_ids:
                app.join_course(course_id, student_id)

        loop_ops = num_courses + num_students + num_students * num_students
        total_ops = loop_ops * (i + 1)
        loop_time = (datetime_now_with_tzinfo() - started_loop).total_seconds()
        loop_rate = loop_ops / loop_time
        total_time = datetime_now_with_tzinfo() - started_script
        print(  # noqa: T201
            f"Iteration {i}: {total_ops} ops, {int(loop_rate)} ops/s after {total_time}"
        )
        started_loop = datetime_now_with_tzinfo()
