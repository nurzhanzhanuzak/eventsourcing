from collections.abc import Iterator

from eventsourcing.domain import datetime_now_with_tzinfo
from examples.coursebookingdcbrefactored.application import EnrolmentWithDCBRefactored

# CREATE DATABASE large_test_dcb_db;
# ALTER DATABASE large_test_dcb_db OWNER TO eventsourcing;

LARGE_DB_NAME = "large_test_dcb_db"


def infrange() -> Iterator[int]:
    i = 1
    while True:
        yield i
        i += 1


env = {}
env["PERSISTENCE_MODULE"] = "eventsourcing.postgres"
env["POSTGRES_DBNAME"] = LARGE_DB_NAME
env["POSTGRES_HOST"] = "127.0.0.1"
env["POSTGRES_PORT"] = "5432"
env["POSTGRES_USER"] = "eventsourcing"
env["POSTGRES_PASSWORD"] = "eventsourcing"  # noqa: S105
env["POSTGRES_ORIGINATOR_ID_TYPE"] = "text"

if __name__ == "__main__":
    # app = Enrolment(env=env)
    app = EnrolmentWithDCBRefactored(env=env)
    r_students = infrange()
    r_courses = infrange()

    started_script = datetime_now_with_tzinfo()
    started_loop = datetime_now_with_tzinfo()
    for i in infrange():
        # if not i % 100:
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

        loop_events = num_courses + num_students + num_students * num_students
        total_events = loop_events * (i + 1)
        loop_time = (datetime_now_with_tzinfo() - started_loop).total_seconds()
        total_time = (datetime_now_with_tzinfo() - started_script).total_seconds()
        loop_rate = loop_events / loop_time
        total_rate = total_events / total_time
        print(  # noqa: T201
            f"Iteration {i}: {total_events} total events, {loop_rate} events/s"
        )
        # print(
        #     "Iteration",
        #     i,
        #     "-",
        #     total_events,
        #     "events",
        #     loop_time,
        #     loop_rate,
        #     total_rate,
        #     "running",
        #     total_time,
        #
        # )
        started_loop = datetime_now_with_tzinfo()
