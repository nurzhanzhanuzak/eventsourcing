from unittest import TestCase

from examples.coursebookingdcbrefactored.eventstore import Decision, Mapper

# TODO: Actually test the event store independently of the example application.


class StudentRegistered(Decision):
    name: str
    max_courses: int


class TestMapper(TestCase):
    def test_mapper(self) -> None:
        mapper = Mapper()

        event = StudentRegistered(
            tags=["student-1"],
            name="Sara",
            max_courses=2,
        )

        dcb_event = mapper.to_dcb_event(event)

        self.assertTrue(dcb_event.type.endswith("StudentRegistered"), dcb_event.type)
        self.assertTrue(dcb_event.tags, dcb_event.type)

        copy = mapper.to_domain_event(dcb_event)
        assert isinstance(copy, StudentRegistered)  # for mypy

        self.assertIsInstance(copy, StudentRegistered)

        self.assertEqual(copy.tags, event.tags)
        self.assertEqual(copy.name, event.name)
        self.assertEqual(copy.max_courses, event.max_courses)
