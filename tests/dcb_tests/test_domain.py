from typing import Any
from unittest import TestCase

from eventsourcing.dcb.domain import CanInitialiseEnduringObject, EnduringObject
from eventsourcing.domain import ProgrammingError


class TestEnduringObject(TestCase):
    def test_subclass_requires_nested_initialiser(self) -> None:
        class MyObj(EnduringObject):
            pass

        with self.assertRaisesRegex(ProgrammingError, "Please define"):
            MyObj()

    def test_subclass_initialiser_attributes_must_match(self) -> None:
        class MyObj(EnduringObject):
            def __init__(self, a: str) -> None:
                self.a = a

            class Created(CanInitialiseEnduringObject):
                pass

        with self.assertRaisesRegex(
            TypeError, f"Unable to construct {MyObj.Created.__qualname__}"
        ):
            MyObj(a="a")

    def test_nice_error_when_initialiser_cannot_construct_enduring_object(self) -> None:
        class MyObj(EnduringObject):
            def __init__(self) -> None:
                pass

            class Created(CanInitialiseEnduringObject):
                def __init__(
                    self, myobj_id: str, originator_topic: str, tags: list[str], a: str
                ) -> None:
                    self.myobj_id = myobj_id
                    self.originator_topic = originator_topic
                    self.tags = tags
                    self.a = a

                def _as_dict(self) -> dict[str, Any]:
                    return self.__dict__

        with self.assertRaisesRegex(
            TypeError, f"cannot __init__ {MyObj.__qualname__} with kwargs"
        ):
            MyObj(a="a")  # type: ignore[call-arg]
