from typing import Tuple, cast
from unittest import TestCase
from uuid import UUID

from eventsourcing.domain import ProgrammingError
from eventsourcing.examples.aggregate7.application import DogSchool
from eventsourcing.examples.aggregate7.domainmodel import (
    Dog,
    Trick,
    add_trick,
    project_dog,
    register_dog,
)


class SubDogSchool(DogSchool):
    snapshotting_intervals = {Dog: 1}

    def register_dog(self, name: str) -> UUID:
        event = register_dog(name)
        dog = project_dog(None, [event])
        self.save(dog, event)
        return event.originator_id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog = self.repository.get(dog_id, projector_func=project_dog)
        event = add_trick(dog, Trick(name=trick))
        dog = cast(Dog, project_dog(dog, [event]))
        self.save(dog, event)


class TestDogSchool(TestCase):
    def test_dog_school(self) -> None:
        # Construct application object.
        school = SubDogSchool()

        # Check error when snapshotting_projectors not set.
        with self.assertRaises(ProgrammingError) as cm:
            school.register_dog("Fido")

        self.assertIn(
            "set snapshotting_projectors on application class", cm.exception.args[0]
        )

        # Set snapshotting_projectors.
        SubDogSchool.snapshotting_projectors = {Dog: project_dog}

        # Check snapshotting when snapshotting_projectors is set.
        dog_id = school.register_dog("Fido")

        assert school.snapshots is not None
        self.assertEqual(1, len(list(school.snapshots.get(dog_id))))

        school.add_trick(dog_id, "roll over")
        self.assertEqual(2, len(list(school.snapshots.get(dog_id))))

        school.add_trick(dog_id, "play dead")
        self.assertEqual(3, len(list(school.snapshots.get(dog_id))))

        # Query application state.
        dog = school.get_dog(dog_id)
        self.assertEqual(dog["name"], "Fido")
        self.assertEqualTricks(dog["tricks"], ("roll over", "play dead"))

    def assertEqualTricks(
        self, actual: Tuple[Trick, ...], expected: Tuple[str, ...]
    ) -> None:
        self.assertEqual(len(actual), len(expected))
        for i, trick in enumerate(actual):
            self.assertIsInstance(trick, Trick)
            self.assertEqual(trick.name, expected[i])
