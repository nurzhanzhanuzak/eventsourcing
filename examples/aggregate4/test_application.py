from __future__ import annotations

from unittest import TestCase

from examples.aggregate4.application import DogSchool
from examples.aggregate4.domainmodel import Dog


class TestDogSchool(TestCase):
    def test_dog_school(self) -> None:
        # Construct application object.
        school = DogSchool()

        # Evolve application state.
        dog_id = school.register_dog("Fido")

        # Query application state.
        dog = school.get_dog(dog_id)
        assert dog["name"] == "Fido"
        assert dog["tricks"] == ()
        assert dog["created_on"] == dog["modified_on"]

        school.add_trick(dog_id, "roll over")
        school.add_trick(dog_id, "play dead")

        # Query application state.
        dog = school.get_dog(dog_id)
        assert dog["name"] == "Fido"
        assert dog["tricks"] == ("roll over", "play dead")
        assert dog["created_on"] < dog["modified_on"]

        # Select notifications.
        notifications = school.notification_log.select(start=1, limit=10)
        assert len(notifications) == 3

        # Take snapshot.
        school.take_snapshot(dog_id, version=3, projector_func=Dog.project_events)
        dog = school.get_dog(dog_id)
        assert dog["name"] == "Fido"
        assert dog["tricks"] == ("roll over", "play dead")
        assert dog["created_on"] < dog["modified_on"]

        # Continue with snapshotted aggregate.
        school.add_trick(dog_id, "fetch ball")
        dog = school.get_dog(dog_id)
        assert dog["name"] == "Fido"
        assert dog["tricks"] == ("roll over", "play dead", "fetch ball")
        assert dog["created_on"] < dog["modified_on"]
