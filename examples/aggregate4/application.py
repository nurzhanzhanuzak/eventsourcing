from __future__ import annotations

from typing import Any
from uuid import UUID

from eventsourcing.application import Application
from examples.aggregate4.domainmodel import Dog


class DogSchool(Application[UUID]):
    is_snapshotting_enabled = True

    def register_dog(self, name: str) -> UUID:
        dog = Dog.register(name)
        self.save(dog)
        return dog.id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog: Dog = self.repository.get(dog_id, projector_func=Dog.project_events)
        dog.add_trick(trick)
        self.save(dog)

    def get_dog(self, dog_id: UUID) -> dict[str, Any]:
        dog: Dog = self.repository.get(dog_id, projector_func=Dog.project_events)
        return {
            "name": dog.name,
            "tricks": tuple(dog.tricks),
            "created_on": dog.created_on,
            "modified_on": dog.modified_on,
        }
