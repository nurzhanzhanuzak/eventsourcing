from __future__ import annotations

from typing import Any
from uuid import UUID

from eventsourcing.application import Application
from examples.aggregate5.domainmodel import Dog


class DogSchool(Application[UUID]):
    is_snapshotting_enabled = True

    def register_dog(self, name: str) -> UUID:
        dog, event = Dog.register(name)
        self.save(event)
        return dog.id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog = self.repository.get(dog_id, projector_func=Dog.projector)
        dog, event = dog.add_trick(trick)
        self.save(event)

    def get_dog(self, dog_id: UUID) -> dict[str, Any]:
        dog = self.repository.get(dog_id, projector_func=Dog.projector)
        return {"name": dog.name, "tricks": dog.tricks}
