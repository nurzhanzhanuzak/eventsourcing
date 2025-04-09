from __future__ import annotations

from typing import TYPE_CHECKING, Any

from eventsourcing.application import Application
from examples.aggregate4.domainmodel import Dog

if TYPE_CHECKING:
    from uuid import UUID


class DogSchool(Application):
    is_snapshotting_enabled = True

    def register_dog(self, name: str) -> UUID:
        dog = Dog.register(name)
        self.save(dog)
        return dog.id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog = self.repository.get(dog_id, projector_func=Dog.projector)
        dog.add_trick(trick)
        self.save(dog)

    def get_dog(self, dog_id: UUID) -> dict[str, Any]:
        dog = self.repository.get(dog_id, projector_func=Dog.projector)
        return {
            "name": dog.name,
            "tricks": tuple(dog.tricks),
            "created_on": dog.created_on,
            "modified_on": dog.modified_on,
        }
