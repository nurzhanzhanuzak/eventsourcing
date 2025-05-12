from __future__ import annotations

from typing import TYPE_CHECKING, Any

from examples.aggregate9.msgspecstructs import MsgspecApplication
from examples.aggregate10.domainmodel import Dog, Trick

if TYPE_CHECKING:
    from uuid import UUID


class DogSchool(MsgspecApplication):
    is_snapshotting_enabled = True

    def register_dog(self, name: str) -> UUID:
        dog = Dog(name)
        self.save(dog)
        return dog.id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog: Dog = self.repository.get(dog_id)
        dog.add_trick(Trick(name=trick))
        self.save(dog)

    def get_dog(self, dog_id: UUID) -> dict[str, Any]:
        dog: Dog = self.repository.get(dog_id)
        return {
            "name": dog.name,
            "tricks": tuple([t.name for t in dog.tricks]),
            "created_on": dog.created_on,
            "modified_on": dog.modified_on,
        }
