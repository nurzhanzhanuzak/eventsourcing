from __future__ import annotations

from typing import TYPE_CHECKING, Any

from eventsourcing.application import Application
from examples.aggregate11.domainmodel import Dog, Snapshot

if TYPE_CHECKING:
    from eventsourcing.domain import SnapshotProtocol


class DogSchool(Application[str]):
    is_snapshotting_enabled = True
    snapshot_class: type[SnapshotProtocol] = Snapshot

    def register_dog(self, name: str) -> str:
        dog = Dog(name)
        self.save(dog)
        return dog.id

    def add_trick(self, dog_id: str, trick: str) -> None:
        dog: Dog = self.repository.get(dog_id)
        dog.add_trick(trick)
        self.save(dog)

    def get_dog(self, dog_id: str) -> dict[str, Any]:
        dog: Dog = self.repository.get(dog_id)
        return {"name": dog.name, "tricks": tuple(dog.tricks)}
