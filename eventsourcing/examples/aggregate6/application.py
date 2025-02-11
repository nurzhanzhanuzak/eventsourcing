from typing import Any, Dict
from uuid import UUID

from eventsourcing.application import Application
from eventsourcing.examples.aggregate6.domainmodel import (
    add_trick,
    project_dog,
    register_dog,
)


class DogSchool(Application):
    is_snapshotting_enabled = True

    def register_dog(self, name: str) -> UUID:
        event = register_dog(name)
        self.save(event)
        return event.originator_id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog = self.repository.get(dog_id, projector_func=project_dog)
        self.save(add_trick(dog, trick))

    def get_dog(self, dog_id: UUID) -> Dict[str, Any]:
        dog = self.repository.get(dog_id, projector_func=project_dog)
        return {"name": dog.name, "tricks": dog.tricks}
