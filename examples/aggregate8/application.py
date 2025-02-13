from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict

from eventsourcing.application import Application
from eventsourcing.utils import get_topic
from examples.aggregate7.orjsonpydantic import OrjsonTranscoder, PydanticMapper
from examples.aggregate8.domainmodel import Dog, Trick

if TYPE_CHECKING:  # pragma: nocover
    from uuid import UUID


class DogSchool(Application):
    is_snapshotting_enabled = True
    env: ClassVar[Dict[str, str]] = {
        "TRANSCODER_TOPIC": get_topic(OrjsonTranscoder),
        "MAPPER_TOPIC": get_topic(PydanticMapper),
    }

    def register_dog(self, name: str) -> UUID:
        dog = Dog(name)
        self.save(dog)
        return dog.id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog: Dog = self.repository.get(dog_id)
        dog.add_trick(Trick(name=trick))
        self.save(dog)

    def get_dog(self, dog_id: UUID) -> Dict[str, Any]:
        dog: Dog = self.repository.get(dog_id)
        return {
            "name": dog.name,
            "tricks": tuple([t.name for t in dog.tricks]),
            "created_on": dog.created_on,
            "modified_on": dog.modified_on,
        }
