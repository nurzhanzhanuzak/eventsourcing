from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict

from eventsourcing.application import Application
from eventsourcing.utils import get_topic
from examples.aggregate7.domainmodel import Trick, add_trick, project_dog, register_dog
from examples.aggregate7.immutablemodel import Snapshot
from examples.aggregate7.orjsonpydantic import OrjsonTranscoder, PydanticMapper

if TYPE_CHECKING:
    from uuid import UUID


class DogSchool(Application):
    is_snapshotting_enabled = True
    snapshot_class = Snapshot
    env: ClassVar[Dict[str, str]] = {
        "TRANSCODER_TOPIC": get_topic(OrjsonTranscoder),
        "MAPPER_TOPIC": get_topic(PydanticMapper),
    }

    def register_dog(self, name: str) -> UUID:
        event = register_dog(name)
        self.save(event)
        return event.originator_id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog = self.repository.get(dog_id, projector_func=project_dog)
        self.save(add_trick(dog, Trick(name=trick)))

    def get_dog(self, dog_id: UUID) -> Dict[str, Any]:
        dog = self.repository.get(dog_id, projector_func=project_dog)
        return {
            "name": dog.name,
            "tricks": tuple([t.name for t in dog.tricks]),
            "created_on": dog.created_on,
            "modified_on": dog.modified_on,
        }
