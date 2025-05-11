from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

import msgspec

from eventsourcing.application import Application
from eventsourcing.persistence import Mapper, StoredEvent, Transcoder
from eventsourcing.utils import get_topic, resolve_topic

if TYPE_CHECKING:
    from eventsourcing.domain import DomainEventProtocol


class MsgspecMapper(Mapper):
    def to_stored_event(self, domain_event: DomainEventProtocol) -> StoredEvent:
        topic = get_topic(domain_event.__class__)
        stored_state = msgspec.json.encode(domain_event)
        if self.compressor:
            stored_state = self.compressor.compress(stored_state)
        if self.cipher:
            stored_state = self.cipher.encrypt(stored_state)
        return StoredEvent(
            originator_id=domain_event.originator_id,
            originator_version=domain_event.originator_version,
            topic=topic,
            state=stored_state,
        )

    def to_domain_event(self, stored_event: StoredEvent) -> DomainEventProtocol:
        stored_state = stored_event.state
        if self.cipher:
            stored_state = self.cipher.decrypt(stored_state)
        if self.compressor:
            stored_state = self.compressor.decompress(stored_state)
        cls = resolve_topic(stored_event.topic)
        return msgspec.json.decode(stored_state, type=cls)


class NullTranscoder(Transcoder):
    def encode(self, obj: Any) -> bytes:
        """Encodes obj as bytes."""
        return b""

    def decode(self, data: bytes) -> Any:
        """Decodes obj from bytes."""
        return None


class MsgspecApplication(Application):
    env: ClassVar[dict[str, str]] = {
        "MAPPER_TOPIC": get_topic(MsgspecMapper),
        "TRANSCODER_TOPIC": get_topic(NullTranscoder),
    }
