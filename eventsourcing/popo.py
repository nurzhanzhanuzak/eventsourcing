from __future__ import annotations

import contextlib
from collections import defaultdict
from threading import Event, Lock
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Sequence, Set, Type

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    IntegrityError,
    ListenNotifySubscription,
    Notification,
    ProcessRecorder,
    StoredEvent,
    Tracking,
    TrackingRecorder,
)
from eventsourcing.utils import resolve_topic, reversed_keys

if TYPE_CHECKING:  # pragma: no cover
    from uuid import UUID


class POPORecorder:
    def __init__(self) -> None:
        self._database_lock = Lock()


class POPOAggregateRecorder(POPORecorder, AggregateRecorder):
    def __init__(self) -> None:
        super().__init__()
        self._stored_events: List[StoredEvent] = []
        self._stored_events_index: Dict[UUID, Dict[int, int]] = defaultdict(dict)

    def insert_events(
        self, stored_events: List[StoredEvent], **kwargs: Any
    ) -> Sequence[int] | None:
        self._insert_events(stored_events, **kwargs)
        return None

    def _insert_events(
        self, stored_events: List[StoredEvent], **kwargs: Any
    ) -> Sequence[int] | None:
        with self._database_lock:
            self._assert_uniqueness(stored_events, **kwargs)
            return self._update_table(stored_events, **kwargs)

    def _assert_uniqueness(self, stored_events: List[StoredEvent], **_: Any) -> None:
        new = set()
        for s in stored_events:
            # Check events don't already exist.
            if s.originator_version in self._stored_events_index[s.originator_id]:
                msg = f"Stored event already recorded: {s}"
                raise IntegrityError(msg)
            new.add((s.originator_id, s.originator_version))
        # Check new events are unique.
        if len(new) < len(stored_events):
            msg = f"Stored events are not unique: {stored_events}"
            raise IntegrityError(msg)

    def _update_table(
        self, stored_events: List[StoredEvent], **_: Any
    ) -> Sequence[int] | None:
        notification_ids = []
        for s in stored_events:
            self._stored_events.append(s)
            self._stored_events_index[s.originator_id][s.originator_version] = (
                len(self._stored_events) - 1
            )
            notification_ids.append(len(self._stored_events))
        return notification_ids

    def select_events(
        self,
        originator_id: UUID,
        *,
        gt: int | None = None,
        lte: int | None = None,
        desc: bool = False,
        limit: int | None = None,
    ) -> List[StoredEvent]:
        with self._database_lock:
            results = []

            index = self._stored_events_index[originator_id]
            positions: Iterable[int]
            positions = reversed_keys(index) if desc else index.keys()
            for p in positions:
                if gt is not None and not p > gt:
                    continue
                if lte is not None and not p <= lte:
                    continue
                s = self._stored_events[index[p]]
                results.append(s)
                if len(results) == limit:
                    break
            return results


class POPOApplicationRecorder(POPOAggregateRecorder, ApplicationRecorder):
    def __init__(self) -> None:
        super().__init__()
        self._listeners: Set[Event] = set()

    def insert_events(
        self, stored_events: List[StoredEvent], **kwargs: Any
    ) -> Sequence[int] | None:
        notification_ids = self._insert_events(stored_events, **kwargs)
        self._notify_listeners()
        return notification_ids

    def select_notifications(
        self,
        start: int | None,
        limit: int,
        stop: int | None = None,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> List[Notification]:
        with self._database_lock:
            results = []
            if start is None:
                start = 1
                inclusive_of_start = True
            if not inclusive_of_start:
                start += 1
            start = max(start, 1)  # Don't use negative indexes!
            i = start - 1  # Zero-based indexing.
            while True:
                if stop is not None and i > stop - 1:
                    break
                try:
                    s = self._stored_events[i]
                except IndexError:
                    break
                i += 1
                if topics and s.topic not in topics:
                    continue
                n = Notification(
                    id=i,
                    originator_id=s.originator_id,
                    originator_version=s.originator_version,
                    topic=s.topic,
                    state=s.state,
                )
                results.append(n)
                if len(results) == limit:
                    break
            return results

    def max_notification_id(self) -> int | None:
        with self._database_lock:
            return len(self._stored_events) or None

    def subscribe(self, gt: int | None = None) -> POPOSubscription:
        return POPOSubscription(self, gt)

    def listen(self, event: Event) -> None:
        self._listeners.add(event)

    def unlisten(self, event: Event) -> None:
        with contextlib.suppress(KeyError):
            self._listeners.remove(event)

    def _notify_listeners(self) -> None:
        for listener in self._listeners:
            listener.set()


class POPOSubscription(ListenNotifySubscription[POPOApplicationRecorder]):
    def __init__(
        self, recorder: POPOApplicationRecorder, gt: int | None = None
    ) -> None:
        assert isinstance(recorder, POPOApplicationRecorder)
        super().__init__(recorder=recorder, gt=gt)
        self._recorder.listen(self._has_been_notified)

    def stop(self) -> None:
        super().stop()
        self._recorder.unlisten(self._has_been_notified)


class POPOTrackingRecorder(POPORecorder, TrackingRecorder):
    def __init__(self) -> None:
        super().__init__()
        self._tracking_table: Dict[str, set[int]] = defaultdict(set)
        self._max_tracking_ids: Dict[str, int | None] = defaultdict(lambda: None)

    def _assert_tracking_uniqueness(self, tracking: Tracking) -> None:
        if tracking.notification_id in self._tracking_table[tracking.application_name]:
            msg = (
                f"Already recorded notification ID {tracking.notification_id} "
                f"for application {tracking.application_name}"
            )
            raise IntegrityError(msg)

    def insert_tracking(self, tracking: Tracking) -> None:
        with self._database_lock:
            self._assert_tracking_uniqueness(tracking)
            self._insert_tracking(tracking)

    def _insert_tracking(self, tracking: Tracking) -> None:
        self._tracking_table[tracking.application_name].add(tracking.notification_id)
        max_tracking_id = self._max_tracking_ids[tracking.application_name]
        if max_tracking_id is None or max_tracking_id < tracking.notification_id:
            self._max_tracking_ids[tracking.application_name] = tracking.notification_id

    def max_tracking_id(self, application_name: str) -> int | None:
        with self._database_lock:
            return self._max_tracking_ids[application_name]

    def has_tracking_id(self, application_name: str, notification_id: int) -> bool:
        with self._database_lock:
            return notification_id in self._tracking_table[application_name]


class POPOProcessRecorder(
    POPOTrackingRecorder, POPOApplicationRecorder, ProcessRecorder
):
    def _assert_uniqueness(
        self, stored_events: List[StoredEvent], **kwargs: Any
    ) -> None:
        super()._assert_uniqueness(stored_events, **kwargs)
        t: Tracking | None = kwargs.get("tracking", None)
        if t:
            self._assert_tracking_uniqueness(t)

    def _update_table(
        self, stored_events: List[StoredEvent], **kwargs: Any
    ) -> Sequence[int] | None:
        notification_ids = super()._update_table(stored_events, **kwargs)
        t: Tracking | None = kwargs.get("tracking", None)
        if t:
            self._insert_tracking(t)
        return notification_ids


class POPOFactory(InfrastructureFactory[POPOTrackingRecorder]):
    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        return POPOAggregateRecorder()

    def application_recorder(self) -> ApplicationRecorder:
        return POPOApplicationRecorder()

    def tracking_recorder(
        self, tracking_recorder_class: Type[POPOTrackingRecorder] | None = None
    ) -> POPOTrackingRecorder:
        if tracking_recorder_class is None:
            tracking_recorder_topic = self.env.get(self.TRACKING_RECORDER_TOPIC)
            if tracking_recorder_topic:
                tracking_recorder_class = resolve_topic(tracking_recorder_topic)
            else:
                tracking_recorder_class = POPOTrackingRecorder
        assert tracking_recorder_class is not None
        assert issubclass(tracking_recorder_class, POPOTrackingRecorder)
        return tracking_recorder_class()

    def process_recorder(self) -> ProcessRecorder:
        return POPOProcessRecorder()


Factory = POPOFactory
