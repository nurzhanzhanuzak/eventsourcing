from __future__ import annotations

from eventsourcing.domain import event
from examples.aggregate7.immutablemodel import Immutable
from examples.aggregate8.mutablemodel import Aggregate, AggregateSnapshot, SnapshotState


class Trick(Immutable):
    name: str


class DogSnapshotState(SnapshotState):
    name: str
    tricks: list[Trick]


class Dog(Aggregate):
    class Snapshot(AggregateSnapshot):
        state: DogSnapshotState

    @event("Registered")
    def __init__(self, name: str) -> None:
        self.name = name
        self.tricks: list[Trick] = []

    @event("TrickAdded")
    def add_trick(self, trick: Trick) -> None:
        self.tricks.append(trick)
