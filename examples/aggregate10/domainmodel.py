from __future__ import annotations

from eventsourcing.domain import event
from examples.aggregate9.immutablemodel import Immutable
from examples.aggregate10.mutablemodel import (
    Aggregate,
    AggregateSnapshot,
    SnapshotState,
)


class Trick(Immutable, frozen=True):
    name: str


class DogSnapshotState(SnapshotState, frozen=True):
    name: str
    tricks: list[Trick]


class Dog(Aggregate):
    class Snapshot(AggregateSnapshot, frozen=True):
        state: DogSnapshotState

    @event("Registered")
    def __init__(self, name: str) -> None:
        self.name = name
        self.tricks: list[Trick] = []

    @event("TrickAdded")
    def add_trick(self, trick: Trick) -> None:
        self.tricks.append(trick)
