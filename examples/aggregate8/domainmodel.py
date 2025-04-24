from __future__ import annotations

from pydantic import BaseModel

from eventsourcing.domain import event
from examples.aggregate8.mutablemodel import Aggregate, AggregateSnapshot, SnapshotState


class Trick(BaseModel):
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
