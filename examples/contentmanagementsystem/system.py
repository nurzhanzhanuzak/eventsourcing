from __future__ import annotations

from examples.contentmanagement.application import (
    ContentManagementApplication,
)
from examples.contentmanagementsystem.application import (
    SearchIndexApplication,
)
from eventsourcing.system import System


class ContentManagementSystem(System):
    def __init__(self) -> None:
        super().__init__(pipes=[[ContentManagementApplication, SearchIndexApplication]])
