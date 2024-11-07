from __future__ import annotations

from eventsourcing.system import System
from examples.contentmanagement.application import ContentManagementApplication
from examples.contentmanagementsystem.application import SearchIndexApplication


class ContentManagementSystem(System):
    def __init__(self) -> None:
        super().__init__(pipes=[[ContentManagementApplication, SearchIndexApplication]])
