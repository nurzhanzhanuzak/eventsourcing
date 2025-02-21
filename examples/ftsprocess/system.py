from __future__ import annotations

from eventsourcing.system import System
from examples.contentmanagement.application import ContentManagement
from examples.ftsprocess.application import FtsProcess


class ContentManagementSystem(System):
    def __init__(self) -> None:
        super().__init__(pipes=[[ContentManagement, FtsProcess]])
