from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Type

from eventsourcing.persistence import StoredEvent
from eventsourcing.postgres import PostgresFactory, PostgresProcessRecorder
from examples.searchablecontent.postgres import PostgresSearchableContentRecorder


class SearchableContentProcessRecorder(
    PostgresSearchableContentRecorder, PostgresProcessRecorder
):
    pass


class SearchableContentFactory(PostgresFactory):
    process_recorder_class: Type[PostgresProcessRecorder] = (
        PostgresSearchableContentProcessRecorder
    )


del PostgresFactory
