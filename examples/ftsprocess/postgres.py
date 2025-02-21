from __future__ import annotations

from eventsourcing.postgres import PostgresProcessRecorder
from examples.ftscontentmanagement.postgres import PostgresFtsApplicationRecorder


class PostgresFtsProcessRecorder(
    PostgresFtsApplicationRecorder, PostgresProcessRecorder
):
    pass
