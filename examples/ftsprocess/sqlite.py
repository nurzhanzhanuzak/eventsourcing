from eventsourcing.sqlite import SQLiteProcessRecorder
from examples.ftscontentmanagement.sqlite import SQLiteFtsApplicationRecorder


class SQLiteFtsProcessRecorder(SQLiteFtsApplicationRecorder, SQLiteProcessRecorder):
    pass
