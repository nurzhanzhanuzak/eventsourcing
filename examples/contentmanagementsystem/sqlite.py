from eventsourcing.sqlite import SQLiteFactory, SQLiteProcessRecorder
from examples.searchablecontent.sqlite import SQLiteSearchableContentRecorder


class SearchableContentProcessRecorder(
    SQLiteSearchableContentRecorder, SQLiteProcessRecorder
):
    pass


class SearchableContentInfrastructureFactory(SQLiteFactory):
    process_recorder_class = SearchableContentProcessRecorder


del SQLiteFactory
