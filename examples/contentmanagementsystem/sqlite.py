from eventsourcing.sqlite import Factory, SQLiteProcessRecorder
from examples.searchablecontent.sqlite import SQLiteSearchableContentRecorder


class SearchableContentProcessRecorder(
    SQLiteSearchableContentRecorder, SQLiteProcessRecorder
):
    pass


class SearchableContentInfrastructureFactory(Factory):
    process_recorder_class = SearchableContentProcessRecorder


del Factory
