from eventsourcing.postgres import Factory, PostgresProcessRecorder
from eventsourcing.postgres import PostgresFactory, PostgresProcessRecorder
from examples.searchablecontent.postgres import PostgresSearchableContentRecorder


class SearchableContentProcessRecorder(
    PostgresSearchableContentRecorder, PostgresProcessRecorder
):
    pass


class SearchableContentInfrastructureFactory(Factory):
    process_recorder_class = SearchableContentProcessRecorder
class SearchableContentFactory(PostgresFactory):


del PostgresFactory
