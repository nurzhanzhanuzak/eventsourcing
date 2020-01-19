from unittest import skip

from eventsourcing.tests.datastore_tests import test_axonserver
from eventsourcing.tests.sequenced_item_tests import base


class AxonServerRecordManagerTestCase(
    test_axonserver.AxonDatastoreTestCase, base.WithRecordManagers
):
    """
    Base class for test cases that need record manager with Axon Server.
    """


class TestAxonServerRecordManagerWithIntegerSequences(
    AxonServerRecordManagerTestCase, base.IntegerSequencedRecordTestCase
):
    """
    Test case for integer sequenced record manager with Axon Server.
    """


@skip("This isn't terminating at the moment, for some reason")
class TestSimpleIteratorWithAxonServer(
    AxonServerRecordManagerTestCase, base.SequencedItemIteratorTestCase
):
    """
    Test case for simple iterator in record manager with Axon Server.
    """


@skip("This isn't terminating at the moment, for some reason")
class TestThreadedIteratorWithAxonServer(
    AxonServerRecordManagerTestCase, base.ThreadedSequencedItemIteratorTestCase
):
    """
    Test case for threaded iterator in record manager with Axon Server.
    """

    use_named_temporary_file = True
