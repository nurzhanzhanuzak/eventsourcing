from eventsourcing.tests.application import (
    TIMEIT_FACTOR,
    ApplicationTestCase,
    ExampleApplicationTestCase,
)


class TestApplicationWithPOPO(ApplicationTestCase):
    pass


class TestExampleApplicationWithPOPO(ExampleApplicationTestCase):
    timeit_number = 100 * TIMEIT_FACTOR
    expected_factory_topic = "eventsourcing.popo:POPOFactory"


del ApplicationTestCase
del ExampleApplicationTestCase
