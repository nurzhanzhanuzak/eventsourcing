from eventsourcing.tests.application import (
    ApplicationTestCase,
    ExampleApplicationTestCase,
)


class TestApplicationWithPOPO(ApplicationTestCase):
    pass


class TestExampleApplicationWithPOPO(ExampleApplicationTestCase):
    expected_factory_topic = "eventsourcing.popo:POPOFactory"


del ApplicationTestCase
del ExampleApplicationTestCase
