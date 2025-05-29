from unittest import TestCase

from eventsourcing.dcb.application import DCBApplication


class TestDCBApplication(TestCase):
    def test_as_context_manager(self) -> None:
        with DCBApplication():
            pass

    def test_construct_with_env(self) -> None:
        with DCBApplication({"NAME": "value"}) as app:
            self.assertIn("NAME", app.env)

    def test_can_subclass(self) -> None:

        class MyApp1(DCBApplication):
            pass

        app1 = MyApp1()
        self.assertEqual("MyApp1", app1.name)

        class MyApp2(DCBApplication):
            name = "name1"

        app2 = MyApp2()
        self.assertEqual("name1", app2.name)
