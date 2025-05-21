from __future__ import annotations

from typing import TYPE_CHECKING

from eventsourcing.postgres import PostgresDatastore
from examples.coursebooking.test_application import TestEnrolment
from examples.coursebookingdcb.application import EnrolmentWithDCB
from examples.dcb.postgres import PostgresDCBEventStore
from examples.dcb.test_dcb import drop_functions_and_types

if TYPE_CHECKING:
    from examples.coursebooking.interface import EnrolmentProtocol


class TestEnrolmentWithDCB(TestEnrolment):
    def setUp(self) -> None:
        super().setUp()
        self.env["PERSISTENCE_MODULE"] = "examples.dcb.popo"

    def construct_app(self) -> EnrolmentProtocol:
        return EnrolmentWithDCB(self.env)

    def test_enrolment_with_postgres(self) -> None:
        self.env["PERSISTENCE_MODULE"] = "examples.dcb.postgres"
        try:
            super().test_enrolment_with_postgres()
        finally:
            eventstore = PostgresDCBEventStore(
                datastore=PostgresDatastore(
                    dbname=self.env["POSTGRES_DBNAME"],
                    host=self.env["POSTGRES_HOST"],
                    port=self.env["POSTGRES_PORT"],
                    user=self.env["POSTGRES_USER"],
                    password=self.env["POSTGRES_PASSWORD"],
                )
            )
            drop_functions_and_types(eventstore)


del TestEnrolment
