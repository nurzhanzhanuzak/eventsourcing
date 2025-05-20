from __future__ import annotations

from typing import TYPE_CHECKING

from eventsourcing.postgres import PostgresDatastore
from examples.coursebooking.test_application import TestEnrolment
from examples.coursebookingdcb.application import EnrolmentWithDCB
from tests.dcb_tests.postgres import PostgresDCBEventStore
from tests.dcb_tests.test_dcb import drop_functions_and_types

if TYPE_CHECKING:
    from examples.coursebooking.interface import EnrolmentProtocol


class TestEnrolmentWithDCB(TestEnrolment):
    def construct_app(self) -> EnrolmentProtocol:
        return EnrolmentWithDCB(self.env)

    def test_enrolment_with_postgres(self) -> None:
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
