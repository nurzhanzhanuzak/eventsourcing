from decimal import Decimal
from unittest.case import TestCase
from uuid import uuid4

from eventsourcing.cipher import AESCipher
from eventsourcing.compressor import ZlibCompressor
from eventsourcing.persistence import (
    DatetimeAsISO,
    DecimalAsStr,
    JSONTranscoder,
    Mapper,
    MapperDeserialisationError,
    TranscodingNotRegisteredError,
    UUIDAsHex,
)
from eventsourcing.tests.domain import BankAccount
from eventsourcing.utils import Environment, get_topic


class TestMapper(TestCase):
    def test(self) -> None:
        # Construct transcoder.
        transcoder = JSONTranscoder()
        transcoder.register(UUIDAsHex())
        transcoder.register(DecimalAsStr())
        transcoder.register(DatetimeAsISO())

        # Construct cipher.
        environment = Environment()
        environment[AESCipher.CIPHER_KEY] = AESCipher.create_key(16)
        cipher = AESCipher(environment)

        # Construct compressor.
        compressor = ZlibCompressor()

        # Create a domain event.
        domain_event = BankAccount.TransactionAppended(
            originator_id=uuid4(),
            originator_version=123456,
            timestamp=BankAccount.TransactionAppended.create_timestamp(),
            amount=Decimal("10.00"),
        )

        # Construct mapper with transcoder.
        mapper = Mapper(transcoder=transcoder)

        # Map to stored event.
        stored_event = mapper.to_stored_event(domain_event)

        # Map to domain event.
        copy = mapper.to_domain_event(stored_event)

        # Check decrypted copy has correct values.
        assert isinstance(copy, BankAccount.TransactionAppended)
        self.assertEqual(copy.originator_id, domain_event.originator_id)
        self.assertEqual(copy.originator_version, domain_event.originator_version)
        self.assertEqual(copy.timestamp, domain_event.timestamp)
        self.assertEqual(copy.amount, domain_event.amount)
        self.assertEqual(copy, domain_event)

        # Construct mapper with less capable transcoder.
        mapper = Mapper(
            transcoder=JSONTranscoder(),
            cipher=cipher,
        )

        # Check mapper raises MapperDeserialisationError.
        with self.assertRaises(MapperDeserialisationError) as cm:
            mapper.to_domain_event(stored_event)

        # Check the error has useful information about the event.
        self.assertIn(get_topic(type(domain_event)), str(cm.exception))
        self.assertIn(str(domain_event.originator_id), str(cm.exception))
        self.assertIn(str(domain_event.originator_version), str(cm.exception))

        # Check mapper raises TranscodingNotRegisteredError.
        with self.assertRaises(TranscodingNotRegisteredError):
            mapper.to_stored_event(domain_event)

        # Construct mapper with cipher.
        mapper = Mapper(transcoder=transcoder, cipher=cipher)

        # Map to stored event.
        stored_event = mapper.to_stored_event(domain_event)

        # Map to domain event.
        copy = mapper.to_domain_event(stored_event)

        # Check values are not visible.
        self.assertNotIn("Alice", str(stored_event.state))

        # Check decrypted copy has correct values.
        self.assertEqual(copy.originator_id, domain_event.originator_id)
        self.assertEqual(copy.originator_version, domain_event.originator_version)

        self.assertEqual(len(stored_event.state), 162)

        # Construct mapper with cipher and compressor.
        mapper = Mapper(
            transcoder=transcoder,
            cipher=cipher,
            compressor=compressor,
        )

        # Map to stored event.
        stored_event = mapper.to_stored_event(domain_event)

        # Map to domain event.
        copy = mapper.to_domain_event(stored_event)

        # Check decompressed copy has correct values.
        self.assertEqual(copy.originator_id, domain_event.originator_id)
        self.assertEqual(copy.originator_version, domain_event.originator_version)

        self.assertIn(len(stored_event.state), range(129, 143))


# Todo: Move the upcasting tests in here.
