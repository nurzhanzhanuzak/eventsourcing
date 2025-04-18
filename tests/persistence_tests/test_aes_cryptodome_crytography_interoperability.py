from unittest import TestCase

import eventsourcing.cipher as pycryptodome
from eventsourcing import cryptography
from eventsourcing.utils import Environment


class TestAesCipherInteroperability(TestCase):
    def test(self) -> None:
        environment = Environment()
        key = pycryptodome.AESCipher.create_key(16)
        environment["CIPHER_KEY"] = key

        aes_pycryptodome = pycryptodome.AESCipher(environment)
        aes_cryptography = cryptography.AESCipher(environment)

        plain_text = b"some text"
        encrypted_text = aes_pycryptodome.encrypt(plain_text)
        recovered_text = aes_cryptography.decrypt(encrypted_text)
        self.assertEqual(plain_text, recovered_text)

        plain_text = b"some text"
        encrypted_text = aes_cryptography.encrypt(plain_text)
        recovered_text = aes_pycryptodome.decrypt(encrypted_text)
        self.assertEqual(plain_text, recovered_text)
