import random

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestPropertyEncryption(TestkitTestCase):
    required_features = (types.Feature.API_PROPERTY_ENCRYPTION,)

    def setUp(self):
        super().setUp()
        self._server = StubServer(9020)
        self._driver = None

    def tearDown(self):
        if self._driver:
            self._driver.close()
        return super().tearDown()

    def _new_driver(self, profiles=("default",)):
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(
            self._backend, uri, auth,
            property_encryption_profiles=list(profiles)
        )
        return self._driver

    def test_round_trips_a_single_value(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )
        decrypted = driver.decrypt(encrypted, use_persisted_aad=True)

        self.assertEqual(decrypted, types.CypherString("hello world"))

    def test_round_trips_many_values_in_shuffled_order(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        values = [
            types.CypherBool(True),
            types.CypherBool(False),
            types.CypherInt(0),
            types.CypherInt(-1),
            types.CypherInt(9223372036854775807),
            types.CypherFloat(3.25),
            types.CypherString(""),
            types.CypherString("a"),
            types.CypherString("hello world"),
            types.CypherBytes(b""),
            types.CypherBytes(b"\x00\x01\x02"),
            types.CypherList([types.CypherInt(1), types.CypherInt(2)])
        ]

        vectors = [
            (value, driver.encrypt_to_bytes(value, key_alias="k1"))
            for value in values
        ]

        order = list(range(len(vectors)))
        random.shuffle(order)

        for i in order:
            original, encrypted = vectors[i]
            decrypted = driver.decrypt(encrypted, use_persisted_aad=True)
            self.assertEqual(
                decrypted, original,
                f"vector {i} ({original!r}) round-tripped to {decrypted!r}"
            )

    def test_encrypting_the_same_value_twice_yields_different_ciphertext(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        value = types.CypherString("hello world")
        first = driver.encrypt_to_bytes(value, key_alias="k1")
        second = driver.encrypt_to_bytes(value, key_alias="k1")

        self.assertNotEqual(first, second)

    def test_decrypt_raises_on_wrong_aad(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        encrypted = driver.encrypt_to_bytes(
            types.CypherString("aad-bound"),
            aad=types.CypherString("row-42"),
            key_alias="k1"
        )

        with self.assertRaises(types.DriverError):
            driver.decrypt(encrypted, aad=types.CypherString("row-999"))
