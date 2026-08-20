import random

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.property_encryption.decrypt_interop_fixtures import (
    DECRYPT_INTEROP_TEST_CASES,
    INTEROP_PROFILE_NAME,
)
from tests.stub.property_encryption.deterministic_fixtures import (
    DETERMINISTIC_ENCAPSULATION,
    DETERMINISTIC_KEK,
    DETERMINISTIC_KEY_METADATA,
    DETERMINISTIC_PROFILE_NAME,
    DETERMINISTIC_TEST_CASES,
)
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
            types.CypherInt(32768),
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

    def test_fixed_iv_produces_identical_ciphertext(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        iv = bytes(range(12))
        value = types.CypherString("hello world")
        first = driver.encrypt_to_bytes(value, key_alias="k1", iv=iv)
        second = driver.encrypt_to_bytes(value, key_alias="k1", iv=iv)

        self.assertEqual(first, second)

    def test_decrypts_an_aad_bound_value_with_the_persisted_aad(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        encrypted = driver.encrypt_to_bytes(
            types.CypherString("aad-bound"),
            aad=types.CypherString("row-42"),
            key_alias="k1"
        )
        decrypted = driver.decrypt(encrypted, use_persisted_aad=True)

        self.assertEqual(decrypted, types.CypherString("aad-bound"))

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

    def test_encrypt_raises_on_unknown_alias(self):
        driver = self._new_driver()

        with self.assertRaises(types.DriverError):
            driver.encrypt_to_bytes(
                types.CypherString("hello world"), key_alias="no-such-key"
            )

    def test_encrypt_raises_when_alias_belongs_to_a_different_profile(self):
        driver = self._new_driver(profiles=("p1", "p2"))
        driver.create_encapsulated_key("k1", profile_name="p1")

        with self.assertRaises(types.DriverError):
            driver.encrypt_to_bytes(
                types.CypherString("hello world"),
                profile_name="p2", key_alias="k1"
            )

    def test_encrypt_raises_on_unknown_key_id(self):
        driver = self._new_driver()

        with self.assertRaises(types.DriverError):
            driver.encrypt_to_bytes(
                types.CypherString("hello world"), key_id="no-such-id"
            )

    def test_key_manager_raises_when_ambiguous_and_no_profile_given(self):
        driver = self._new_driver(profiles=("p1", "p2"))

        with self.assertRaises(types.DriverError):
            driver.create_encapsulated_key("k1")

    def test_imported_key_decrypts_with_a_fixed_kek(self):
        kek = bytes(range(32))

        driver_1 = self._new_driver(
            profiles=({"name": "fx", "kek": kek},)
        )
        key = driver_1.create_encapsulated_key("k1", profile_name="fx")
        encrypted = driver_1.encrypt_to_bytes(
            types.CypherString("hello world"),
            profile_name="fx", key_alias="k1"
        )
        driver_1.close()

        driver_2 = self._new_driver(
            profiles=({"name": "fx", "kek": kek},)
        )
        driver_2.import_encapsulated_key(
            "k1", key.encapsulated_bytes, key.metadata, profile_name="fx"
        )

        decrypted = driver_2.decrypt(encrypted, use_persisted_aad=True)

        self.assertEqual(decrypted, types.CypherString("hello world"))

    def test_encrypts_to_known_bytes(self):
        driver = self._new_driver(
            profiles=(
                {
                    "name": DETERMINISTIC_PROFILE_NAME,
                    "kek": DETERMINISTIC_KEK,
                },
            )
        )
        driver.import_encapsulated_key(
            "k", DETERMINISTIC_ENCAPSULATION,
            DETERMINISTIC_KEY_METADATA,
            profile_name=DETERMINISTIC_PROFILE_NAME
        )

        for case in DETERMINISTIC_TEST_CASES:
            with self.subTest(value=case.value):
                encrypted = driver.encrypt_to_bytes(
                    case.value, profile_name=DETERMINISTIC_PROFILE_NAME,
                    key_alias="k", iv=case.iv, aad=case.aad
                )
                self.assertEqual(encrypted, case.encrypted)

    def test_decrypts_values_produced_by_other_drivers(self):
        for case in DECRYPT_INTEROP_TEST_CASES:
            with self.subTest(driver=case.driver):
                driver = self._new_driver(
                    profiles=(
                        {
                            "name": INTEROP_PROFILE_NAME,
                            "kek": case.kek,
                        },
                    )
                )
                driver.import_encapsulated_key(
                    "k", case.encapsulation, case.metadata,
                    profile_name=INTEROP_PROFILE_NAME
                )

                decrypted = driver.decrypt(
                    case.encrypted, use_persisted_aad=True
                )

                self.assertEqual(
                    decrypted, case.value,
                    "Could not decrypt value encrypted by driver: "
                    f"{case.driver}"
                )
                driver.close()
        self._driver = None
