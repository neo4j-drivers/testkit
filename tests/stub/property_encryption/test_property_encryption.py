import random

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.property_encryption.deterministic_fixtures import (
    AES_128_KEY_ENCAPSULATION,
    AES_128_KEY_ID,
    AES_128_KEY_METADATA,
    DETERMINISTIC_ENCAPSULATION,
    DETERMINISTIC_KEK,
    DETERMINISTIC_KEY_ID,
    DETERMINISTIC_KEY_METADATA,
    DETERMINISTIC_PROFILE_NAME,
    DETERMINISTIC_TEST_CASES,
)
from tests.stub.shared import StubServer


def _with_mutated_profile_version(encrypted, version):
    # profile_version is the tiny-int byte right after the "ENVELOPE" string.
    mutated = bytearray(encrypted)
    mutated[mutated.index(b"ENVELOPE") + len(b"ENVELOPE")] = version
    return bytes(mutated)


def _with_mutated_type_encoding_scheme_major(encrypted, type_name, major):
    # type_encoding_scheme_major is the tiny-int byte right after type_name.
    packed_type_name = bytes([0x80 + len(type_name)]) + type_name.encode()
    mutated = bytearray(encrypted)
    mutated[mutated.index(packed_type_name) + len(packed_type_name)] = major
    return bytes(mutated)


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

    def _new_deterministic_driver(self):
        driver = self._new_driver(
            profiles=(
                {
                    "name": DETERMINISTIC_PROFILE_NAME,
                    "kek": DETERMINISTIC_KEK,
                },
            )
        )
        driver.import_encapsulated_key(
            DETERMINISTIC_KEY_ID, "k", DETERMINISTIC_ENCAPSULATION,
            DETERMINISTIC_KEY_METADATA,
            profile_name=DETERMINISTIC_PROFILE_NAME
        )
        return driver

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

    def test_decrypt_resolves_the_profile_from_the_encrypted_bytes(self):
        driver = self._new_driver(profiles=("p1", "p2"))
        driver.create_encapsulated_key("k1", profile_name="p2")

        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"),
            profile_name="p2", key_alias="k1"
        )
        decrypted = driver.decrypt(encrypted, use_persisted_aad=True)

        self.assertEqual(decrypted, types.CypherString("hello world"))

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

    def test_decrypt_raises_on_unsupported_profile_version(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )

        for bad_version in (0, 2, 255):
            with self.subTest(version=bad_version):
                with self.assertRaises(types.DriverError):
                    driver.decrypt(
                        _with_mutated_profile_version(encrypted, bad_version),
                        use_persisted_aad=True
                    )

    def _encrypt_with_a_newer_type_encoding_scheme(self, driver):
        encrypted = driver.encrypt_to_bytes(
            types.CypherString("from-the-future"),
            aad=types.CypherString("row-42"),
            key_alias="k1"
        )
        return _with_mutated_type_encoding_scheme_major(
            encrypted, "STRING", 0x7F
        )

    def test_decrypt_returns_unsupported_type_for_a_newer_type_encoding(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")
        encrypted = self._encrypt_with_a_newer_type_encoding_scheme(driver)

        decrypted = driver.decrypt(
            encrypted, aad=types.CypherString("row-42")
        )

        self.assertIsInstance(decrypted, types.CypherUnsupportedType)
        self.assertEqual(decrypted.name, "STRING")

    def test_decrypt_authenticates_before_returning_unsupported_type(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")
        encrypted = self._encrypt_with_a_newer_type_encoding_scheme(driver)

        with self.assertRaises(types.DriverError):
            driver.decrypt(encrypted, aad=types.CypherString("row-999"))

    def test_decrypt_raises_on_bytes_trailing_the_structure(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")
        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )

        with self.assertRaises(types.DriverError):
            driver.decrypt(encrypted + b"\x01", use_persisted_aad=True)

    def test_decrypt_raises_on_a_structure_with_an_extra_field(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")
        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )
        # Byte 0 is the encoding version; byte 1 is the 8-field header 0xB8.
        nine_fields = bytearray(encrypted + b"\x01")
        nine_fields[1] = 0xB9

        with self.assertRaises(types.DriverError):
            driver.decrypt(bytes(nine_fields), use_persisted_aad=True)

    def test_decrypt_raises_on_truncated_bytes(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")
        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )

        for length in (0, 1, 2, len(encrypted) // 2, len(encrypted) - 1):
            with self.subTest(length=length):
                with self.assertRaises(types.DriverError):
                    driver.decrypt(
                        encrypted[:length], use_persisted_aad=True
                    )

    def test_decrypt_raises_on_an_unknown_encoding_version(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")
        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )

        for version in (0x00, 0x02, 0xFF):
            with self.subTest(version=version):
                with self.assertRaises(types.DriverError):
                    driver.decrypt(
                        bytes([version]) + encrypted[1:],
                        use_persisted_aad=True
                    )

    def test_decrypt_raises_on_an_unknown_profile_type(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")
        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )

        with self.assertRaises(types.DriverError):
            driver.decrypt(
                encrypted.replace(b"ENVELOPE", b"ENVELOPX", 1),
                use_persisted_aad=True
            )

    def test_decrypt_raises_when_the_driver_lacks_the_values_profile(self):
        driver_1 = self._new_driver(profiles=("p1",))
        driver_1.create_encapsulated_key("k1")
        encrypted = driver_1.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )
        driver_1.close()

        driver_2 = self._new_driver(profiles=("p2",))

        with self.assertRaises(types.DriverError):
            driver_2.decrypt(encrypted, use_persisted_aad=True)

    _DISALLOWED_AADS = (
        types.CypherNull(),
        types.CypherFloat(1.5),
        types.CypherList([types.CypherString("row-42")]),
        types.CypherDuration(1, 2, 3, 4),
        types.CypherDateTime(2026, 1, 2, 3, 4, 5, 6),
        types.CypherDateTime(2026, 1, 2, 3, 4, 5, 6, utc_offset_s=3600),
        types.CypherDateTime(
            2026, 1, 2, 3, 4, 5, 6, utc_offset_s=3600,
            timezone_id="Europe/Stockholm"
        ),
        types.CypherVector("i8", b"\x01"),
    )

    def test_encrypt_raises_on_a_disallowed_aad_type(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        for aad in self._DISALLOWED_AADS:
            with self.subTest(aad=aad):
                with self.assertRaises(types.DriverError):
                    driver.encrypt_to_bytes(
                        types.CypherString("hello world"),
                        aad=aad, key_alias="k1"
                    )

    def test_decrypt_raises_on_a_disallowed_aad_type(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")
        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )

        for aad in self._DISALLOWED_AADS:
            with self.subTest(aad=aad):
                with self.assertRaises(types.DriverError):
                    driver.decrypt(encrypted, aad=aad)

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

    def test_create_raises_when_the_repository_rejects_the_alias(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        with self.assertRaises(types.DriverError):
            driver.create_encapsulated_key("k1")

    def test_encrypt_raises_on_unknown_key_id(self):
        driver = self._new_driver()

        with self.assertRaises(types.DriverError):
            driver.encrypt_to_bytes(
                types.CypherString("hello world"), key_id="no-such-id"
            )

    def test_encrypt_raises_when_ambiguous_and_no_profile_given(self):
        driver = self._new_driver(profiles=("p1", "p2"))
        driver.create_encapsulated_key("k1", profile_name="p1")

        with self.assertRaises(types.DriverError):
            driver.encrypt_to_bytes(
                types.CypherString("hello world"), key_alias="k1"
            )

    def test_raises_when_two_profiles_share_a_name(self):
        # Both statements are inside the block because the ADR requires
        # profile names to be unique but says nothing about when a driver
        # must reject a duplicate, so rejecting at construction and
        # rejecting at first use are both conforming. The profile is named
        # so that a duplicate is the only thing left to fail on; omitting it
        # would also be ambiguous, and the test would pass either way.
        with self.assertRaises(types.DriverError):
            driver = self._new_driver(profiles=("dup", "dup"))
            driver.create_encapsulated_key("k1", profile_name="dup")

    def test_key_manager_raises_when_ambiguous_and_no_profile_given(self):
        driver = self._new_driver(profiles=("p1", "p2"))

        with self.assertRaises(types.DriverError):
            driver.create_encapsulated_key("k1")

    def test_imported_key_decrypts_with_a_fixed_kek(self):
        driver_1 = self._new_deterministic_driver()
        encrypted = driver_1.encrypt_to_bytes(
            types.CypherString("hello world"),
            profile_name=DETERMINISTIC_PROFILE_NAME, key_alias="k"
        )
        driver_1.close()

        driver_2 = self._new_deterministic_driver()
        decrypted = driver_2.decrypt(encrypted, use_persisted_aad=True)

        self.assertEqual(decrypted, types.CypherString("hello world"))

    def test_imports_into_the_sole_profile_when_no_profile_is_named(self):
        driver = self._new_driver(
            profiles=(
                {
                    "name": DETERMINISTIC_PROFILE_NAME,
                    "kek": DETERMINISTIC_KEK,
                },
            )
        )
        driver.import_encapsulated_key(
            DETERMINISTIC_KEY_ID, "k", DETERMINISTIC_ENCAPSULATION,
            DETERMINISTIC_KEY_METADATA
        )

        case = DETERMINISTIC_TEST_CASES[0]
        decrypted = driver.decrypt(case.encrypted, use_persisted_aad=True)

        self.assertEqual(decrypted, case.value)

    def test_an_unconsumed_fixed_iv_is_replaced_by_the_next_one(self):
        driver = self._new_deterministic_driver()
        case = DETERMINISTIC_TEST_CASES[0]

        with self.assertRaises(types.DriverError):
            driver.encrypt_to_bytes(
                case.value, profile_name=DETERMINISTIC_PROFILE_NAME,
                key_alias="no-such-key", iv=case.iv
            )

        encrypted = driver.encrypt_to_bytes(
            case.value, profile_name=DETERMINISTIC_PROFILE_NAME,
            key_alias="k", iv=case.iv, aad=case.aad
        )

        self.assertEqual(encrypted, case.encrypted)

    def test_encrypts_to_known_bytes(self):
        driver = self._new_deterministic_driver()

        for case in DETERMINISTIC_TEST_CASES:
            with self.subTest(value=case.value):
                encrypted = driver.encrypt_to_bytes(
                    case.value, profile_name=DETERMINISTIC_PROFILE_NAME,
                    key_alias="k", iv=case.iv, aad=case.aad
                )
                self.assertEqual(encrypted, case.encrypted)

    def test_decrypts_known_bytes(self):
        driver = self._new_deterministic_driver()

        for case in DETERMINISTIC_TEST_CASES:
            with self.subTest(value=case.value):
                decrypted = driver.decrypt(
                    case.encrypted, use_persisted_aad=True
                )
                self.assertEqual(decrypted, case.value)

    def test_decrypts_known_bytes_with_an_explicit_aad(self):
        driver = self._new_deterministic_driver()

        for case in DETERMINISTIC_TEST_CASES:
            if case.aad is None:
                continue
            with self.subTest(aad=case.aad):
                decrypted = driver.decrypt(case.encrypted, aad=case.aad)
                self.assertEqual(decrypted, case.value)

    def test_encrypt_raises_when_the_decapsulated_key_is_not_aes_256(self):
        driver = self._new_deterministic_driver()
        driver.import_encapsulated_key(
            AES_128_KEY_ID, "aes-128", AES_128_KEY_ENCAPSULATION,
            AES_128_KEY_METADATA, profile_name=DETERMINISTIC_PROFILE_NAME
        )

        with self.assertRaises(types.DriverError):
            driver.encrypt_to_bytes(
                types.CypherString("hello world"),
                profile_name=DETERMINISTIC_PROFILE_NAME, key_alias="aes-128"
            )

    def test_raises_on_disallowed_value(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        for disallowed_value in [
            types.CypherMap({"string": types.CypherString("hello")}),
            types.CypherList([
                types.CypherTime(0, 0, 0, 0),
                types.CypherTime(0, 0, 0, 0, utc_offset_s=60 * 60 * 1)
            ]),
            types.CypherList([
                types.CypherPoint("cartesian", 1, 1),
                types.CypherPoint("cartesian", 1, 1, 1)
            ]),
            types.CypherList([
                types.CypherPoint("cartesian", 1, 1),
                types.CypherPoint("wgs84", 1, 1)
            ]),
            types.CypherList([types.CypherVector("i8", b"\x01")]),
            types.CypherList([
                types.CypherList([types.CypherString("hello")])
            ]),
            types.CypherList([
                types.CypherMap({"string": types.CypherString("hello")})
            ]),
        ]:
            with self.subTest(value=disallowed_value):
                with self.assertRaises(types.DriverError):
                    driver.encrypt_to_bytes(
                        disallowed_value,
                        key_alias="k1"
                    )
