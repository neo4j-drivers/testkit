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

    def test_encrypts_and_decrypts_hello_world(self):
        driver = self._new_driver()
        driver.create_encapsulated_key("k1")

        encrypted = driver.encrypt_to_bytes(
            types.CypherString("hello world"), key_alias="k1"
        )
        decrypted = driver.decrypt(encrypted, use_persisted_aad=True)

        self.assertEqual(decrypted, types.CypherString("hello world"))
