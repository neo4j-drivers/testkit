import nutkit.protocol as types
from tests.shared import get_driver_name
from tests.tls.shared import (
    TestkitTlsTestCase,
    TlsServer,
)


class TestTlsVersions(TestkitTlsTestCase):

    def setUp(self):
        super().setUp()
        self._server = None
        self._driver = get_driver_name()

    def tearDown(self):
        if self._server:
            # If test raised an exception this will make sure that the stub
            # server is killed and it's output is dumped for analysis.
            self._server.reset()
            self._server = None
        super().tearDown()

    def _try_connect_smart(self):
        if self.driver_supports_features(types.Feature.API_SSL_SCHEMES):
            return super()._try_connect(self._server, "neo4j+s", "thehost")
        elif self.driver_supports_features(types.Feature.API_SSL_CONFIG):
            return super()._try_connect(self._server, "neo4j", "thehost",
                                        encrypted=True)
        self.skipTest("Needs support for either of %s" % ", ".join(
            map(lambda f: f.value,
                (types.Feature.API_SSL_SCHEMES, types.Feature.API_SSL_CONFIG))
        ))

    def test_1_1(self):
        if self._driver in ["dotnet"]:
            self.skipTest("TLS 1.1 is not supported")

        self._server = TlsServer("trustedRoot_thehost",
                                 min_tls="1", max_tls="1")
        if self.driver_supports_features(types.Feature.TLS_1_1):
            self.assertTrue(self._try_connect_smart())
        else:
            self.assertFalse(self._try_connect_smart())

    def test_1_2(self):
        self._server = TlsServer("trustedRoot_thehost",
                                 min_tls="2", max_tls="2")
        if self.driver_supports_features(types.Feature.TLS_1_2):
            self.assertTrue(self._try_connect_smart())
        else:
            self.assertFalse(self._try_connect_smart())

    def test_1_3(self):
        self._server = TlsServer("trustedRoot_thehost",
                                 min_tls="3", max_tls="3")
        if self.driver_supports_features(types.Feature.TLS_1_3):
            self.assertTrue(self._try_connect_smart())
        else:
            self.assertFalse(self._try_connect_smart())
