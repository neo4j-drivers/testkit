import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    get_driver_name,
)
from tests.tls.shared import (
    TestkitTlsTestCase,
    TlsServer,
)

schemes = ["neo4j", "bolt"]


class TestUnsecureScheme(TestkitTlsTestCase):
    # Tests URL scheme neo4j/bolt where TLS is not used. The fact that driver
    # can not connect to a TLS server with this configuration is less
    # interesting than the error handling when this happens, the driver backend
    # should "survive" (without special hacks in it).

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

    def test_driver_is_not_encrypted(self):
        for scheme in schemes:
            with self.subTest(scheme=scheme):
                self._test_reports_encrypted(False, scheme)

    def test_secure_server(self):
        for scheme in schemes:
            with self.subTest(scheme=scheme):
                self._server = TlsServer("trustedRoot_thehost")
                with self._make_driver(scheme, "thehost") as driver:
                    self.assertFalse(self._try_connect(
                        self._server, driver
                    ))
            self._server.reset()

    @driver_feature(types.Feature.API_SSL_CONFIG)
    def test_secure_server_explicitly_disabled_encryption(self):
        for scheme in schemes:
            with self.subTest(scheme=scheme):
                self._server = TlsServer("trustedRoot_thehost")
                with self._make_driver(
                    scheme, "thehost", encrypted=False
                ) as driver:
                    self.assertFalse(self._try_connect(
                        self._server, driver
                    ))
            self._server.reset()
