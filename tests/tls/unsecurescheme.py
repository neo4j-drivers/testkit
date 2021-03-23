from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.tls.shared import (
    TlsServer,
    try_connect,
)


schemes = ["neo4j", "bolt"]


class TestUnsecureScheme(TestkitTestCase):
    """ Tests URL scheme neo4j/bolt where TLS is not used. The fact that driver can not connect
    to a TLS server with this configuration is less interesting than the error handling when
    this happens, the driver backend should "survive" (without special hacks in it).
    """
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

    def test_secure_server(self):
        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("trustedRoot_thehost")
                self.assertFalse(try_connect(self._backend, self._server, scheme, "thehost"))

