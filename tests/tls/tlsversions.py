from tests.shared import get_driver_name, new_backend, TestkitTestCase
from tests.tls.shared import TlsServer, try_connect


class TestTlsVersions(TestkitTestCase):

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

    def test_1_1(self):
        if self._driver in ["dotnet", "python"]:
            self.skipTest("TLS 1.1 is not supported")

        self._server = TlsServer("trustedRoot_thehost", minTls="1", maxTls="1")
        self.assertTrue(try_connect(self._backend, self._server,
                                    "neo4j+s", "thehost"))

    def test_1_2(self):
        self._server = TlsServer("trustedRoot_thehost", minTls="2", maxTls="2")
        self.assertTrue(try_connect(self._backend, self._server,
                                    "neo4j+s", "thehost"))
