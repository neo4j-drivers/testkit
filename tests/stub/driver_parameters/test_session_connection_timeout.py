from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestConnectionAcquisitionTimeoutMs(TestkitTestCase):
    """
    Connection Acquisition Timeout Tests.

    The connection acquisition timeout must account for the
    whole acquisition execution time, whether a new connection is created,
    an idle connection is picked up instead or we need to wait
    until the full pool depletes.

    In particular, the connection acquisition timeout (CAT) has precedence
    over the socket connection timeout (SCT).

    If the SCT is set to 2 hours and CAT to 50ms,
    the connection acquisition should time out after 50ms,
    even if the connection is successfully created within the SCT period.

    The CAT must NOT be replaced by the lowest of the two values (CAT and SCT).
    Indeed, even if SCT is lower than CAT, there could be situations
    where the pool takes longer to borrow an _idle_ connection than the SCT.
    Such a scenario should work as long as the overall acquisition happens
    within the CAT.
    This is unfortunately hard to translate into a test.
    """

    required_features = (
        types.Feature.BOLT_4_4,
        types.Feature.API_SESSION_CONNECTION_TIMEOUT
    )

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._router = StubServer(9000)
        self._driver = None
        self._session = None
        self._sessions = []
        self._txs = []

    def tearDown(self):
        self._server.reset()
        self._router.reset()
        for tx in self._txs:
            with self.assertRaises(types.DriverError):
                # The server does not accept ending the transaction.
                # We still call it to potentially free resources.
                tx.commit()

        for s in self._sessions:
            s.close()

        if self._session:
            self._session.close()

        if self._driver:
            self._driver.close()

        return super().tearDown()

    def _start_server(self, server, script):
        server.start(self.script_path(script),
                     vars_={"#HOST#": self._router.host})

    def test_should_work_when_every_step_is_done_in_time(self):
        self._start_server(self._server, "session_run_auth_delay.script")

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth,
                              session_connection_timeout_ms=10000)

        self._session = self._driver.session("r")

    def test_should_work_when_every_step_is_done_in_time_with_routing(self):
        self._start_server(self._server, "session_run_auth_delay.script")
        self._start_server(self._router, "router_route_delay.script")

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth,
                              session_connection_timeout_ms=10000)

        self._session = self._driver.session("r")

    def test_encompasses_router_connection_time(self):
        """Router connection times out."""
        uri = "neo4j://10.255.255.255"
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(self._backend, uri, auth,
                              session_connection_timeout_ms=2000)

        self._session = self._driver.session("r")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 as n"))

    def test_encompasses_router_handshake(self):
        """Router available but with delayed HELLO response."""
        self._start_server(self._router, "router_hello_delay.script")
        self._start_server(self._server, "session_run.script")

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(self._backend, uri, auth,
                              session_connection_timeout_ms=2000)

        self._session = self._driver.session("r")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 as n"))

        self._session.close()
        self._session = None
        self._driver.close()
        self._driver = None
        self._server.reset()
        reader_connections = self._server.count_responses("<ACCEPT>")
        self.assertEqual(0, reader_connections)

    def test_encompasses_router_route_response(self):
        """Router available but with delayed ROUTE response."""
        self._start_server(self._router, "router_route_delay.script")
        self._start_server(self._server, "session_run.script")

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(self._backend, uri, auth,
                              session_connection_timeout_ms=2000)

        self._session = self._driver.session("r")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 as n"))

        self._session.close()
        self._session = None
        self._driver.close()
        self._driver = None
        self._server.reset()
        reader_connections = self._server.count_responses("<ACCEPT>")
        self.assertEqual(0, reader_connections)

    def test_combined_router_and_reader_delay(self):
        """Slow but in time router + slow but in time router == too slow."""
        self._start_server(self._router, "router_hello_delay.script")
        self._start_server(self._server, "session_run_auth_delay.script")

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(self._backend, uri, auth,
                              session_connection_timeout_ms=6000)

        self._session = self._driver.session("r")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 as n"))

        self._session.close()
        self._session = None
        self._driver.close()
        self._driver = None
        self._server.reset()
        reader_connections = self._server.count_responses("<ACCEPT>")
        self.assertEqual(1, reader_connections)
