from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestUpdateRoutingTableTimeoutMs(TestkitTestCase):

    required_features = (
        types.Feature.BOLT_5_0,
        types.Feature.API_UPDATE_ROUTING_TABLE_TIMEOUT,
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
        """Everything in time."""
        self._start_server(self._router, "router.script")
        self._start_server(self._server, "session_run.script")
        uri = "neo4j://10.255.255.255"
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")

        uri = "neo4j://%s" % self._router.address
        self._driver = Driver(self._backend, uri, auth,
                              update_routing_table_timeout_ms=2000)

        self._session = self._driver.session("r")

        list(self._session.run("RETURN 1 AS n"))

    def test_encompasses_router_connection_time(self):
        """Router connection times out."""
        uri = "neo4j://10.255.255.255"
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(self._backend, uri, auth,
                              update_routing_table_timeout_ms=2000)

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
                              update_routing_table_timeout_ms=2000)

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
                              update_routing_table_timeout_ms=2000)

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

    def test_does_not_encompass_reader_connection_time(self):
        self._start_server(self._router, "router.script")
        self._start_server(self._server, "session_run_auth_delay.script")

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(self._backend, uri, auth,
                              update_routing_table_timeout_ms=2000)

        self._session = self._driver.session("r")

        list(self._session.run("RETURN 1 AS n"))

        self._session.close()
        self._session = None
        self._driver.close()
        self._driver = None
        self._router.done()
        self._server.done()
