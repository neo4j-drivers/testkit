from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
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

    def tearDown(self) -> None:
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

    def test_should_work_when_every_step_is_done_in_time(self):
        """
        Everything in time scenario.

        This test scenario tests the case where:

        1. the connection acquisition timeout is higher than
            the connection creation timeout
        2. the connection is successfully created and in due time

        Then the query is executed successfully
        """
        self._start_server(self._server, "session_run_auth_delay.script")

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=10000,
                              connection_timeout_ms=5000)

        self._session = self._driver.session("r")

        list(self._session.run("RETURN 1 AS n"))

    def test_should_encompass_the_handshake_time(self):
        """
        Handshake takes longer scenario.

        This test scenario tests the case where:

        1. the connection acquisition timeout is smaller than
            the connection creation timeout
        2. the connection is successfully created and in due time
        3. the handshake takes longer than the connection acquisition timeout

        Then the query is not executed since the connection acquisition
        timed out.
        """
        self._start_server(self._server, "session_run_auth_delay.script")

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=2000,
                              connection_timeout_ms=720000)

        self._session = self._driver.session("r")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 AS n"))

    def test_should_fail_when_acquisition_timeout_is_reached_first(self):
        """
        Connection creation bigger than acquisition timeout scenario.

        This test scenario tests the case where:

        1. the connection acquisition timeout is smaller than
            the connection creation timeout
        2. the connection takes longer than the
            acquisition timeout to be created

        Then the query is not executed since the connection acquisition
        times out.
        """
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")

        # Non routable address
        uri = "bolt://10.255.255.255"
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=2000,
                              connection_timeout_ms=720000)

        self._session = self._driver.session("r")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 AS n"))

    def test_should_fail_when_connection_timeout_is_reached_first(self):
        """
        Acquisition timeout bigger than connection creation timeout scenario.

        This test scenario tests the case where:

        1. the connection acquisition timeout is bigger than
            the connection creation timeout
        2. the connection is successfully takes longer than the
            connection timeout to be created

        Then the query is not executed since the connection creation
        times out.
        """
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")

        # Non routable address
        uri = "bolt://10.255.255.255"
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=72000,
                              connection_timeout_ms=2000)

        self._session = self._driver.session("r")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 AS n"))

    def test_does_not_encompass_router_handshake(self):
        self._start_server(self._router, "router_hello_delay.script")
        self._start_server(self._server, "session_run.script")

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=2000,
                              connection_timeout_ms=720000)
        self._session = self._driver.session("r")
        list(self._session.run("RETURN 1 AS n"))

        self._session.close()
        self._session = None
        self._driver.close()
        self._driver = None
        self._router.done()
        self._server.done()

    def test_does_not_encompass_router_route_response(self):
        self._start_server(self._router, "router_route_delay.script")
        self._start_server(self._server, "session_run.script")

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=2000,
                              connection_timeout_ms=720000)
        self._session = self._driver.session("r")
        list(self._session.run("RETURN 1 AS n"))

        self._session.close()
        self._session = None
        self._driver.close()
        self._driver = None
        self._router.done()
        self._server.done()

    @driver_feature(types.Feature.OPT_EAGER_TX_BEGIN)
    def test_should_regulate_the_time_for_acquiring_connections(self):
        """
        No connection available scenario.

        This test scenario tests the case where:
        1. The connection pool is configured for max 1 connection
        2. A connection is acquired and locked by another transaction
        3. When the new session try to acquire a connection, the connection
           pool doesn't have connections available in suitable time

        Then the begin transaction is not executed
        since the connection acquisition times out.
        """
        self._start_server(self._server,
                           "tx_without_commit_or_rollback.script")

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=2000,
                              connection_timeout_ms=720000,
                              max_connection_pool_size=1)

        self._sessions = [
            self._driver.session("r"),
            self._driver.session("r"),
        ]

        self._txs = [self._sessions[0].begin_transaction()]

        with self.assertRaises(types.DriverError):
            self._txs.append(self._sessions[1].begin_transaction())
