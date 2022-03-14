from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestConnectionAcquisitionTimeoutMs(TestkitTestCase):
    """
    Connection Acquition Timeout Tests.

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
        types.Feature.API_CONNECTION_ACQUISITION_TIMEOUT
    )

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._driver = None
        self._session = None
        self._sessions = []
        self._txs = []

    def tearDown(self) -> None:
        self._server.reset()
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

    def test_should_work_when_every_step_is_done_in_time(self):
        """
        Everything in time scenario.

        This test scenario tests the case where:

        1. the connection acquisition timeout is higher than
            the connection creation timeout
        2. the connection is successfully created and in due time

        Then the query is executed successfully
        """
        self._server.start(
            self.script_path("session_run_auth_delay.script")
        )

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=10000,
                              connection_timeout_ms=5000)

        self._session = self._driver.session("r")

        list(self._session.run("RETURN 1 as n"))

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
        self._server.start(
            self.script_path("session_run_auth_delay.script")
        )

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=2000,
                              connection_timeout_ms=720000)

        self._session = self._driver.session("r")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 as n"))

    def test_should_fail_when_acquisition_timeout_is_reached_first(self):
        """
        Connection creation bigger then acquisition timeout scenario.

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
            list(self._session.run("RETURN 1 as n"))

    def test_should_fail_when_connection_timeout_is_reached_first(self):
        """
        Acquisition timeout bigger then connection creation timeout scenario.

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
            list(self._session.run("RETURN 1 as n"))

    @driver_feature(
        types.Feature.TMP_DRIVER_MAX_CONNECTION_POOL_SIZE,
        types.Feature.OPT_EAGER_TX_BEGIN
    )
    def test_should_regulate_the_time_for_acquiring_connections(self):
        """
        No connection available scenario.

        This test scenario tests the case where:

        1. the connection acquisition timeout is higher than
            the connection creation timeout
        2. the connection is successfully created and in due time
        3. the connection pool doesn't have connections available in
            suitable time

        Then the begin transaction is not executed
        since the connection acquisition times out.
        """
        self._server.start(
            self.script_path("tx_without_commit_or_rollback.script")
        )

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
