from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestConnectionAcquisitionTimeoutMs(TestkitTestCase):

    required_features = (
        types.Feature.BOLT_4_4,
        types.Feature.CONNECTION_ACQUISITION_TIMEOUT
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

    def test_should_encompass_the_handshake_time(self):
        self._server.start(
            self.script_path("session_run_auth_delay.script")
        )

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=2000,
                              connection_timeout_ms=720000)

        self._session = self._driver.session("w")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 as n"))

    def test_should_have_priotity_over_the_connection_timeout(self):
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")

        # Non routable address
        uri = "bolt://10.255.255.255"
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=2000,
                              connection_timeout_ms=720000)

        self._session = self._driver.session("w")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 as n"))

    def test_should_not_suppress_connection_timeout(self):
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")

        # Non routable address
        uri = "bolt://10.255.255.255"
        self._driver = Driver(self._backend, uri, auth,
                              connection_acquisition_timeout_ms=72000,
                              connection_timeout_ms=2000)

        self._session = self._driver.session("w")

        with self.assertRaises(types.DriverError):
            list(self._session.run("RETURN 1 as n"))

    @driver_feature(
        types.Feature.TMP_DRIVER_MAX_CONNECTION_POOL_SIZE,
        types.Feature.OPT_EAGER_TX_BEGIN
    )
    def test_should_regulates_the_time_for_acquiring_connections(self):
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
            self._driver.session("w"),
            self._driver.session("w"),
        ]

        self._txs = [self._sessions[0].begin_transaction()]

        with self.assertRaises(types.DriverError):
            self._txs.append(self._sessions[1].begin_transaction())
