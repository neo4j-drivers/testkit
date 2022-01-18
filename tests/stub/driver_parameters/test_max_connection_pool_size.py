from contextlib import contextmanager

from nutkit.backend.backend import backend_timeout_adjustment
from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestMaxConnectionPoolSize(TestkitTestCase):

    required_features = types.Feature.BOLT_4_4,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._server.start(
            self.script_path("tx_without_commit_or_rollback.script")
        )
        self._driver = None
        self._sessions = []
        self._transactions = []
        self._last_exc = None

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        for tx in self._transactions:
            with self.assertRaises(types.DriverError):
                # The server does not accept ending the transaction.
                # We still call it to potentially free resources.
                tx.commit()
        for s in self._sessions:
            s.close()
        if self._driver:
            self._driver.close()
        super().tearDown()

    def _open_driver(self, max_pool_size=None):
        assert self._driver is None
        kwargs = {}
        if self.driver_supports_features(
            types.Feature.TMP_CONNECTION_ACQUISITION_TIMEOUT
        ):
            kwargs["connection_acquisition_timeout_ms"] = 500
        if max_pool_size is not None:
            kwargs["max_connection_pool_size"] = max_pool_size
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth, **kwargs)

    @contextmanager
    def _backend_timeout_adjustment(self):
        if self.driver_supports_features(
            types.Feature.TMP_CONNECTION_ACQUISITION_TIMEOUT
        ):
            yield
        else:
            with backend_timeout_adjustment(self._backend, 70):
                yield

    def _open_connections(self, n):
        for _ in range(n):
            self._sessions.append(self._driver.session("r"))
            self._transactions.append(
                self._sessions[-1].begin_transaction()
            )
            list(self._transactions[-1].run("RETURN 1 AS n"))

    def test_connection_pool_maxes_out_at_100_by_default(self):
        self._open_driver()
        self._open_connections(100)
        with self._backend_timeout_adjustment():
            with self.assertRaises(types.DriverError):
                self._open_connections(1)
        self.assertEqual(self._server.count_responses("<HANGUP>"), 0)
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 100)

    @driver_feature(types.Feature.TMP_DRIVER_MAX_CONNECTION_POOL_SIZE)
    def test_connection_pool_custom_max_size(self):
        self._open_driver(2)
        self._open_connections(2)
        with self._backend_timeout_adjustment():
            with self.assertRaises(types.DriverError):
                self._open_connections(1)
        self.assertEqual(self._server.count_responses("<HANGUP>"), 0)
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
