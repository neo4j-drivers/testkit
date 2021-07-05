from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestDirectConnectionRecvTimeout(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth)
        self._session = self._driver.session("w")
        self._last_exc = None

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        super().tearDown()

    def _start_server(self, script):
        self._server.start(path=self.script_path(script))

    def _assert_it_timout_exception(self, e):
        if get_driver_name() in ["python"]:
            self.assertEqual(e.errorType,
                             "<class 'neo4j.exceptions.ServiceUnavailable'>")

    def _on_failed_retry_assertions(self):
        pass

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_timeout(self):
        self._start_server("1_second_exceeds.script")
        with self.assertRaises(types.DriverError) as exc:
            self._session.run("RETURN 1 AS n")
        self._assert_it_timout_exception(exc.exception)
        response_after_sleep = bool(
            self._server.count_responses('SUCCESS {"type": "r"}')
        )
        self.assertFalse(response_after_sleep)

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_timeout_unmanaged_tx(self):
        self._start_server("1_second_exceeds_tx.script")
        tx = self._session.beginTransaction()
        with self.assertRaises(types.DriverError) as exc:
            tx.run("RETURN 1 AS n")
        self._assert_it_timout_exception(exc.exception)
        response_after_sleep = bool(
            self._server.count_responses('SUCCESS {"type": "r"}')
        )
        self.assertFalse(response_after_sleep)

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_timeout_managed_tx_retry(self):
        retries = 0
        record = None

        def work(tx):
            nonlocal retries
            nonlocal record
            retries += 1
            if retries == 1:
                with self.assertRaises(types.DriverError) as exc:
                    tx.run("RETURN 1 AS n")
                self._assert_it_timout_exception(exc.exception)
                self._on_failed_retry_assertions()
                raise exc.exception
            result = tx.run("RETURN %i AS n" % retries)
            record = result.next()
            self.assertIsInstance(result.next(), types.NullRecord)

        self._start_server("1_second_exceeds_tx_retry.script")
        self._session.writeTransaction(work)
        run_success_count = \
            self._server.count_responses('SUCCESS {"type": "r"}')
        self.assertEqual(retries, 2)
        self.assertEqual(run_success_count, 1)
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherInt(1)])

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_in_time(self):
        self._start_server("2_seconds_in_time.script")
        result = self._session.run("RETURN 1 AS n")
        record = result.next()
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherInt(1)])
        self.assertIsInstance(result.next(), types.NullRecord)
        self._server.done()

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_in_time_unmanaged_tx(self):
        self._start_server("2_seconds_in_time_tx.script")
        tx = self._session.beginTransaction()
        result = tx.run("RETURN 1 AS n")
        record = result.next()
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherInt(1)])
        self.assertIsInstance(result.next(), types.NullRecord)
        self._server.done()

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_in_time_managed_tx_retry(self):
        retries = 0
        record = None

        def work(tx):
            nonlocal retries
            nonlocal record
            retries += 1
            result = tx.run("RETURN %i AS n" % retries)
            record = result.next()
            self.assertIsInstance(result.next(), types.NullRecord)

        self._start_server("2_seconds_in_time_tx_retry.script")
        self._session.writeTransaction(work)
        self.assertEqual(retries, 1)
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherInt(1)])
        self._server.done()


class TestRoutingConnectionRecvTimeout(TestDirectConnectionRecvTimeout):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._router = StubServer(9000)
        self._router.start(path=self.script_path("router.script"),
                           vars={"#HOST#": self._router.host})
        auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                        credentials="pass")
        uri = "neo4j://%s" % self._router.address
        self._driver = Driver(self._backend, uri, auth)
        self._session = self._driver.session("w")
        self._last_exc = None

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        self._router.reset()
        super().tearDown()

    def _assert_it_timout_exception(self, e):
        if get_driver_name() in ["python"]:
            self.assertEqual(e.errorType,
                             "<class 'neo4j.exceptions.SessionExpired'>")
        else:
            super()._assert_it_timout_exception(e)

    def _on_failed_retry_assertions(self):
        rt = self._driver.getRoutingTable()
        self.assertEqual(rt.routers, [self._router.address])
        self.assertEqual(rt.readers, [])
        self.assertEqual(rt.writers, [])

    def _assert_routing_table(self, timed_out, managed):
        self.assertEqual(self._router.count_responses("<HANGUP>"), 0)
        self._router.done()
        self._server.reset()
        if timed_out and managed:
            self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
            self.assertEqual(self._router.count_requests("ROUTE"), 2)
        else:
            self.assertEqual(self._router.count_requests("ROUTE"), 1)
            self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._router.count_responses("<ACCEPT>"), 1)
        rt = self._driver.getRoutingTable()
        self.assertEqual(rt.routers, [self._router.address])
        if timed_out and not managed:
            self.assertEqual(rt.readers, [])
            self.assertEqual(rt.writers, [])
        else:
            self.assertEqual(rt.readers, [self._server.address])
            self.assertEqual(rt.writers, [self._server.address])

    def test_timeout(self):
        super().test_timeout()
        self._assert_routing_table(True, False)

    def test_timeout_unmanaged_tx(self):
        super().test_timeout_unmanaged_tx()
        self._assert_routing_table(True, False)

    def test_timeout_managed_tx_retry(self):
        super().test_timeout_managed_tx_retry()
        self._assert_routing_table(True, True)

    def test_in_time(self):
        super().test_in_time()
        self._assert_routing_table(False, False)

    def test_in_time_unmanaged_tx(self):
        super().test_in_time_unmanaged_tx()
        self._assert_routing_table(False, False)

    def test_in_time_managed_tx_retry(self):
        super().test_in_time_managed_tx_retry()
        self._assert_routing_table(False, True)
