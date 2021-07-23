from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
    dns_resolve_single,
    get_dns_resolved_server_address
)
from tests.stub.shared import (
    StubServer,
)


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
        if self._session:
            self._session.close()
        if self._driver:
            self._driver.close()
        super().tearDown()

    def _start_server(self, script):
        self._server.start(path=self.script_path(script))

    def _assert_is_timout_exception(self, e):
        if get_driver_name() in ["python"]:
            self.assertEqual(e.errorType,
                             "<class 'neo4j.exceptions.ServiceUnavailable'>")

    def _on_failed_retry_assertions(self):
        pass

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_timeout(self):
        self._start_server("1_second_exceeds.script")
        with self.assertRaises(types.DriverError) as exc:
            result = self._session.run("timeout")
            # TODO It will be removed as soon as JS Driver
            # has async iterator api
            if get_driver_name() in ['javascript']:
                result.next()

        result = self._session.run("in time")
        # TODO It will be removed as soon as JS Driver
        # has async iterator api
        if get_driver_name() in ['javascript']:
            result.next()

        self._server.done()
        self._assert_is_timout_exception(exc.exception)
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
        self.assertEqual(self._server.count_responses("<HANGUP>"), 2)
        self.assertEqual(self._server.count_requests('RUN "timeout"'), 1)
        self.assertEqual(self._server.count_requests('RUN "in time"'), 1)

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_timeout_unmanaged_tx(self):
        self._start_server("1_second_exceeds_tx.script")
        tx = self._session.beginTransaction()
        with self.assertRaises(types.DriverError) as exc:
            result = tx.run("timeout")
            # TODO It will be removed as soon as JS Driver
            # has async iterator api
            if get_driver_name() in ['javascript']:
                result.next()

        tx = self._session.beginTransaction()
        res = tx.run("in time")
        res.next()
        tx.commit()
        self._server.done()
        self._assert_is_timout_exception(exc.exception)
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
        self.assertEqual(self._server.count_responses("<HANGUP>"), 2)
        self.assertEqual(self._server.count_requests('RUN "timeout"'), 1)
        self.assertEqual(self._server.count_requests('RUN "in time"'), 1)

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
                    result = tx.run("RETURN 1 AS n")
                    # TODO It will be removed as soon as JS Driver
                    # has async iterator api
                    if get_driver_name() in ['javascript']:
                        result.next()

                self._assert_is_timout_exception(exc.exception)
                self._on_failed_retry_assertions()
                raise exc.exception
            result = tx.run("RETURN %i AS n" % retries)
            record = result.next()
            self.assertIsInstance(result.next(), types.NullRecord)

        self._start_server("1_second_exceeds_tx_retry.script")
        self._session.writeTransaction(work)
        self._server.done()
        self.assertEqual(retries, 2)
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherInt(1)])
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
        self.assertEqual(self._server.count_responses("<HANGUP>"), 2)
        self.assertEqual(self._server.count_responses("<BROKEN>"), 1)

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_in_time(self):
        self._start_server("2_seconds_in_time.script")
        result = self._session.run("RETURN 1 AS n")
        record = result.next()
        self.assertIsInstance(result.next(), types.NullRecord)
        self._server.done()
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherInt(1)])
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._server.count_responses("<BROKEN>"), 0)

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_in_time_unmanaged_tx(self):
        self._start_server("2_seconds_in_time_tx.script")
        tx = self._session.beginTransaction()
        result = tx.run("RETURN 1 AS n")
        record = result.next()
        tx.commit()
        self.assertIsInstance(result.next(), types.NullRecord)
        self._server.done()
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherInt(1)])
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._server.count_responses("<BROKEN>"), 0)

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_in_time_managed_tx_retry(self):
        retries = 0
        record = None

        def work(tx):
            nonlocal retries
            nonlocal record
            retries += 1
            result = tx.run("RETURN 1 AS n")
            record = result.next()
            self.assertIsInstance(result.next(), types.NullRecord)

        self._start_server("2_seconds_in_time_tx_retry.script")
        self._session.writeTransaction(work)
        self._server.done()
        self.assertEqual(retries, 1)
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherInt(1)])
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._server.count_responses("<BROKEN>"), 0)


class TestRoutingConnectionRecvTimeout(TestDirectConnectionRecvTimeout):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._router = StubServer(9000)
        self._router.start(path=self.script_path("router.script"), vars={
            "#HOST#": dns_resolve_single(self._router.host)
        })
        auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                        credentials="pass")
        uri = "neo4j://%s" % self._router.address
        self._driver = Driver(self._backend, uri, auth)
        self._session = self._driver.session("w")
        self._last_exc = None

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._router.reset()
        super().tearDown()

    def _assert_is_timout_exception(self, e):
        if get_driver_name() in ["python"]:
            self.assertEqual(e.errorType,
                             "<class 'neo4j.exceptions.SessionExpired'>")
        else:
            super()._assert_is_timout_exception(e)

    def _on_failed_retry_assertions(self):
        rt = self._driver.getRoutingTable()
        self.assertEqual(rt.routers, [
            get_dns_resolved_server_address(self._router)
        ])
        self.assertEqual(rt.readers, [])
        self.assertEqual(rt.writers, [])

    def _assert_routing_table(self, timed_out, managed):
        self.assertEqual(self._router.count_responses("<HANGUP>"), 0)
        self._router.done()
        self._server.reset()
        if timed_out:
            self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
            self.assertEqual(self._router.count_requests("ROUTE"), 2)
        else:
            self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
            self.assertEqual(self._router.count_requests("ROUTE"), 1)
        self.assertEqual(self._router.count_responses("<ACCEPT>"), 1)
        rt = self._driver.getRoutingTable()
        self.assertEqual(rt.routers, [
            get_dns_resolved_server_address(self._router)
        ])
        self.assertEqual(rt.readers, [
            get_dns_resolved_server_address(self._server)
        ])
        self.assertEqual(rt.writers, [
            get_dns_resolved_server_address(self._server)
        ])

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
