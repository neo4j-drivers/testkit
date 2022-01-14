from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    dns_resolve_single,
    driver_feature,
    get_dns_resolved_server_address,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestDirectConnectionRecvTimeout(TestkitTestCase):

    required_features = types.Feature.BOLT_4_3,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        auth = types.AuthorizationToken("basic", principal="neo4j",
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

    def _assert_is_timeout_exception(self, e):
        if get_driver_name() in ["python"]:
            self.assertEqual("<class 'neo4j.exceptions.ServiceUnavailable'>",
                             e.errorType)
        elif get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.ConnectionReadTimeoutException",
                e.errorType)
        elif get_driver_name() in ["go"]:
            self.assertIn("i/o timeout", e.msg)
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::ConnectionReadTimeoutException",
                e.errorType)

    def _assert_is_client_exception(self, e):
        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.ClientException",
                e.errorType)
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::ClientException",
                e.errorType)

    def _on_failed_retry_assertions(self):
        pass

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_timeout(self):
        self._start_server("1_second_exceeds.script")
        with self.assertRaises(types.DriverError) as exc:
            result = self._session.run("timeout")
            # TODO It will be removed as soon as JS Driver
            # has async iterator api
            if get_driver_name() in ["javascript"]:
                result.next()

        result = self._session.run("in time")
        # TODO It will be removed as soon as JS Driver
        # has async iterator api
        if get_driver_name() in ["javascript"]:
            result.next()

        self._server.done()
        self._assert_is_timeout_exception(exc.exception)
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
        self.assertEqual(self._server.count_responses("<HANGUP>"), 2)
        self.assertEqual(self._server.count_requests('RUN "timeout"'), 1)
        self.assertEqual(self._server.count_requests('RUN "in time"'), 1)

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_timeout_unmanaged_tx(self):
        self._start_server("1_second_exceeds_tx.script")
        tx = self._session.begin_transaction()
        with self.assertRaises(types.DriverError) as exc:
            result = tx.run("timeout")
            # TODO It will be removed as soon as JS Driver
            # has async iterator api
            if get_driver_name() in ["javascript"]:
                result.next()
        tx.close()
        # TODO Remove when explicit rollback requirement is removed
        if get_driver_name() in ["java", "ruby"]:
            tx.rollback()

        tx = self._session.begin_transaction()
        res = tx.run("in time")
        res.next()
        tx.commit()
        self._server.done()
        self._assert_is_timeout_exception(exc.exception)
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
        self.assertEqual(self._server.count_responses("<HANGUP>"), 2)
        self.assertEqual(self._server.count_requests('RUN "timeout"'), 1)
        self.assertEqual(self._server.count_requests('RUN "in time"'), 1)

    @driver_feature(types.Feature.CONF_HINT_CON_RECV_TIMEOUT)
    def test_timeout_unmanaged_tx_should_fail_subsequent_usage_after_timeout(
            self):
        self._start_server("1_second_exceeds_tx.script")
        tx = self._session.begin_transaction()
        with self.assertRaises(types.DriverError) as first_run_error:
            result = tx.run("timeout")
            # TODO It will be removed as soon as JS Driver
            # has async iterator api
            if get_driver_name() in ["javascript"]:
                result.next()

        with self.assertRaises(types.DriverError) as second_run_error:
            result = tx.run("in time")
            if get_driver_name() in ["javascript"]:
                result.next()

        tx.close()

        # TODO Remove when explicit rollback requirement is removed
        if get_driver_name() in ["java", "ruby"]:
            tx.rollback()

        self._server.done()
        self._assert_is_timeout_exception(first_run_error.exception)
        self._assert_is_client_exception(second_run_error.exception)
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._server.count_responses("<HANGUP>"), 1)
        self.assertEqual(self._server.count_requests('RUN "timeout"'), 1)
        self.assertEqual(self._server.count_requests('RUN "in time"'), 0)

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
                    if get_driver_name() in ["javascript"]:
                        result.next()

                self._assert_is_timeout_exception(exc.exception)
                self._on_failed_retry_assertions()
                raise exc.exception
            result = tx.run("RETURN %i AS n" % retries)
            record = result.next()
            self.assertIsInstance(result.next(), types.NullRecord)

        self._start_server("1_second_exceeds_tx_retry.script")
        self._session.write_transaction(work)
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
        tx = self._session.begin_transaction()
        result = tx.run("RETURN 1 AS n")
        records = []
        while True:
            next_record = result.next()
            if isinstance(next_record, types.NullRecord):
                break
            records.append(next_record)
        tx.commit()
        self.assertEqual(1, len(records))
        self._server.done()
        self.assertIsInstance(records[0], types.Record)
        self.assertEqual(records[0].values, [types.CypherInt(1)])
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
        self._session.write_transaction(work)
        self._server.done()
        self.assertEqual(retries, 1)
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherInt(1)])
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._server.count_responses("<BROKEN>"), 0)


class TestRoutingConnectionRecvTimeout(TestDirectConnectionRecvTimeout):
    def setUp(self):
        TestkitTestCase.setUp(self)
        self._server = StubServer(9010)
        self._router = StubServer(9000)
        self._router.start(path=self.script_path("router.script"), vars_={
            "#HOST#": dns_resolve_single(self._router.host)
        })
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "neo4j://%s:%s" % (dns_resolve_single(self._router.host),
                                 self._router.port)
        self._driver = Driver(self._backend, uri, auth)
        self._session = self._driver.session("w")
        self._last_exc = None

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        self._router.reset()
        if self._session:
            self._session.close()
        if self._driver:
            self._driver.close()
        TestkitTestCase.tearDown(self)

    def _assert_is_timeout_exception(self, e):
        if get_driver_name() in ["python"]:
            self.assertEqual("<class 'neo4j.exceptions.SessionExpired'>",
                             e.errorType)
        elif get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.SessionExpiredException",
                e.errorType)
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::SessionExpiredException",
                e.errorType)
        else:
            super()._assert_is_timeout_exception(e)

    def _on_failed_retry_assertions(self):
        rt = self._driver.get_routing_table()
        self.assertEqual(rt.routers, [
            get_dns_resolved_server_address(self._router)
        ])
        self.assertEqual(rt.readers, [])
        self.assertEqual(rt.writers, [])

    def _assert_routing_table(self, timed_out, managed):
        if self.driver_supports_features(types.Feature.OPT_CONNECTION_REUSE):
            self.assertEqual(self._router.count_responses("<HANGUP>"), 0)

        self._router.done()
        self._server.reset()
        if timed_out:
            self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
            self.assertEqual(self._router.count_requests("ROUTE"), 2)
        else:
            self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
            self.assertEqual(self._router.count_requests("ROUTE"), 1)

        if self.driver_supports_features(types.Feature.OPT_CONNECTION_REUSE):
            self.assertLessEqual(self._router.count_responses("<ACCEPT>"), 1)

        rt = self._driver.get_routing_table()
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
