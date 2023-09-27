from __future__ import annotations

import contextlib
import typing as t

import nutkit.protocol as types
from nutkit.frontend import (
    Driver,
    Session,
)
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestTelemetry(TestkitTestCase):

    required_features = (types.Feature.BOLT_5_4,)

    def setUp(self):
        super().setUp()
        self.stub_server = StubServer(9000)

    def tearDown(self):
        self.stub_server.reset()
        super().tearDown()

    def get_url(self):
        return "bolt://%s" % self.stub_server.address

    def start_server(self, server, script, vars_=None):
        server.start(self.script_path(script), vars_=vars_)

    def start_servers(self, script, vars_=None):
        self.start_server(self.stub_server, script, vars_=vars_)

    def servers_reset(self):
        self.stub_server.reset()

    def servers_done(self):
        self.stub_server.done()

    @contextlib.contextmanager
    def driver(self, **config) -> t.Generator[Driver, None, None]:
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        driver = Driver(self._backend, self.get_url(), auth, **config)
        try:
            yield driver
        finally:
            driver.close()

    @contextlib.contextmanager
    def session(
        self, driver: Driver, **config
    ) -> t.Generator[Session, None, None]:
        session = driver.session("r", **config)
        try:
            yield session
        finally:
            session.close()

    def _test_telemetry(self, work, telemetry_number):
        def test(server_telemetry_enabled_, driver_telemetry_disabled_):
            work(driver_telemetry_disabled_)

            self.servers_done()
            requests = self.stub_server.get_requests("TELEMETRY")
            if server_telemetry_enabled_ and not driver_telemetry_disabled_:
                self.assertEqual(len(requests), 1)
                self.assertEqual(requests[0], f"TELEMETRY {telemetry_number}")
            else:
                self.assertEqual(len(requests), 0)

        def server_vars(server_telemetry_enabled_):
            if server_telemetry_enabled_:
                return {"#SERVER_TELEMETRY_ENABLED#": "True"}
            else:
                return {"#SERVER_TELEMETRY_ENABLED#": "False"}

        for server_telemetry_enabled in (True, False):
            for driver_telemetry_disabled in (True, False, None):
                with self.subTest(
                    server_telemetry_enabled=server_telemetry_enabled,
                    driver_telemetry_disabled=driver_telemetry_disabled
                ):
                    self.start_servers("reader.script",
                                       server_vars(server_telemetry_enabled))
                    test(server_telemetry_enabled, driver_telemetry_disabled)
                self.servers_reset()

    def test_execute_read(self):
        def work(driver_telemetry_disabled_):
            def tx_func(tx):
                result = tx.run("RETURN 1 AS n")
                list(result)

            with self.driver(
                telemetry_disabled=driver_telemetry_disabled_
            ) as driver:
                with self.session(driver) as session:
                    session.execute_read(tx_func)

        self._test_telemetry(work, 0)

    def test_execute_write(self):
        def work(driver_telemetry_disabled_):
            def tx_func(tx):
                result = tx.run("RETURN 1 AS n")
                list(result)

            with self.driver(
                telemetry_disabled=driver_telemetry_disabled_
            ) as driver:
                with self.session(driver) as session:
                    session.execute_write(tx_func)

        self._test_telemetry(work, 0)

    def test_begin_transaction(self):
        def work(driver_telemetry_disabled_):
            with self.driver(
                telemetry_disabled=driver_telemetry_disabled_
            ) as driver:
                with self.session(driver) as session:
                    tx = session.begin_transaction()
                    try:
                        result = tx.run("RETURN 1 AS n")
                        list(result)
                        tx.commit()
                    finally:
                        tx.close()

        self._test_telemetry(work, 1)

    def test_session_run(self):
        def work(driver_telemetry_disabled_):
            with self.driver(
                telemetry_disabled=driver_telemetry_disabled_
            ) as driver:
                with self.session(driver) as session:
                    result = session.run("RETURN 1 AS n")
                    list(result)

        self._test_telemetry(work, 2)

    @driver_feature(types.Feature.API_DRIVER_EXECUTE_QUERY)
    def test_execute_query(self):
        def work(driver_telemetry_disabled_):
            with self.driver(
                telemetry_disabled=driver_telemetry_disabled_
            ) as driver:
                driver.execute_query("RETURN 1 AS n")

        self._test_telemetry(work, 3)

    def _test_telemetry_retry(self, work, telemetry_number):
        self.start_servers("reader_telemetry_retry.script")

        work()

        self.servers_done()
        requests = self.stub_server.get_requests("TELEMETRY")
        self.assertTrue(all(
            request == f"TELEMETRY {telemetry_number}"
            for request in requests
        ))

    def test_execute_read_retry(self):
        def work():
            def tx_func(tx):
                result = tx.run("RETURN 1 AS n")
                list(result)

            with self.driver() as driver:
                with self.session(driver) as session:
                    session.execute_read(tx_func)

        self._test_telemetry_retry(work, 0)

    def test_execute_write_retry(self):
        def work():
            def tx_func(tx):
                result = tx.run("RETURN 1 AS n")
                list(result)

            with self.driver() as driver:
                with self.session(driver) as session:
                    session.execute_write(tx_func)

        self._test_telemetry_retry(work, 0)

    @driver_feature(types.Feature.API_DRIVER_EXECUTE_QUERY)
    def test_execute_query_retry(self):
        def work():
            with self.driver() as driver:
                driver.execute_query("RETURN 1 AS n")

        self._test_telemetry_retry(work, 3)


class TestTelemetryRouting(TestTelemetry):

    def setUp(self):
        super().setUp()
        self.router = StubServer(9010)

    def tearDown(self):
        self.router.reset()
        super().tearDown()

    def get_url(self):
        return "neo4j://%s" % self.router.address

    def start_servers(self, script, vars_=None):
        router_vars = {
            "#SERVER_TELEMETRY_ENABLED#": "False",
            "#HOST#": self.router.host,
        }
        router_vars.update(vars_ or {})
        self.start_server(self.router, "router.script", vars_=router_vars)
        super().start_servers(script, vars_=vars_)

    def servers_reset(self):
        self.router.reset()
        super().servers_reset()

    def servers_done(self):
        self.router.done()
        super().servers_done()

    def test_execute_read(self):
        super().test_execute_read()

    def test_execute_write(self):
        super().test_execute_write()

    def test_begin_transaction(self):
        super().test_begin_transaction()

    def test_session_run(self):
        super().test_session_run()

    def test_execute_query(self):
        super().test_execute_query()

    def test_execute_read_retry(self):
        super().test_execute_read_retry()

    def test_execute_write_retry(self):
        super().test_execute_write_retry()

    def test_execute_query_retry(self):
        super().test_execute_query_retry()
