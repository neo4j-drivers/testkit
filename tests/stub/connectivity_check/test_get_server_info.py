from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import (
    StubScriptNotFinishedError,
    StubServer,
)


class TestGetServerInfo(TestkitTestCase):

    required_features = types.Feature.API_DRIVER_GET_SERVER_INFO,

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._server1 = StubServer(9010)
        self._server2 = StubServer(9020)
        self._server3 = StubServer(9030)
        self._server4 = StubServer(9040)
        self._server5 = StubServer(9050)
        self._server6 = StubServer(9060)

    def tearDown(self):
        self._router.reset()
        self._server1.reset()
        self._server2.reset()
        self._server3.reset()
        self._server4.reset()
        self._server5.reset()
        self._server6.reset()
        super().tearDown()

    def _build_result_check_cb(self, expected_port,
                               expected_agent="Neo4j/4.4.0"):
        def check(server_info):
            port = server_info.address.rsplit(":", 1)[1]
            self.assertEqual(port, str(expected_port))
            self.assertEqual(server_info.agent, expected_agent)
            self.assertEqual(server_info.protocol_version, "4.4")

        return check

    def _test_call(self, driver, result_check_cb=None):
        server_info = driver.get_server_info()
        if result_check_cb:
            result_check_cb(server_info)

    def _start_server(self, server, script, vars_=None):
        vars__ = {"#HOST#": self._router.host}
        if vars_:
            vars__.update(vars_)
        server.start(path=self.script_path(script), vars_=vars__)

    def _create_direct_driver(self):
        uri = "bolt://%s" % self._server1.address
        return Driver(
            self._backend, uri,
            types.AuthorizationToken("basic", principal="", credentials="")
        )

    def _create_routing_driver(self):
        uri = "neo4j://%s" % self._router.address
        return Driver(
            self._backend, uri,
            types.AuthorizationToken("basic", principal="", credentials="")
        )

    @contextmanager
    def _direct_driver(self, script=None):
        if script:
            self._start_server(self._server1, script)
        driver = self._create_direct_driver()
        try:
            yield driver
        finally:
            driver.close()

    @contextmanager
    def _routing_driver(self, scripts=None):
        if scripts is None:
            scripts = {}
        for server, script in scripts.items():
            self._start_server(server, script)
        driver = self._create_routing_driver()
        try:
            yield driver
        finally:
            driver.close()

    def test_direct_no_server(self):
        # driver should try to open a connection to the server
        with self._direct_driver() as driver:
            with self.assertRaises(types.DriverError):
                self._test_call(driver)

    def test_direct_raises_error(self):
        # driver should try to open a connection to the server
        with self._direct_driver("hello_auth_error.script") as driver:
            with self.assertRaises(types.DriverError) as exc:
                self._test_call(driver)
            self.assertEqual(exc.exception.code,
                             "Neo.ClientError.Security.Unauthorized")

    def test_direct(self):
        # driver should open a connection to the server
        with self._direct_driver("hello_only.script") as driver:
            self._test_call(driver,
                            self._build_result_check_cb(self._server1.port))
        self._server1.done()
        if self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            self.assertEqual(0, self._server1.count_requests("RESET"))

    def test_direct_from_pool(self):
        # driver should pick connection from the pool and send RESET
        with self._direct_driver("query_then_resets.script") as driver:
            session = driver.session("r")
            list(session.run("QUERY"))
            session.close()
            resets1 = self._server1.count_requests("RESET")
            self._test_call(driver,
                            self._build_result_check_cb(self._server1.port))
            resets2 = self._server1.count_requests("RESET")
            if self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
                self.assertEqual(resets1 + 1, resets2)
            else:
                self.assertGreater(resets2, resets1)
        self._server1.done()

    def test_routing_no_server(self):
        # driver should try to open a connection to the server
        with self._routing_driver() as driver:
            with self.assertRaises(types.DriverError):
                self._test_call(driver)

    def test_routing_raises_error(self):
        # driver should try to open a connection to the server
        with self._routing_driver({
            self._router: "router.script",
            self._server1: "hello_auth_error.script",
        }) as driver:
            with self.assertRaises(types.DriverError) as exc:
                self._test_call(driver)
            self.assertEqual(exc.exception.code,
                             "Neo.ClientError.Security.Unauthorized")

    def test_routing(self):
        # driver should open a connection to the server
        with self._routing_driver({
            self._router: "router.script",
            self._server1: "hello_only.script",
        }) as driver:
            self._test_call(driver,
                            self._build_result_check_cb(self._server1.port))

        self._router.done()
        self._server1.done()
        if self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            self.assertEqual(0, self._server1.count_requests("RESET"))

    def test_routing_from_pool(self):
        # driver should pick connection from the pool and send RESET
        with self._routing_driver({
            self._router: "router_multi.script",
            self._server1: "query_then_resets.script",
        }) as driver:
            session = driver.session("r")
            list(session.run("QUERY"))
            session.close()
            resets1 = self._server1.count_requests("RESET")
            self.assertEqual(self._router.count_requests("ROUTE"), 1)
            self._test_call(driver,
                            self._build_result_check_cb(self._server1.port))
            self.assertEqual(self._router.count_requests("ROUTE"), 2)
            resets2 = self._server1.count_requests("RESET")
            if self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
                self.assertEqual(resets1 + 1, resets2)
            else:
                self.assertGreater(resets2, resets1)
        self._router.done()
        self._server1.done()

    def test_routing_fetches_home_db(self):
        # driver should pick connection from the pool and send RESET
        with self._routing_driver({
            self._router: "router_changing_home_db.script",
            self._server1: "hello_only.script",
            self._server2: "hello_only.script",
        }) as driver:
            # resolves home db and received server1 as reader
            self._test_call(driver,
                            self._build_result_check_cb(self._server1.port))
            self._server1.done()
            # resolves home db again and received server2 as reader
            self._test_call(driver,
                            self._build_result_check_cb(self._server2.port))
            self._server2.done()
        self._router.done()

    def test_routing_tries_all_readers(self):
        # driver should pick connection from the pool and send RESET
        with self._routing_driver({
            self._router: "router_5_readers.script",
            self._server1: "handshake_only.script",
            self._server2: "handshake_only.script",
            self._server3: "handshake_only.script",
            self._server4: "handshake_only.script",
            self._server5: "handshake_only.script",
            self._server6: "handshake_only.script",
        }) as driver:
            # resolves home db and received server1 as reader
            with self.assertRaises(types.DriverError):
                self._test_call(driver)
        self._server1.done()
        self._server2.done()
        self._server3.done()
        self._server4.done()
        self._server5.done()
        with self.assertRaises(StubScriptNotFinishedError):
            # driver should not try to contact the writer
            self._server6.done()
        self._router.done()

    def test_routing_should_resolve_if_at_least_one_reader_is_up(self):
        with self._routing_driver({
            self._router: "router_5_readers.script",
            self._server3: "hello_only.script",
            self._server4: "hello_only.script"
        }) as driver:
            self._test_call(driver)

        # Should connect to one of the reader,
        # but not both
        try:
            self._server3.done()
        except StubScriptNotFinishedError:
            self._server4.done()
        else:
            with self.assertRaises(StubScriptNotFinishedError):
                self._server4.done()

        self._router.done()

    def test_routing_fail_when_no_reader_are_available(self):
        with self._routing_driver({
            self._router: "router_no_readers.script",
            self._server2: "hello_only.script",  # the writer
        }) as driver:
            # no readers are up
            with self.assertRaises(types.DriverError):
                self._test_call(driver)

        with self.assertRaises(StubScriptNotFinishedError):
            # driver should not try to contact the writer
            self._server2.done()
        self._router.done()
