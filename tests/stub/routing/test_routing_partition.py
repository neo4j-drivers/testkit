import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class RoutingPartition(TestkitTestCase):
    required_features = types.Feature.BOLT_5_8,

    def setUp(self):
        super().setUp()
        self._server1 = StubServer(9001)
        self._server2 = StubServer(9002)
        self._server3 = StubServer(9003)
        self._server4 = StubServer(9004)
        self._server5 = StubServer(9005)
        self._uri_template = "neo4j://%s:%d"
        self._auth = types.AuthorizationToken(
            "basic", principal="p", credentials="c"
        )

    def tearDown(self):
        self._reset_servers()
        super().tearDown()

    def _start_servers(self, partition2=True):
        vars_ = {
            "#HOST#": self._server1.host,
        }

        script_p1 = self.script_path("partitioning", "partition_1.script")
        script_p2 = self.script_path("partitioning", "partition_2.script")
        self._server1.start(script_p1, vars_=vars_)
        self._server2.start(script_p1, vars_=vars_)
        if not partition2:
            return
        self._server3.start(script_p2, vars_=vars_)
        self._server4.start(script_p2, vars_=vars_)
        self._server5.start(script_p2, vars_=vars_)

    def _stop_servers(self, partition2=True):
        self._server1.done(ignore_never_started=True)
        self._server2.done(ignore_never_started=True)
        if not partition2:
            return
        self._server3.done(ignore_never_started=True)
        self._server4.done(ignore_never_started=True)
        self._server5.done(ignore_never_started=True)

    def _reset_servers(self):
        self._server1.reset()
        self._server2.reset()
        self._server3.reset()
        self._server4.reset()
        self._server5.reset()

    def _driver(self):
        uri = "neo4j://127.0.0.1:12345"

        def resolver(address):
            return [
                self._server1.address,
                self._server3.address,
            ]

        return Driver(
            self._backend,
            uri,
            self._auth,
            resolver_fn=resolver,
        )

    def _session_run(self, driver):
        with driver.session("w", database="neo4j") as session:
            result = session.run("RETURN 1 AS n")
            result.consume()

    def _explicit_transaction(self, driver):
        with driver.session("w", database="neo4j") as session:
            with session.begin_transaction() as tx:
                result = tx.run("RETURN 1 AS n")
                result.consume()

    def _managed_transaction(self, driver):
        def work(tx):
            result = tx.run("RETURN 1 AS n")
            result.consume()

        with driver.session("w", database="neo4j") as session:
            session.execute_write(work)

    def _execute_query(self, driver):
        driver.execute_query(
            "RETURN 1 AS n",
            routing="w",
            database="neo4j",
        )

    def test_routing_partitioning(self):
        for work in (
                self._session_run,
                self._explicit_transaction,
                self._managed_transaction,
                self._execute_query,
        ):
            with self.subTest(work=work.__name__):
                try:
                    self._start_servers()

                    with self._driver() as driver:
                        work(driver)

                    self._stop_servers()
                    p1_route_call_count = (
                        self._server1.count_requests("ROUTE")
                        + self._server2.count_requests("ROUTE")
                    )
                    p2_route_call_count = (
                        self._server3.count_requests("ROUTE")
                        + self._server4.count_requests("ROUTE")
                        + self._server5.count_requests("ROUTE")
                    )
                    run_call_count = (
                        self._server1.count_requests("RUN")
                        + self._server2.count_requests("RUN")
                        + self._server3.count_requests("RUN")
                        + self._server4.count_requests("RUN")
                        + self._server5.count_requests("RUN")
                    )
                    self.assertEqual(p1_route_call_count, 1)
                    self.assertEqual(p2_route_call_count, 1)
                    self.assertEqual(run_call_count, 1)
                finally:
                    self._reset_servers()

    def test_routing_partitioning_read_only(self):
        for work in (
            self._session_run,
            self._explicit_transaction,
            self._managed_transaction,
            self._execute_query,
        ):
            with self.subTest(work=work.__name__):
                try:
                    self._start_servers(partition2=False)

                    with self._driver() as driver:
                        work(driver)

                    self._stop_servers(partition2=False)
                    route_call_count = (
                        self._server1.count_requests("ROUTE")
                        + self._server2.count_requests("ROUTE")
                    )
                    run_call_count = (
                        self._server1.count_requests("RUN")
                        + self._server2.count_requests("RUN")
                    )
                    self.assertEqual(route_call_count, 1)
                    self.assertEqual(run_call_count, 1)
                finally:
                    self._reset_servers()
