import time
from contextlib import contextmanager

import nutkit.protocol as types
from nutkit.frontend import (
    Driver,
    FakeTime,
)
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TimeoutManager:
    def __init__(
        self, test_case: TestkitTestCase, timeout_ms: int,
        use_real_timers: bool = False
    ):
        self._timeout_ms = timeout_ms
        self._fake_time = None
        if test_case.driver_supports_features(
            types.Feature.BACKEND_MOCK_TIME
        ) and not use_real_timers:
            self._fake_time = FakeTime(test_case._backend)

    def __enter__(self):
        if self._fake_time:
            self._fake_time.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._fake_time:
            self._fake_time.__exit__(exc_type, exc_val, exc_tb)

    def tick_to_before_timeout(self):
        if self._fake_time:
            self._fake_time.tick(self._timeout_ms - 1)

    def tick_to_after_timeout(self):
        if self._fake_time:
            self._fake_time.tick(self._timeout_ms + 1)
        else:
            time.sleep(self._timeout_ms / 1000)


class TestLivenessCheck(TestkitTestCase):
    required_features = (
        types.Feature.BOLT_5_4,
        types.Feature.API_LIVENESS_CHECK,
    )

    _DB = "adb"

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._router = StubServer(9000)

    def tearDown(self):
        self._server.reset()
        self._router.reset()

    def _start_server(self, server, script):
        extra_reset = ""
        if not self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            extra_reset = "A: RESET"

        server.start(self.script_path("v5x4", script),
                     vars_={"#HOST#": self._router.host,
                            "#EXTRA_RESET#": extra_reset})

    def start_servers(self, server_script="liveness_check.script"):
        self._start_server(self._server, server_script)

    def servers_done(self):
        self._server.done()

    @contextmanager
    def driver(self, routing=False, **kwargs):
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        if routing:
            uri = f"neo4j://{self._router.address}"
        else:
            uri = f"bolt://{self._server.address}"
        driver = Driver(self._backend, uri, auth, **kwargs)
        try:
            yield driver
        finally:
            driver.close()

    def get_reset_counts(self):
        return (self._server.count_requests("RESET"),)

    def get_new_reset_counts(self, counts):
        return tuple(
            c1 - c2 for c1, c2 in zip(self.get_reset_counts(), counts)
        )

    def assert_one_more_reset(self, counts_old, counts_new):
        expected_resets = tuple(count + 1 for count in counts_old)
        self.assertEqual(expected_resets, counts_new)

    def get_connection_count(self):
        return self._server.count_responses("<ACCEPT>")

    def get_new_connection_count(self, count_old):
        return self._server.count_responses("<ACCEPT>") - count_old

    @staticmethod
    def _execute_query(driver, query, database=None, auth_token=None):
        def work(tx):
            result = tx.run(query)
            records = list(result)
            summary = result.consume()
            return records, summary

        session = driver.session("w", database=database, auth_token=auth_token)
        try:
            return session.execute_write(work)
        finally:
            session.close()

    @driver_feature(types.Feature.BACKEND_MOCK_TIME)
    def test_no_timeout(self):
        self.start_servers()
        with FakeTime(self._backend) as time_mock:
            with self.driver(liveness_check_timeout_ms=None) as driver:
                self._execute_query(driver, "warmup", database=self._DB)

                # count RESETs without timeout
                counts_pre = self.get_reset_counts()
                self._execute_query(driver, "reference", database=self._DB)
                counts_ref = self.get_new_reset_counts(counts_pre)

                # can't test more than 60 minutes to not trip the default
                # max connection lifetime
                time_mock.tick(59 * 60 * 1000)  # 59 minutes

                # count RESETs after potential timeout
                counts_pre = self.get_reset_counts()
                self._execute_query(driver, "test", database=self._DB)
                counts = self.get_new_reset_counts(counts_pre)

                # assert no extra RESETs
                self.assertEqual(counts_ref, counts)
        self.servers_done()

    def test_timeout(self):
        timeout = 2000
        self.start_servers()
        with TimeoutManager(self, timeout) as time_mock:
            with self.driver(liveness_check_timeout_ms=timeout) as driver:
                self._execute_query(driver, "warmup", database=self._DB)

                # count RESETs without timeout
                counts_pre = self.get_reset_counts()
                self._execute_query(driver, "reference", database=self._DB)
                counts_ref = self.get_new_reset_counts(counts_pre)

                time_mock.tick_to_before_timeout()

                counts_pre = self.get_reset_counts()
                self._execute_query(driver, "test pre timeout",
                                    database=self._DB)
                counts = self.get_new_reset_counts(counts_pre)

                # assert no extra RESETs
                self.assertEqual(counts_ref, counts)

                time_mock.tick_to_after_timeout()
                # now we should get an extra RESET
                counts_pre = self.get_reset_counts()
                self._execute_query(driver, "test post timeout",
                                    database=self._DB)
                counts = self.get_new_reset_counts(counts_pre)

                # assert one extra RESET
                self.assert_one_more_reset(counts_ref, counts)
        self.servers_done()

    def test_timeout_recv_timeout(self):
        timeout = 2000
        self.start_servers("liveness_check_recv_timeout.script")
        # tests should use real timers since mocking timers can mess
        # with connection receive timeout implementations.
        with TimeoutManager(self, timeout, use_real_timers=True) as time_mock:
            with self.driver(liveness_check_timeout_ms=timeout) as driver:
                self._execute_query(driver, "warmup", database=self._DB)

                # count RESETs without timeout
                reset_counts_pre = self.get_reset_counts()
                self._execute_query(driver, "reference", database=self._DB)
                counts_ref = self.get_new_reset_counts(reset_counts_pre)

                time_mock.tick_to_before_timeout()

                reset_counts_pre = self.get_reset_counts()
                self._execute_query(driver, "test pre timeout",
                                    database=self._DB)
                reset_counts = self.get_new_reset_counts(reset_counts_pre)

                # assert no extra RESETs
                self.assertEqual(counts_ref, reset_counts)

                time_mock.tick_to_after_timeout()
                # now we should get an extra RESET
                # the RESET should time out and a new connection should be
                # established
                con_count_pre = self.get_connection_count()
                reset_counts_pre = self.get_reset_counts()
                self._execute_query(driver, "test post timeout",
                                    database=self._DB)
                reset_counts = self.get_new_reset_counts(reset_counts_pre)

                # assert one extra RESET
                self.assert_one_more_reset(counts_ref, reset_counts)
                # assert new connection was established
                new_con_count = self.get_new_connection_count(con_count_pre)
                self.assertEqual(1, new_con_count)
        self._server._dump()
        self.servers_done()

    @staticmethod
    def auth_generator():
        i = 0
        while True:
            yield types.AuthorizationToken("basic", principal=f"neo4j_{i}",
                                           credentials="pass")
            i += 1

    def test_timeout_with_re_auth(self):
        timeout = 2000
        auth_gen = self.auth_generator()
        self.start_servers()
        with TimeoutManager(self, timeout) as time_mock:
            with self.driver(liveness_check_timeout_ms=timeout) as driver:
                self._execute_query(driver, "warmup", database=self._DB)

                # count RESETs without timeout
                counts_pre = self.get_reset_counts()
                self._execute_query(driver, "reference", database=self._DB,
                                    auth_token=next(auth_gen))
                counts_ref = self.get_new_reset_counts(counts_pre)

                time_mock.tick_to_before_timeout()

                counts_pre = self.get_reset_counts()
                self._execute_query(
                    driver, "test pre timeout", database=self._DB,
                    auth_token=next(auth_gen)
                )
                counts = self.get_new_reset_counts(counts_pre)

                # assert no extra RESETs
                self.assertEqual(counts_ref, counts)

                time_mock.tick_to_after_timeout()
                # now we should get an extra RESET
                counts_pre = self.get_reset_counts()
                self._execute_query(
                    driver, "test post timeout", database=self._DB,
                    auth_token=next(auth_gen)
                )
                counts = self.get_new_reset_counts(counts_pre)

                # assert one extra RESET
                self.assert_one_more_reset(counts_ref, counts)
        self.servers_done()


class TestLivenessCheckRouting(TestLivenessCheck):
    _DB = None

    def start_servers(self, server_script="liveness_check.script"):
        self._start_server(self._router, "liveness_check_router.script")
        super().start_servers(server_script)

    def servers_done(self):
        self._server.done()
        super().servers_done()

    def get_reset_counts(self):
        return (self._server.count_requests("RESET"),
                self._router.count_requests("RESET"))

    def test_no_timeout(self):
        super().test_no_timeout()

    def test_timeout(self):
        super().test_timeout()

    def test_timeout_with_re_auth(self):
        super().test_timeout_with_re_auth()

    def driver(self, routing=True, **kwargs):
        return super().driver(routing=routing, **kwargs)
