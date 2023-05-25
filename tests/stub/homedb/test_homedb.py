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


class TestHomeDbUncached(TestkitTestCase):

    required_features = types.Feature.BOLT_4_4,

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader1 = StubServer(9010)
        self._reader2 = StubServer(9011)
        self._auth_token = types.AuthorizationToken("basic", principal="p",
                                                    credentials="c")
        self._uri = "neo4j://%s" % self._router.address

    def tearDown(self):
        self._reader1.reset()
        self._reader2.reset()
        self._router.reset()
        super().tearDown()

    def _get_driver(self):
        args = (self._backend, self._uri, self._auth_token)
        kwargs = {}
        if self.driver_supports_features(types.Feature.HOME_DB_CACHE):
            kwargs["max_home_database_delay_ms"] = 0
        return Driver(*args, **kwargs)

    @driver_feature(types.Feature.IMPERSONATION)
    def test_should_resolve_db_per_session_session_run(self):
        def _test():
            self._router.start(
                path=self.script_path("router_change_homedb.script"),
                vars_={"#HOST#": self._router.host}
            )

            self._reader1.start(
                path=self.script_path("reader_change_homedb.script")
            )

            driver = self._get_driver()

            session1 = driver.session("r", impersonated_user="the-imposter")
            result = session1.run("RETURN 1")
            result.consume()
            if not parallel_sessions:
                session1.close()

            session2 = driver.session(
                "r", bookmarks=["bookmark"], impersonated_user="the-imposter"
            )
            result = session2.run("RETURN 2")
            result.consume()
            session2.close()
            if parallel_sessions:
                session1.close()

            driver.close()

            self._router.done()
            self._reader1.done()

        for parallel_sessions in (True, False):
            with self.subTest(parallel_sessions=parallel_sessions):
                _test()
            self._router.reset()
            self._reader1.reset()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_should_resolve_db_per_session_tx_run(self):
        def _test():
            self._router.start(
                path=self.script_path("router_change_homedb.script"),
                vars_={"#HOST#": self._router.host}
            )

            self._reader1.start(
                path=self.script_path("reader_tx_change_homedb.script")
            )

            driver = self._get_driver()

            session1 = driver.session("r", impersonated_user="the-imposter")
            tx = session1.begin_transaction()
            result = tx.run("RETURN 1")
            result.consume()
            tx.commit()
            if not parallel_sessions:
                session1.close()

            session2 = driver.session(
                "r", bookmarks=["bookmark"], impersonated_user="the-imposter"
            )
            tx = session2.begin_transaction()
            result = tx.run("RETURN 2")
            result.consume()
            tx.commit()
            session2.close()
            if parallel_sessions:
                session1.close()

            driver.close()

            self._router.done()
            self._reader1.done()

        for parallel_sessions in (True, False):
            with self.subTest(parallel_sessions=parallel_sessions):
                _test()
            self._router.reset()
            self._reader1.reset()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_should_resolve_db_per_session_tx_func_run(self):
        def _test():
            def work(tx):
                result = tx.run(query)
                result.consume()

            self._router.start(
                path=self.script_path("router_change_homedb.script"),
                vars_={"#HOST#": self._router.host}
            )

            self._reader1.start(
                path=self.script_path("reader_tx_change_homedb.script")
            )

            driver = self._get_driver()

            session1 = driver.session("r", impersonated_user="the-imposter")
            query = "RETURN 1"
            session1.execute_read(work)
            if not parallel_sessions:
                session1.close()

            session2 = driver.session(
                "r", bookmarks=["bookmark"], impersonated_user="the-imposter"
            )
            query = "RETURN 2"
            session2.execute_read(work)
            session2.close()
            if parallel_sessions:
                session1.close()

            driver.close()

            self._router.done()
            self._reader1.done()

        for parallel_sessions in (True, False):
            with self.subTest(parallel_sessions=parallel_sessions):
                _test()
            self._router.reset()
            self._reader1.reset()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_session_should_cache_home_db_despite_new_rt(self):
        i = 0

        def work(tx):
            nonlocal i
            i += 1
            if i == 1:
                with self.assertRaises(types.DriverError) as exc:
                    res = tx.run("RETURN 1")
                    return res.next()
                self._router.done()
                self._reader1.done()
                self._router.start(
                    path=self.script_path("router_explicit_homedb.script"),
                    vars_={"#HOST#": self._router.host}
                )
                self._reader2.start(
                    path=self.script_path("reader_tx_homedb.script")
                )
                raise exc.exception
            else:
                res = tx.run("RETURN 1")
                return res.next()

        driver = self._get_driver()

        self._router.start(
            path=self.script_path("router_homedb.script"),
            vars_={"#HOST#": self._router.host}
        )
        self._reader1.start(
            path=self.script_path("reader_tx_exits.script")
        )

        session = driver.session("r", impersonated_user="the-imposter")
        session.execute_read(work)
        session.close()

        driver.close()

        self._router.done()
        self._reader2.done()
        self.assertEqual(i, 2)


class TestHomeDbCached(TestkitTestCase):

    required_features = (
        types.Feature.BOLT_4_4,
        types.Feature.HOME_DB_CACHE,
    )

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader = StubServer(9010)
        self._auth_token = types.AuthorizationToken("basic", principal="p",
                                                    credentials="c")
        self._uri = "neo4j://%s" % self._router.address

    def tearDown(self):
        self._reader.reset()
        self._router.reset()
        super().tearDown()

    def _start_server(self, server, script):
        server.start(self.script_path(script),
                     vars_={"#HOST#": self._router.host})

    @staticmethod
    def _build_work(method):
        def session_run_worker(session, iteration):
            result = session.run(f"RETURN {iteration}")
            list(result)

        def begin_transaction_worker(session, iteration):
            tx = session.begin_transaction()
            result = tx.run(f"RETURN {iteration}")
            list(result)
            tx.commit()

        def execute_read_worker(session, iteration):
            def work(tx):
                result = tx.run(f"RETURN {iteration}")
                list(result)
            session.execute_read(work)

        if method == "session_run":
            return session_run_worker
        if method == "begin_transaction":
            return begin_transaction_worker
        if method == "execute_read":
            return execute_read_worker
        raise ValueError("Unknown method: %s" % method)

    @driver_feature(types.Feature.IMPERSONATION)
    def test_should_cache_home_db(self):
        def _test(parallel_sessions, method, script):
            work = self._build_work(method)
            self._start_server(self._router, "router_change_homedb.script")
            self._start_server(self._reader, script)
            driver = Driver(self._backend, self._uri, self._auth_token)
            session1 = driver.session("r", impersonated_user="the-imposter")
            work(session1, 1)
            if not parallel_sessions:
                session1.close()
            session2 = driver.session("r", impersonated_user="the-imposter",
                                      bookmarks=["bookmark"])
            work(session2, 2)
            if parallel_sessions:
                session1.close()
            session2.close()
            driver.close()
            self._router.done()
            self._reader.done()
            router_calls = self._router.count_requests("ROUTE")
            self.assertEqual(router_calls, 1)

        for parallel_sessions_ in (True, False):
            for (method_, script_) in (
                ("session_run", "reader_cache_homedb.script"),
                ("begin_transaction", "reader_tx_cache_homedb.script"),
                ("execute_read", "reader_tx_cache_homedb.script"),
            ):
                with self.subTest(parallel_sessions=parallel_sessions_,
                                  method=method_):
                    _test(parallel_sessions_, method_, script_)
                self._router.reset()
                self._reader.reset()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_home_db_time_based_expiration(self):
        default_home_db_cache_ttl_ms = 5000

        def _test(method, script, cache_ttl):
            work = self._build_work(method)
            self._start_server(self._router, "router_change_homedb.script")
            self._start_server(self._reader, script)
            driver_config = {}
            if cache_ttl is not None:
                driver_config["max_home_database_delay_ms"] = cache_ttl
            driver = Driver(self._backend, self._uri, self._auth_token,
                            **driver_config)
            session1 = driver.session("r", impersonated_user="the-imposter")
            work(session1, 1)
            session1.close()
            if cache_ttl is None:
                time_mock.tick(default_home_db_cache_ttl_ms + 1)
            elif cache_ttl > 0:
                time_mock.tick(cache_ttl + 1)
            session2 = driver.session("r", impersonated_user="the-imposter",
                                      bookmarks=["bookmark"])
            work(session2, 2)
            session2.close()
            driver.close()
            self._router.done()
            self._reader.done()
            router_calls = self._router.count_requests("ROUTE")
            self.assertEqual(router_calls, 2)

        for (method_, script_) in (
            ("session_run", "reader_change_homedb.script"),
            ("begin_transaction", "reader_tx_change_homedb.script"),
            ("execute_read", "reader_tx_change_homedb.script"),
        ):
            for cache_ttl_ in (None, 0, 3600_000):
                with self.subTest(cache_ttl=cache_ttl_, method=method_):
                    with FakeTime(self._backend) as time_mock:
                        _test(method_, script_, cache_ttl_)
                self._router.reset()
                self._reader.reset()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_home_db_manual_expiration(self):
        def _test(method, script_template, expiration_point):
            work = self._build_work(method)
            self._start_server(self._router, "router_change_homedb.script")
            changes_db = expiration_point != "after_session_usage"
            if changes_db:
                script = script_template.format("change")
            else:
                script = script_template.format("cache")
            self._start_server(self._reader, script)
            driver = Driver(self._backend, self._uri, self._auth_token)
            session1 = driver.session("r", impersonated_user="the-imposter")
            work(session1, 1)
            session1.close()
            if expiration_point == "before_session_creation":
                driver.force_home_database_resolution()
            session2 = driver.session("r", impersonated_user="the-imposter",
                                      bookmarks=["bookmark"])
            if expiration_point == "before_session_usage":
                driver.force_home_database_resolution()
            work(session2, 2)
            if expiration_point == "after_session_usage":
                driver.force_home_database_resolution()
            work(session2, 3)
            session2.close()
            driver.close()
            self._router.done()
            self._reader.done()
            router_calls = self._router.count_requests("ROUTE")
            if changes_db:
                self.assertEqual(router_calls, 2)
            else:
                self.assertEqual(router_calls, 1)

        for (method_, script_template_) in (
            ("session_run", "reader_{}_homedb.script"),
            ("begin_transaction", "reader_tx_{}_homedb.script"),
            ("execute_read", "reader_tx_{}_homedb.script"),
        ):
            for expiration_point_ in (
                "before_session_creation",
                "before_session_usage",
                "after_session_usage",
            ):
                with self.subTest(expiration_point=expiration_point_,
                                  method=method_):
                    _test(method_, script_template_, expiration_point_)
                self._router.reset()
                self._reader.reset()
