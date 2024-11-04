import abc

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class _TestHomeDbWithoutCache(abc.ABC, TestkitTestCase):

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

    def start_server(self, server, *path):
        server.start(
            path=self.script_path("no_cache", *path),
            vars_=self.vars_(),
        )

    def vars_(self):
        return {"#HOST#": self._router.host}

    @driver_feature(types.Feature.IMPERSONATION)
    def test_should_resolve_db_per_session_session_run(self):
        def _test():
            self.start_server(self._router, "router_change_homedb.script")
            self.start_server(self._reader1, "reader_change_homedb.script")

            driver = Driver(self._backend, self._uri, self._auth_token)

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
            self.start_server(self._router, "router_change_homedb.script")
            self.start_server(self._reader1, "reader_tx_change_homedb.script")

            driver = Driver(self._backend, self._uri, self._auth_token)

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

            self.start_server(self._router, "router_change_homedb.script")
            self.start_server(self._reader1, "reader_tx_change_homedb.script")

            driver = Driver(self._backend, self._uri, self._auth_token)

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
                self.start_server(
                    self._router, "router_explicit_homedb.script"
                )
                self.start_server(self._reader2, "reader_tx_homedb.script")
                raise exc.exception
            else:
                res = tx.run("RETURN 1")
                return res.next()

        driver = Driver(self._backend, self._uri, self._auth_token)

        self.start_server(self._router, "router_homedb.script")
        self.start_server(self._reader1, "reader_tx_exits.script")

        session = driver.session("r", impersonated_user="the-imposter")
        session.execute_read(work)
        session.close()

        driver.close()

        self._router.done()
        self._reader2.done()
        self.assertEqual(i, 2)


class Test4x4HomeDbWithoutCache(_TestHomeDbWithoutCache):

    required_features = types.Feature.BOLT_4_4,

    def vars_(self):
        return {
            **super().vars_(),
            "#BOLT_PROTOCOL#": "4.4",
            "#CONVERSTAION_START#": 'A: HELLO {"{}": "*"}',
        }

    def test_should_resolve_db_per_session_session_run(self):
        super().test_should_resolve_db_per_session_session_run()

    def test_should_resolve_db_per_session_tx_run(self):
        super().test_should_resolve_db_per_session_tx_run()

    def test_should_resolve_db_per_session_tx_func_run(self):
        super().test_should_resolve_db_per_session_tx_func_run()

    def test_session_should_cache_home_db_despite_new_rt(self):
        super().test_session_should_cache_home_db_despite_new_rt()


class Test5x8HomeDbWithoutCache(_TestHomeDbWithoutCache):

    required_features = types.Feature.BOLT_5_8,

    def vars_(self):
        return {
            **super().vars_(),
            "#BOLT_PROTOCOL#": "5.8",
            # TODO: server agent might be 2025.01.00 or such
            "#CONVERSTAION_START#": """\
C: HELLO {"{}": "*"}
S: SUCCESS {"server": "Neo4j/5.27.0", "connection_id": "conn-1"}
A: LOGON {"{}": "*"}
""",
        }

    def test_should_resolve_db_per_session_session_run(self):
        super().test_should_resolve_db_per_session_session_run()

    def test_should_resolve_db_per_session_tx_run(self):
        super().test_should_resolve_db_per_session_tx_run()

    def test_should_resolve_db_per_session_tx_func_run(self):
        super().test_should_resolve_db_per_session_tx_func_run()

    def test_session_should_cache_home_db_despite_new_rt(self):
        super().test_session_should_cache_home_db_despite_new_rt()


class TestHomeDbWithCache(TestkitTestCase):
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

    def start_server(self, server, *path):
        server.start(
            path=self.script_path("cache", *path),
            vars_={"#HOST#": self._router.host},
        )

    @driver_feature(types.Feature.IMPERSONATION)
    def test_homedb_cache(self):
        def _test():
            def work(tx):
                result = tx.run(query)
                result.consume()

            self.start_server(self._router, "router_homedb_cache.script")
            self.start_server(self._reader1, "reader_tx_homedb_cache.script")

            driver = Driver(self._backend, self._uri, self._auth_token)

            session1 = driver.session("r", impersonated_user="the-imposter")
            query = "RETURN 1"
            session1.execute_read(work)
            if not parallel_sessions:
                session1.close()
            session2 = driver.session("r", impersonated_user="the-imposter")
            query = "RETURN 2"
            session2.execute_read(work)
            if not parallel_sessions:
                session2.close()
            session3 = driver.session("r", impersonated_user="the-imposter")
            query = "RETURN 3"
            session3.execute_read(work)
            if not parallel_sessions:
                session3.close()

            session4 = driver.session(
                "r", bookmarks=["bookmark"], impersonated_user="the-imposter"
            )
            query = "RETURN 4"
            session4.execute_read(work)
            session4.close()
            if parallel_sessions:
                session1.close()
                session2.close()
                session3.close()

            driver.close()

            self._router.done()
            self._reader1.done()

        for parallel_sessions in (True, False):
            with self.subTest(parallel_sessions=parallel_sessions):
                try:
                    _test()
                finally:
                    self._router.reset()
                    self._reader1.reset()
            self._router.reset()
            self._reader1.reset()

# Test home db cache
#  * [x] with ssr.enabled
#  * [x] without ssr.enabled
#  * [ ] test uses cache to determine routing
#  * [ ] test cache key precedence
#    * [ ] None (driver auth, even when changing)
#    * [ ] session-auth
#    * [ ] imp-user
#  * [ ] caches home db despite RT changing
#  * [ ] caches home db despite server mis-behaving and sending different
#        resolved home DB
#  * [x] never sends the cached key to the server when RT is missing
#  * [x] pinns resolved home db from fetched RT if RT was missing
#  * [ ] does never pin for direct connection (bolt scheme)
