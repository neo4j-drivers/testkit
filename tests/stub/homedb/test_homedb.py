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
        self._uri = f"neo4j://{self._router.address}"

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


class Test5x8HomeDbDirectDriver(TestkitTestCase):

    required_features = types.Feature.BOLT_5_8,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9000)
        self._auth_token = types.AuthorizationToken("basic", principal="p",
                                                    credentials="c")
        self._uri = f"bolt://{self._server.address}"

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def start_server(self, *path):
        self._server.start(path=self.script_path("no_cache", *path))

    def test_homedb_is_not_pinned_for_direct_drivers_session_run(self):
        self.start_server("single_server.script")

        driver = Driver(self._backend, self._uri, self._auth_token)

        session = driver.session("w")

        for i in range(2):
            res = session.run(f"RETURN {i + 1}")
            res.consume()

        session.close()
        driver.close()
        self._server.done()


class _RouteTracker:
    def __init__(self, server: StubServer, test: TestkitTestCase):
        self._server = server
        self._expected_route_count = 0
        self._test = test

    def assert_new_route_request(
        self,
        database=None,
        impersonated_user=None,
        auth=None,
    ):
        self._expected_route_count += 1

        requests = self._server.get_requests("ROUTE")
        self._test.assertEqual(len(requests), self._expected_route_count)
        route = requests[-1]

        if database:
            self._test.assertIn(f'"db": "{database}"', route)
        else:
            self._test.assertNotIn('"db":', route)

        if impersonated_user:
            self._test.assertIn(f'"imp_user": "{impersonated_user}"', route)
        else:
            self._test.assertNotIn('"imp_user":', route)

        if auth:
            logon_requests = self._server.get_requests("LOGON")
            last_logon = logon_requests[-1]
            self._test.assertIn(f'"scheme": "{auth.scheme}"', last_logon)

    def asset_no_new_route_request(self):
        requests = self._server.get_requests("ROUTE")
        self._test.assertEqual(len(requests), self._expected_route_count)


class TestHomeDbWithCache(TestkitTestCase):

    required_features = types.Feature.BOLT_5_8,

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader1 = StubServer(9010)
        self._reader2 = StubServer(9011)
        self._reader3 = StubServer(9012)
        self._auth1 = types.AuthorizationToken(
            "basic", principal="p", credentials="c"
        )
        self._auth2 = types.AuthorizationToken(
            "special",
            principal="p", credentials="c", realm=None, parameters=None,
        )
        self._uri = f"neo4j://{self._router.address}"

    def tearDown(self):
        self._reader1.reset()
        self._reader2.reset()
        self._reader3.reset()
        self._router.reset()
        super().tearDown()

    def start_server(self, server, *path, vars_=None):
        server.start(
            path=self.script_path("cache", *path),
            vars_={"#HOST#": self._router.host, **(vars_ or {})},
        )

    @driver_feature(types.Feature.IMPERSONATION)
    def test_homedb_cache(self):
        def _test():
            def work(tx):
                result = tx.run(query)
                result.consume()

            self.start_server(self._router, "router_homedb_cache.script")
            self.start_server(self._reader1, "reader_tx_homedb_cache.script")

            driver = Driver(self._backend, self._uri, self._auth1)

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

    def _test_homedb_cache_used_for_routing(
        self, driver_auth, session_args, session_user, home_db
    ):
        if home_db == "db1":
            home_server = "reader1"
            other_server = "reader2"
            other_db = "db2"
        elif home_db == "db2":
            home_server = "reader2"
            other_server = "reader1"
            other_db = "db1"
        else:
            raise ValueError(f"Unhandled home db: {home_db!r}")

        def whoami(
            driver_,
            database=None,
            impersonated_user=None,
            auth=None,
            new_home_db=None,
        ):
            if database is not None and new_home_db is not None:
                raise ValueError(
                    "Cannot inform the driver about a new home db when "
                    "running a query a against a fixed database"
                )
            session = driver_.session(
                "r",
                database=database,
                impersonated_user=impersonated_user,
                auth_token=auth,
                bookmarks=[new_home_db] if new_home_db else None,
            )
            try:
                records = list(session.run("WHOAMI"))
                self.assertEqual(len(records), 1)
                return records[0].values[0].value
            finally:
                session.close()

        self.start_server(self._router, "router_whoami.script")
        route_tracker = _RouteTracker(self._router, self)
        self.start_server(
            self._reader1,
            "reader_whoami.script",
            vars_={"#SERVER_NAME#": "reader1"},
        )
        self.start_server(
            self._reader2,
            "reader_whoami.script",
            vars_={"#SERVER_NAME#": "reader2"},
        )

        # Given: driver has cached the home db for the key and knows the RT
        #        for that home db
        driver = Driver(self._backend, self._uri, driver_auth)
        self.assertEqual(
            whoami(driver, **session_args),
            # expecting db1 (not homedb1) as the driver does not yet know
            # any routing information and will use the route request to pin
            # the db to the session
            f"{session_user}@{home_db}@{home_server}"
        )
        route_tracker.assert_new_route_request(**session_args)
        # Given: driver knows RT for db1
        self.assertRegex(
            whoami(driver, database="db1"),
            "admin[12]@db1@reader1"
        )
        if home_db == "db1":
            # already knowing RT for db1
            route_tracker.asset_no_new_route_request()
        else:
            route_tracker.assert_new_route_request(database="db1")
        # Given: driver knows RT for db2
        self.assertRegex(
            whoami(driver, database="db2"),
            "admin[12]@db2@reader2"
        )
        if home_db == "db2":
            # already knowing RT for db2
            route_tracker.asset_no_new_route_request()
        else:
            route_tracker.assert_new_route_request(database="db2")

        # When: cache key exists
        me = whoami(driver, **session_args)

        # Then: does not send routing request, uses RT pointed to by cache key
        route_tracker.asset_no_new_route_request()
        # Then: does not send the db name explicitly to the server
        self.assertEqual(me, f"{session_user}@home{home_db}@{home_server}")

        # When: home db changes
        self.assertEqual(
            whoami(driver, **session_args, new_home_db=other_db),
            f"{session_user}@home{other_db}@{home_server}"
        )
        me = whoami(driver, **session_args, new_home_db=other_db)

        # Then: does not send routing request, and uses new RT pointed to by
        #       the cache key
        route_tracker.asset_no_new_route_request()
        self.assertEqual(me, f"{session_user}@home{other_db}@{other_server}")
        route_tracker.asset_no_new_route_request()

        self._router.done()
        self._reader1.done()
        self._reader2.done()

    @driver_feature(
        types.Feature.IMPERSONATION,
        types.Feature.API_SESSION_AUTH_CONFIG,
        types.Feature.AUTH_CUSTOM,
    )
    def test_homedb_cache_used_for_routing(self):
        for (
            name,
            driver_auth,
            session_args,
            session_user,
            home_db,
            features,
        ) in (
            (
                "driver-level auth",
                self._auth1,
                {},
                "admin1",
                "db1",
                (),
            ),
            (
                "impersonation user1",
                self._auth1,
                {"impersonated_user": "user1"},
                "user1",
                "db1",
                (types.Feature.IMPERSONATION,),
            ),
            (
                "impersonation user2",
                self._auth1,
                {"impersonated_user": "user2"},
                "user2",
                "db2",
                (types.Feature.IMPERSONATION,),
            ),
            (
                "impersonation user1 (with session auth)",
                self._auth1,
                {"impersonated_user": "user1", "auth": self._auth2},
                "user1",
                "db1",
                (
                    types.Feature.IMPERSONATION,
                    types.Feature.API_SESSION_AUTH_CONFIG,
                ),
            ),
            (
                "impersonation user2 (with session auth)",
                self._auth2,
                {"impersonated_user": "user2", "auth": self._auth1},
                "user2",
                "db2",
                (
                    types.Feature.IMPERSONATION,
                    types.Feature.API_SESSION_AUTH_CONFIG,
                ),
            ),
            (
                "session-auth admin1",
                self._auth1,
                {"auth": self._auth1},
                "admin1",
                "db1",
                (types.Feature.API_SESSION_AUTH_CONFIG,),
            ),
            (
                "session-auth admin2",
                self._auth1,
                {"auth": self._auth2},
                "admin2",
                "db2",
                (
                    types.Feature.API_SESSION_AUTH_CONFIG,
                    types.Feature.AUTH_CUSTOM,
                ),
            ),
            (
                "session-auth admin1 (driver-auth admin2)",
                self._auth2,
                {"auth": self._auth1},
                "admin1",
                "db1",
                (
                    types.Feature.API_SESSION_AUTH_CONFIG,
                    types.Feature.AUTH_CUSTOM,
                ),
            ),
            (
                "session-auth admin2 (driver-auth admin2)",
                self._auth2,
                {"auth": self._auth2},
                "admin2",
                "db2",
                (
                    types.Feature.API_SESSION_AUTH_CONFIG,
                    types.Feature.AUTH_CUSTOM,
                ),
            ),
        ):
            with self.subTest(name=name):
                self.skip_if_missing_driver_features(*features)
                try:
                    self._test_homedb_cache_used_for_routing(
                        driver_auth, session_args, session_user, home_db
                    )
                finally:
                    self._router.reset()
                    self._reader1.reset()
                    self._reader2.reset()

    def test_home_db_is_pinned_even_if_server_re_resolves(self):
        def _query(session, i):
            result = list(session.run(f"RETURN {i} AS n"))
            self.assertEqual(len(result), 1)
            self.assertEqual(len(result[0].values), 1)
            self.assertIsInstance(result[0].values[0], types.CypherInt)
            self.assertEqual(result[0].values[0].value, i)

        def _test(resolved_from_cache_):
            self.start_server(self._router, "router_single_server.script")
            self.start_server(self._reader1, "reader_misbehaving.script")

            driver = Driver(self._backend, self._uri, self._auth1)

            if resolved_from_cache_:
                # populate cache
                session = driver.session("r")
                _query(session, 0)
                session.close()

            session = driver.session("r")

            for i in range(4):
                _query(session, i)

            session.close()
            driver.close()

        for resolved_from_cache in (True, False)[1:]:
            with self.subTest(resolved_from_cache=resolved_from_cache):
                try:
                    _test(resolved_from_cache)
                finally:
                    self._router.reset()
                    self._reader1.reset()


# Test home db cache
#  * [x] with ssr.enabled
#  * [x] without ssr.enabled
#  * [x] test uses cache to determine routing
#  * [x] test cache key precedence
#    * [x] None (driver auth, even when changing)
#    * [x] session-auth
#      * [x] basic auth
#      * [x] custom auth
#    * [x] imp-user
#  * [ ] caches home db despite RT changing
#  * [x] pinns home db despite server mis-behaving and sending different
#        resolved home DB
#  * [x] never sends the cached key to the server when RT is missing
#  * [x] pinns resolved home db from fetched RT if RT was missing
#  * [x] does never pin for direct connection (bolt scheme)
