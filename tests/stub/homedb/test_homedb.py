import abc
import concurrent.futures
from contextlib import contextmanager

import nutkit.protocol as types
from nutkit.frontend import (
    AuthTokenManager,
    BookmarkManager,
    Driver,
    Neo4jBookmarkManagerConfig,
)
from tests.shared import (
    driver_feature,
    TestkitTestCase,
    TimeoutManager,
)
from tests.stub.shared import StubServer


class _Base:
    # nested class to avoid test runner picking up the tests from the base

    class HomeDbWithoutCache(abc.ABC, TestkitTestCase):

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

                session1 = driver.session(
                    "r", impersonated_user="the-imposter"
                )
                result = session1.run("RETURN 1")
                result.consume()
                if not parallel_sessions:
                    session1.close()

                session2 = driver.session(
                    "r", bookmarks=["bookmark"],
                    impersonated_user="the-imposter"
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
                self.start_server(
                    self._reader1, "reader_tx_change_homedb.script"
                )

                driver = Driver(self._backend, self._uri, self._auth_token)

                session1 = driver.session(
                    "r", impersonated_user="the-imposter"
                )
                tx = session1.begin_transaction()
                result = tx.run("RETURN 1")
                result.consume()
                tx.commit()
                if not parallel_sessions:
                    session1.close()

                session2 = driver.session(
                    "r", bookmarks=["bookmark"],
                    impersonated_user="the-imposter"
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
                self.start_server(
                    self._reader1, "reader_tx_change_homedb.script"
                )

                driver = Driver(self._backend, self._uri, self._auth_token)

                session1 = driver.session(
                    "r", impersonated_user="the-imposter"
                )
                query = "RETURN 1"
                session1.execute_read(work)
                if not parallel_sessions:
                    session1.close()

                session2 = driver.session(
                    "r", bookmarks=["bookmark"],
                    impersonated_user="the-imposter"
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

    class TestHomeDbWithCache(abc.ABC, TestkitTestCase):
        required_features = (types.Feature.BOLT_5_8,
                             types.Feature.OPT_HOME_DB_CACHE)

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
                "special", principal="p", credentials="c",
                realm=None, parameters=None,
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

        def _get_driver(self, auth):
            return Driver(self._backend, self._uri, auth)

        @property
        @abc.abstractmethod
        def _whoami_script(self) -> str:
            ...

        @abc.abstractmethod
        def _whoami_query(
            self,
            driver_,
            database=None,
            impersonated_user=None,
            auth=None,
            new_home_db=None,
        ):
            ...

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

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=3,
            ) as executor:
                futures = [
                    executor.submit(
                        self.start_server, self._router, "router_whoami.script"
                    ),
                    executor.submit(
                        self.start_server,
                        self._reader1,
                        self._whoami_script,
                        vars_={"#SERVER_NAME#": "reader1"},
                    ),
                    executor.submit(
                        self.start_server,
                        self._reader2,
                        self._whoami_script,
                        vars_={"#SERVER_NAME#": "reader2"},
                    ),
                ]
            for future in futures:
                future.result()
            route_tracker = _RouteTracker(self._router, self)

            driver = Driver(self._backend, self._uri, driver_auth)

            # Given: driver has cached the home db for the key and knows the RT
            #        for that home db
            self.assertEqual(
                self._whoami_query(driver, **session_args),
                # expecting db1 (not homedb1) as the driver does not yet know
                # any routing information and will use the route request to pin
                # the db to the session
                f"{session_user}@{home_db}@{home_server}"
            )
            route_tracker.assert_new_route_request(**session_args)
            # Given: driver knows RT for db1
            self.assertRegex(
                self._whoami_query(driver, database="db1"),
                "admin[12]@db1@reader1"
            )
            if home_db == "db1":
                # already knowing RT for db1
                route_tracker.assert_no_new_route_request()
            else:
                route_tracker.assert_new_route_request(database="db1")
            # Given: driver knows RT for db2
            self.assertRegex(
                self._whoami_query(driver, database="db2"),
                "admin[12]@db2@reader2"
            )
            if home_db == "db2":
                # already knowing RT for db2
                route_tracker.assert_no_new_route_request()
            else:
                route_tracker.assert_new_route_request(database="db2")

            # When: cache key exists
            me = self._whoami_query(driver, **session_args)

            # Then: does not send routing request, uses RT pointed to by cache
            # key
            route_tracker.assert_no_new_route_request()
            # Then: does not send the db name explicitly to the server
            self.assertEqual(me, f"{session_user}@home{home_db}@{home_server}")

            # When: home db changes
            self.assertEqual(
                self._whoami_query(
                    driver, **session_args, new_home_db=other_db,
                ),
                f"{session_user}@home{other_db}@{home_server}"
            )
            me = self._whoami_query(
                driver, **session_args, new_home_db=other_db,
            )

            # Then: does not send routing request, and uses new RT pointed to
            #       by the cache key
            route_tracker.assert_no_new_route_request()
            self.assertEqual(
                me, f"{session_user}@home{other_db}@{other_server}"
            )
            route_tracker.assert_no_new_route_request()

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

            for resolved_from_cache in (True, False):
                with self.subTest(resolved_from_cache=resolved_from_cache):
                    try:
                        _test(resolved_from_cache)
                    finally:
                        self._router.reset()
                        self._reader1.reset()

        def test_rt_changes_underneath_cache(self):
            self.start_server(self._router, "router_db_moves.script")
            self.start_server(
                self._reader1,
                self._whoami_script,
                vars_={"#SERVER_NAME#": "reader1"},
            )
            self.start_server(
                self._reader2,
                self._whoami_script,
                vars_={"#SERVER_NAME#": "reader2"},
            )
            driver = Driver(self._backend, self._uri, self._auth1)

            # Given: driver has cached home db
            self.assertEqual(self._whoami_query(driver), "admin1@db1@reader1")
            self.assertEqual(
                self._whoami_query(driver), "admin1@homedb1@reader1",
            )

            # When: routing table for the home db changes
            self.assertEqual(
                self._whoami_query(driver, impersonated_user="user1"),
                "user1@db1@reader2"
            )

            # Then: driver should find the new routing table through the cache
            self.assertEqual(
                self._whoami_query(driver), "admin1@homedb1@reader2"
            )

            driver.close()
            self._router.done()

        @driver_feature(types.Feature.AUTH_MANAGED, types.Feature.AUTH_CUSTOM)
        def test_cache_considers_all_driver_auth_equal(self):
            auth1 = types.AuthorizationToken(
                "basic", principal="user1", credentials="password1"
            )
            auth2 = types.AuthorizationToken(
                "basic", principal="user2", credentials="password2",
            )
            auth3 = types.AuthorizationToken(
                "basic", principal="user3", credentials="password3"
            )
            current_auth = auth1

            def get_auth(*_, **__):
                return current_auth

            def handle_exc(*_, **__):
                raise NotImplementedError

            def make_auth_switch(new_auth):
                def step(driver, tracker):
                    nonlocal current_auth
                    current_auth = new_auth
                return step

            auth_manager = AuthTokenManager(
                self._backend, get_auth, handle_exc,
            )

            self._run_whoami_test(
                auth_manager,
                (
                    # When: driver-level auth manager has been used to cache
                    #       the home db
                    self._make_assert_whoami("admin1@db1@reader1"),
                    self._make_assert_route_request(auth=auth1),
                    # Then: regardless of the token returned by the auth
                    #       manager the driver should use the cached home db
                    make_auth_switch(auth2),
                    self._make_assert_whoami("admin1@homedb1@reader1"),
                    self._make_assert_no_route_request(),

                    make_auth_switch(auth3),
                    self._make_assert_whoami("admin1@homedb1@reader1"),
                    self._make_assert_no_route_request(),
                ),
            )

        def test_cached_home_db_changing(self):
            self._run_whoami_test(
                self._auth1,
                (
                    # Given: driver has cached home db 1
                    self._make_assert_whoami("admin1@db1@reader1"),
                    self._make_assert_route_request(auth=self._auth1),

                    # When: home db changes to 2
                    self._make_assert_whoami(
                        "admin1@homedb2@reader1",
                        new_home_db="db2",
                    ),
                    self._make_assert_no_route_request(),

                    # Then: driver should use the new home db
                    #       However, the driven doesn't know a route for db2
                    #       yet therefore, it will send a ROUTE request and
                    #       pin the returned db to the session
                    self._make_assert_whoami(
                        "admin1@db2@reader2",
                        new_home_db="db2",
                    ),
                    self._make_assert_route_request(),

                    # When: the home db remains 2
                    # Then: driver should not send another ROUTE request
                    self._make_assert_whoami(
                        "admin1@homedb2@reader2",
                        new_home_db="db2",
                    ),
                    self._make_assert_no_route_request(),

                    # When: home db changes back to 1
                    self._make_assert_whoami("admin1@homedb1@reader2"),
                    self._make_assert_no_route_request(),

                    # Then: driver should use the new home db
                    self._make_assert_whoami("admin1@homedb1@reader1"),
                    self._make_assert_no_route_request(),
                ),
            )

        def test_cached_home_db_changing_before_route(self):
            self._run_whoami_test(
                self._auth1,
                (
                    # Given: driver has cached home db 2
                    self._make_assert_whoami(
                        "admin1@db2@reader2",
                        new_home_db="db2",
                    ),
                    self._make_assert_route_request(auth=self._auth1),

                    # When: home db changes to 1
                    self._make_assert_whoami("admin1@homedb1@reader2"),
                    self._make_assert_no_route_request(),
                    # And: before the next route request, the home db changes
                    #      back
                    # Then: driver should pin the reverted home db to the
                    #       session
                    self._make_assert_whoami(
                        "admin1@db2@reader2",
                        new_home_db="db2",
                    ),
                    self._make_assert_route_request(auth=self._auth1),
                ),
            )

        @driver_feature(
            types.Feature.OPT_HOME_DB_CACHE_BASIC_PRINCIPAL_IS_IMP_USER,
        )
        def test_optimization_basic_principal_equals_impersonated_user(self):
            auth_basic_user2 = types.AuthorizationToken(
                "basic", principal="user2", credentials="password",
            )
            auth_special_user2 = types.AuthorizationToken(
                "special", principal="user2", credentials="password",
                realm=None, parameters=None,
            )

            for (
                name,
                assertions,
                features,
            ) in (
                (
                    "basic principal is impersonated user",
                    (
                        # When: driver has resolved home db for basic principal
                        self._make_assert_whoami(
                            "admin1@db1@reader1",
                            auth=auth_basic_user2,
                        ),
                        self._make_assert_route_request(auth=auth_basic_user2),
                        # Then: uses the cached home db for the impersonated
                        #       user
                        self._make_assert_whoami(
                            "user2@homedb2@reader1",
                            impersonated_user=auth_basic_user2.principal,
                        ),
                        self._make_assert_no_route_request(),
                    ),
                    (
                        types.Feature.IMPERSONATION,
                        types.Feature.API_SESSION_AUTH_CONFIG,
                    ),
                ),
                (
                    "impersonated user is basic principal",
                    (
                        # When: driver has resolved home db for impersonated
                        #       user
                        self._make_assert_whoami(
                            "user2@db2@reader2",
                            impersonated_user=auth_basic_user2.principal,
                        ),
                        self._make_assert_route_request(
                            impersonated_user=auth_basic_user2.principal,
                        ),
                        # Then: uses the cached home db for the basic principal
                        self._make_assert_whoami(
                            "admin1@homedb1@reader2",
                            auth=auth_basic_user2,
                        ),
                        self._make_assert_no_route_request(),
                    ),
                    (
                        types.Feature.IMPERSONATION,
                        types.Feature.API_SESSION_AUTH_CONFIG,
                    ),
                ),
                (
                    "basic principal is not special principal",
                    (
                        # When: driver has cached home db for basic principal
                        self._make_assert_whoami(
                            "admin1@db1@reader1",
                            auth=auth_basic_user2,
                        ),
                        self._make_assert_route_request(auth=auth_basic_user2),
                        # Then: custom auth with same principal is a cache miss
                        self._make_assert_whoami(
                            "admin2@db2@reader2",
                            auth=auth_special_user2,
                        ),
                        self._make_assert_route_request(
                            auth=auth_special_user2,
                        ),
                    ),
                    (
                        types.Feature.API_SESSION_AUTH_CONFIG,
                    ),
                ),
                (
                    "special principal is not basic principal",
                    (
                        # When: driver has cached home db for custom principal
                        self._make_assert_whoami(
                            "admin2@db2@reader2",
                            auth=auth_special_user2,
                        ),
                        self._make_assert_route_request(
                            auth=auth_special_user2,
                        ),
                        # Then: basic auth with same principal is a cache miss
                        self._make_assert_whoami(
                            "admin1@db1@reader1",
                            auth=auth_basic_user2,
                        ),
                        self._make_assert_route_request(auth=auth_basic_user2),
                    ),
                    (
                        types.Feature.API_SESSION_AUTH_CONFIG,
                    ),
                ),
                (
                    "special principal is not impersonated user",
                    (
                        # When: driver has cached home db for custom principal
                        self._make_assert_whoami(
                            "admin2@db2@reader2",
                            auth=auth_special_user2,
                        ),
                        self._make_assert_route_request(
                            auth=auth_special_user2,
                        ),
                        # Then: impersonation of custom principal is a cache
                        #       miss
                        self._make_assert_whoami(
                            "user2@db2@reader2",
                            impersonated_user=auth_special_user2.principal,
                        ),
                        self._make_assert_route_request(
                            impersonated_user=auth_special_user2.principal,
                        ),
                    ),
                    (
                        types.Feature.IMPERSONATION,
                        types.Feature.API_SESSION_AUTH_CONFIG,
                    ),
                ),
                (
                    "impersonated user is not special principal",
                    (
                        # When: driver has cached home db for impersonated user
                        self._make_assert_whoami(
                            "user2@db2@reader2",
                            impersonated_user=auth_special_user2.principal,
                        ),
                        self._make_assert_route_request(
                            impersonated_user=auth_special_user2.principal,
                        ),
                        # Then: custom auth with same principal is a cache miss
                        self._make_assert_whoami(
                            "admin2@db2@reader2",
                            auth=auth_special_user2,
                        ),
                        self._make_assert_route_request(
                            auth=auth_special_user2,
                        ),
                    ),
                    (
                        types.Feature.IMPERSONATION,
                        types.Feature.API_SESSION_AUTH_CONFIG,
                    ),
                ),
            ):
                with self.subTest(name=name):
                    self.skip_if_missing_driver_features(*features)
                    try:
                        self._run_whoami_test(self._auth1, assertions)
                    finally:
                        self._router.reset()
                        self._reader1.reset()
                        self._reader2.reset()

        def _make_assert_whoami(
            self,
            who,
            database=None,
            impersonated_user=None,
            auth=None,
            new_home_db=None,
        ):
            def assertion(driver, tracker):
                self.assertEqual(
                    self._whoami_query(
                        driver,
                        database=database,
                        impersonated_user=impersonated_user,
                        auth=auth,
                        new_home_db=new_home_db,
                    ),
                    who
                )
            return assertion

        def _make_assert_no_route_request(self):
            def assertion(driver, tracker):
                tracker.assert_no_new_route_request()
            return assertion

        def _make_assert_route_request(
            self,
            database=None,
            impersonated_user=None,
            auth=None,
        ):
            def assertion(driver, tracker):
                tracker.assert_new_route_request(
                    database=database,
                    impersonated_user=impersonated_user,
                    auth=auth,
                )
            return assertion

        def _run_whoami_test(self, driver_auth, steps):
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=3,
            ) as executor:
                futures = [
                    executor.submit(
                        self.start_server, self._router, "router_whoami.script"
                    ),
                    executor.submit(
                        self.start_server,
                        self._reader1,
                        self._whoami_script,
                        vars_={"#SERVER_NAME#": "reader1"},
                    ),
                    executor.submit(
                        self.start_server,
                        self._reader2,
                        self._whoami_script,
                        vars_={"#SERVER_NAME#": "reader2"},
                    ),
                ]
            for future in futures:
                future.result()
            route_tracker = _RouteTracker(self._router, self)

            driver = Driver(self._backend, self._uri, driver_auth)
            for step in steps:
                step(driver, route_tracker)
            driver.close()
            self._router.done()
            self._reader1.done(ignore_never_started=True)
            self._reader2.done(ignore_never_started=True)


class Test4x4HomeDbWithoutCache(_Base.HomeDbWithoutCache):

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


class Test5x8HomeDbWithoutCache(_Base.HomeDbWithoutCache):

    required_features = types.Feature.BOLT_5_8,

    def vars_(self):
        return {
            **super().vars_(),
            "#BOLT_PROTOCOL#": "5.8",
            "#CONVERSTAION_START#": """\
C: HELLO {"{}": "*"}
S: SUCCESS {"server": "Neo4j/5.26.0", "connection_id": "conn-1"}
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
            for key, attr in vars(auth).items():
                if isinstance(attr, str):
                    self._test.assertIn(f'"{key}": "{attr}"', last_logon)

    def assert_no_new_route_request(self):
        requests = self._server.get_requests("ROUTE")
        self._test.assertEqual(len(requests), self._expected_route_count)


class TestHomeDbWithCache(_Base.TestHomeDbWithCache):

    required_features = (types.Feature.BOLT_5_8,
                         types.Feature.OPT_HOME_DB_CACHE)

    @property
    def _whoami_script(self) -> str:
        return "reader_whoami.script"

    def _whoami_query(
        self,
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

    def test_homedb_cache_used_for_routing(self):
        super().test_homedb_cache_used_for_routing()

    def test_home_db_is_pinned_even_if_server_re_resolves(self):
        super().test_home_db_is_pinned_even_if_server_re_resolves()

    def test_rt_changes_underneath_cache(self):
        super().test_rt_changes_underneath_cache()

    def test_cache_considers_all_driver_auth_equal(self):
        super().test_cache_considers_all_driver_auth_equal()

    def test_cached_home_db_changing(self):
        super().test_cached_home_db_changing()

    def test_cached_home_db_changing_before_route(self):
        super().test_cached_home_db_changing_before_route()

    def test_optimization_basic_principal_equals_impersonated_user(self):
        super().test_optimization_basic_principal_equals_impersonated_user()


class TestHomeDbWithCacheTx(_Base.TestHomeDbWithCache):
    @property
    def _whoami_script(self) -> str:
        return "reader_whoami_tx.script"

    def _whoami_query(
        self,
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
            tx = session.begin_transaction()
            try:
                records = list(tx.run("WHOAMI"))
                tx.commit()
                self.assertEqual(len(records), 1)
                return records[0].values[0].value
            finally:
                tx.close()
        finally:
            session.close()

    def test_homedb_cache_used_for_routing(self):
        super().test_homedb_cache_used_for_routing()

    def test_home_db_is_pinned_even_if_server_re_resolves(self):
        super().test_home_db_is_pinned_even_if_server_re_resolves()

    def test_rt_changes_underneath_cache(self):
        super().test_rt_changes_underneath_cache()

    def test_cache_considers_all_driver_auth_equal(self):
        super().test_cache_considers_all_driver_auth_equal()

    def test_cached_home_db_changing(self):
        super().test_cached_home_db_changing()

    def test_cached_home_db_changing_before_route(self):
        super().test_cached_home_db_changing_before_route()

    def test_optimization_basic_principal_equals_impersonated_user(self):
        super().test_optimization_basic_principal_equals_impersonated_user()


class TestHomeDbWithCacheExecuteQuery(_Base.TestHomeDbWithCache):

    required_features = (*_Base.TestHomeDbWithCache.required_features,
                         types.Feature.API_DRIVER_EXECUTE_QUERY)

    @property
    def _whoami_script(self) -> str:
        return "reader_whoami_tx.script"

    def _whoami_query(
        self,
        driver_,
        database=None,
        impersonated_user=None,
        auth=None,
        new_home_db=None,
    ):
        if auth is not None:
            self.skip_if_missing_driver_features(
                types.Feature.API_DRIVER_EXECUTE_QUERY_WITH_AUTH
            )
        if database is not None and new_home_db is not None:
            raise ValueError(
                "Cannot inform the driver about a new home db when "
                "running a query a against a fixed database"
            )
        bookmarks = [new_home_db] if new_home_db else []
        bookmark_manager = BookmarkManager(
            self._backend,
            Neo4jBookmarkManagerConfig(initial_bookmarks=bookmarks),
        )
        try:
            result = driver_.execute_query(
                "WHOAMI",
                routing="r",
                database=database,
                impersonated_user=impersonated_user,
                auth_token=auth,
                bookmark_manager=bookmark_manager,
            )
            self.assertEqual(len(result.records), 1)
            return result.records[0].values[0].value
        finally:
            bookmark_manager.close()

    def test_homedb_cache_used_for_routing(self):
        super().test_homedb_cache_used_for_routing()

    def test_home_db_is_pinned_even_if_server_re_resolves(self):
        super().test_home_db_is_pinned_even_if_server_re_resolves()

    def test_rt_changes_underneath_cache(self):
        super().test_rt_changes_underneath_cache()

    def test_cache_considers_all_driver_auth_equal(self):
        super().test_cache_considers_all_driver_auth_equal()

    def test_cached_home_db_changing(self):
        super().test_cached_home_db_changing()

    def test_cached_home_db_changing_before_route(self):
        super().test_cached_home_db_changing_before_route()

    def test_optimization_basic_principal_equals_impersonated_user(self):
        super().test_optimization_basic_principal_equals_impersonated_user()


class TestHomeDbMixedCluster(TestkitTestCase):

    required_features = (types.Feature.BOLT_5_8,
                         types.Feature.OPT_HOME_DB_CACHE)

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader = StubServer(9010)
        self._writer1 = StubServer(9020)
        self._writer2 = StubServer(9021)
        self._auth1 = types.AuthorizationToken(
            "basic", principal="p", credentials="c"
        )
        self._uri = f"neo4j://{self._router.address}"

    def tearDown(self):
        self._reader.reset()
        self._writer1.reset()
        self._router.reset()
        super().tearDown()

    def start_server(self, server, *path, vars_=None):
        server.start(
            path=self.script_path("mixed", *path),
            vars_={
                "#HOST#": self._router.host,
                "#WRITER_2#": self._writer1.port,
                "#EXTRA_BANG_LINES#": "",
                **(vars_ or {})
            },
        )

    @contextmanager
    def driver(self, **kwargs):
        driver = Driver(self._backend, self._uri, self._auth1, **kwargs)
        try:
            yield driver
        finally:
            driver.close()

    @contextmanager
    def session(self, driver, access_mode, **kwargs):
        session = driver.session(access_mode, **kwargs)
        try:
            yield session
        finally:
            session.close()

    def _test_mixed_cluster(self):
        with self.driver() as driver:
            # 1st connection => explicit home db resolution (homedb1)
            with self.session(driver, "r") as session:
                result = session.run("RETURN 1 AS n")
                result.consume()

            # 2nd connection has no ssr support
            # => falling back to explicit home db resolution (homedb2)
            with self.session(driver, "w") as session:
                result = session.run("RETURN 2 AS n")
                result.consume()

            # making sure the connection to the reader is still alive after
            # the fallback
            with self.session(driver, "r", database="homedb1") as session:
                result = session.run("RETURN 3 AS n")
                result.consume()

        self._router.done()
        self._reader.done()
        self._writer1.done()

    @driver_feature(types.Feature.BOLT_5_7)
    def test_home_db_fallback_mixed_bolt_versions(self):
        self.start_server(self._router, "router_5x8.script")
        self.start_server(self._reader, "reader_5x8_ssr.script")
        self.start_server(
            self._writer1,
            "writer_no_ssr.script",
            vars_={
                "#BOLT_VERSION#": "5.7",
                "#HELLO_MESSAGE#": """A: HELLO {"{}": "*"}""",
            },
        )

        self._test_mixed_cluster()

    VARS_WRITER_NO_SSR = {
        "#BOLT_VERSION#": "5.8",
        "#HELLO_MESSAGE#": (  # noqa: PAR001
            'C: HELLO {"{}": "*"}\n'
            "S: SUCCESS "
            '{"connection_id": "bolt-1", "server": "Neo4j/5.26.0"}'
        ),
    }

    def test_home_db_fallback_no_ssr_hint(self):
        self.start_server(self._router, "router_5x8.script")
        self.start_server(self._reader, "reader_5x8_ssr.script")
        self.start_server(
            self._writer1,
            "writer_no_ssr.script",
            vars_=self.VARS_WRITER_NO_SSR,
        )

        self._test_mixed_cluster()

    @driver_feature(types.Feature.API_CONNECTION_ACQUISITION_TIMEOUT)
    def test_connection_acquisition_timeout_during_fallback(self):
        self.start_server(
            self._router,
            "router_5x8.script",
            vars_={
                "#WRITER_2#": self._writer2.port,
            },
        )
        self.start_server(self._reader, "reader_5x8_ssr.script")
        self.start_server(
            self._writer1,
            "writer_no_ssr_only_connect.script",
            vars_={
                **self.VARS_WRITER_NO_SSR,
                "#EXTRA_BANG_LINES#": "!: HANDSHAKE_DELAY 1.1",
            },
        )
        self.start_server(
            self._writer2,
            "writer_no_ssr.script",
            vars_={
                **self.VARS_WRITER_NO_SSR,
                "#EXTRA_BANG_LINES#": "!: HANDSHAKE_DELAY 1.1",
            },
        )

        with self.driver(
            connection_acquisition_timeout_ms=2000,
        ) as driver:
            # set-up driver to have home db cache enabled
            with self.session(driver, "r") as session:
                result = session.run("RETURN 1 AS n")
                result.consume()

            # detecting server without SSR => fallback to explicit home db
            # together, connection acquisition trigger the timeout
            with self.assertRaises(types.DriverError):
                with self.session(driver, "w") as session:
                    result = session.run("RETURN 2 AS n")
                    result.consume()

        self._router.done()
        self._reader.done()
        self._writer1.done()
        # acquisition timeout kicks in => not finishing the script
        self._writer2.reset()

    # This test ensures that the connection acquisition timeout covers the full
    # acqusition under the same timer without resetting. This must include both
    # optimistic routing and falling back. If this is not the case, this test
    # can be skipped on the driver side until the behavior is unified.
    @driver_feature(
        types.Feature.BOLT_5_7,
        types.Feature.API_DRIVER_MAX_CONNECTION_LIFETIME,
    )
    def test_warm_cache_during_cluster_upgrade(self):
        self.start_server(
            self._router,
            "router_keep_warm.script",
            vars_={
                "#BOLT_VERSION#": "5.7",
                "#IMPERSONATED_USER#": "",
            },
        )
        self.start_server(self._reader, "reader_5x7_keep_warm.script")
        with TimeoutManager(self, 2000) as timeout:
            with self.driver(max_connection_lifetime_ms=2000) as driver:
                with self.session(driver, "r") as session:
                    result = session.run("RETURN 1 AS n")
                    result.consume()

                self._router.done()
                self._reader.done()
                # Mock time or wait until open connections lifetimes expire
                timeout.tick_to_after_timeout()
                self.start_server(
                    self._router,
                    "router_keep_warm.script",
                    vars_={
                        "#BOLT_VERSION#": "5.8",
                        "#IMPERSONATED_USER#": ', "imp_user": "user2"',
                    },
                )
                self.start_server(self._reader, "reader_5x8_keep_warm.script")

                with self.session(
                    driver, "r", impersonated_user="user2"
                ) as session:
                    result = session.run("RETURN 2 as n")
                    result.consume()
                self._router.done()

                with self.session(driver, "r") as session:
                    result = session.run("RETURN 3 as n")
                    result.consume()
                self._reader.done()
