from contextlib import contextmanager

import nutkit.protocol as types
from nutkit.frontend import (
    BearerAuthTokenManager,
    Driver,
    FakeTime,
)
from tests.shared import driver_feature
from tests.stub.authorization.test_authorization import AuthorizationBase
from tests.stub.shared import StubServer


class TestBearerAuthManager5x1(AuthorizationBase):

    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.AUTH_MANAGED)

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader = StubServer(9010)
        self._writer = StubServer(9020)
        self._uri = "bolt://%s:%d" % (self._reader.host,
                                      self._reader.port)
        self._driver = None

    def tearDown(self):
        self._router.reset()
        self._reader.reset()
        self._writer.reset()
        if self._driver:
            self._driver.close()
        super().tearDown()

    def get_vars(self):
        host = self._router.host
        return {
            "#VERSION#": "5.1",
            "#HOST#": host,
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }

    @contextmanager
    def driver(self, auth, routing=False, **kwargs):
        if routing:
            uri = f"neo4j://{self._router.address}"
        else:
            uri = f"bolt://{self._reader.address}"
        driver = Driver(self._backend, uri, auth, **kwargs)
        try:
            yield driver
        finally:
            driver.close()

    @contextmanager
    def session(self, driver, access_mode="r"):
        session = driver.session(access_mode, database="adb")
        try:
            yield session
        finally:
            session.close()

    def post_script_assertions(self, server):
        # add OPT_MINIMAL_RESETS assertion (if driver claims to support it)
        if self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            self.assertEqual(server.count_requests("RESET"), 0)

    def test_static_provider(self):
        count = 0

        def provider():
            nonlocal count
            count += 1
            return types.AuthTokenAndExpiration(
                types.AuthorizationToken(
                    scheme="basic",
                    principal="neo4j",
                    credentials="pass"
                )
            )

        auth_manager = BearerAuthTokenManager(self._backend, provider)

        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_no_reauth.script")
        )
        with self.driver(auth_manager) as driver:
            with self.session(driver) as session:
                list(session.run("RETURN 1 AS n"))

        self._reader.done()
        self.assertEqual(count, 1)
        self.post_script_assertions(self._reader)

    @driver_feature(types.Feature.BACKEND_MOCK_TIME)
    def test_static_provider_long_time(self):
        count = 0

        def provider():
            nonlocal count
            count += 1
            return types.AuthTokenAndExpiration(
                types.AuthorizationToken(
                    scheme="basic",
                    principal="neo4j",
                    credentials="pass"
                )
            )

        auth_manager = BearerAuthTokenManager(self._backend, provider)

        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_no_reauth.script")
        )

        with FakeTime(self._backend) as time:
            with self.driver(auth_manager) as driver:
                with self.session(driver) as session:
                    list(session.run("RETURN 1 AS n"))
                # just under 1 hour to make sure to not trip over the
                # connection max lifetime
                time.tick(1000 * 3600 - 1)
                with self.session(driver) as session:
                    # should still use the same token without calling the
                    # provider
                    list(session.run("RETURN 1 AS n"))

        self._reader.done()
        self.assertEqual(count, 1)
        self.post_script_assertions(self._reader)

    @driver_feature(types.Feature.BACKEND_MOCK_TIME)
    def test_expiring_token_deadline(self):
        count = 0

        def provider():
            nonlocal count
            count += 1
            credentials = "pass++" if count > 1 else "pass"
            principal = "neo5j" if count > 1 else "neo4j"

            return types.AuthTokenAndExpiration(
                types.AuthorizationToken(
                    scheme="basic",
                    principal=principal,
                    credentials=credentials
                ),
                10_000
            )

        auth_manager = BearerAuthTokenManager(self._backend, provider)

        self.start_server(self._reader,
                          self.script_fn_with_features("reader_reauth.script"))
        with FakeTime(self._backend) as time:
            with self.driver(auth_manager) as driver:
                with self.session(driver) as session:
                    list(session.run("RETURN 1 AS n"))

                time.tick(9_000)

                with self.session(driver) as session:
                    list(session.run("RETURN 2 AS n"))

                self.assertEqual(1, count)

                time.tick(1_001)

                with self.session(driver) as session:
                    list(session.run("RETURN 3 AS n"))

                with self.session(driver) as session:
                    list(session.run("RETURN 4 AS n"))

                self.assertEqual(2, count)

        self._reader.done()
        self.post_script_assertions(self._reader)

    def test_renewing_on_expiration_error(self):
        def _test(error_, routing_):
            count = 0

            def provider():
                nonlocal count
                count += 1
                credentials = "pass++" if count > 1 else "pass"
                principal = "neo5j" if count > 1 else "neo4j"

                return types.AuthTokenAndExpiration(
                    types.AuthorizationToken(
                        scheme="basic",
                        principal=principal,
                        credentials=credentials
                    ),
                    10_000
                )

            auth_manager = BearerAuthTokenManager(self._backend, provider)

            if error_ in ("authorization_expired",):
                reader_script = self.script_fn_with_features(
                    f"reader_reauth_{error_}.script"
                )
                writer_script = self.script_fn_with_features(
                    f"writer_reauth_{error_}.script"
                )
                vars_ = None
                expected_call_count = 1
            elif error_ in ("token_expired", "unauthorized"):
                reader_script = self.script_fn_with_features(
                    "reader_reauth_handled.script"
                )
                writer_script = self.script_fn_with_features(
                    "writer_reauth_handled.script"
                )
                vars_ = self.get_vars()
                expected_call_count = 2
                if error_ == "token_expired":
                    vars_["#ERROR#"] = self._TOKEN_EXPIRED
                elif error_ == "unauthorized":
                    vars_["#ERROR#"] = self._UNAUTHORIZED
            else:
                reader_script = "reader_reauth_unhandled.script"
                writer_script = "writer_reauth_unhandled.script"
                expected_call_count = 1
                vars_ = self.get_vars()
                if error_ == "security":
                    vars_["#ERROR#"] = self._SECURITY_EXC
                else:
                    self.fail(f"Unknown error type {error_}")
            self.start_server(self._reader, reader_script, vars_=vars_)
            if routing_:
                self.start_server(self._writer, writer_script, vars_=vars_)
                self.start_server(self._router, "router_single_reader.script")

            with self.driver(auth_manager, routing=routing_,
                             max_connection_pool_size=3) as driver:
                if routing_:
                    with self.session(driver, "w") as session_w:
                        list(session_w.run("RETURN 1 AS n"))

                with self.session(driver) as session_r1:
                    with self.session(driver) as session_r2:
                        with self.session(driver) as session_r3:
                            # bind connection 1
                            s1_tx = session_r1.begin_transaction()
                            list(s1_tx.run("RETURN 1.1 AS n"))

                            self.assertEqual(1, count)

                            # bind connection 2
                            s2_tx = session_r2.begin_transaction()
                            list(s2_tx.run("RETURN 2.1 AS n"))

                            # bind connection 3
                            s3_tx = session_r3.begin_transaction()
                            list(s3_tx.run("RETURN 3.1 AS n"))

                            s2_tx.commit()

                            self.assertEqual(1, count)

                            with self.assertRaises(types.DriverError) as exc:
                                # connection 2 fails, gets closed
                                list(session_r2.run("RETURN 2.2 AS n"))
                            if error_ == "token_expired":
                                self.assert_is_token_error(
                                    exc.exception, retryable=True
                                )
                            elif error_ == "authorization_expired":
                                self.assert_is_authorization_error(
                                    exc.exception
                                )
                            elif error == "unauthorized":
                                self.assert_is_unauthorized_error(
                                    exc.exception, retryable=True
                                )
                            elif error == "security":
                                self.assert_is_security_error(
                                    exc.exception
                                )
                            else:
                                raise ValueError(f"Unknown error {error_}")

                            # bind connection 2
                            s2_tx = session_r2.begin_transaction()
                            self.assertEqual(expected_call_count, count)
                            list(s2_tx.run("RETURN 2.3 AS n"))

                            # free connection 1
                            s1_tx.commit()
                            # bind connection 1
                            s1_tx = session_r1.begin_transaction()
                            list(s1_tx.run("RETURN 1.2 AS n"))

                            # free connection 3
                            s3_tx.commit()
                            # bind connection 3
                            s3_tx = session_r3.begin_transaction()
                            list(s3_tx.run("RETURN 3.2 AS n"))

                            # free all connections
                            s3_tx.commit()
                            s1_tx.commit()
                            s2_tx.commit()

                if routing_:
                    with self.session(driver, "w") as session_w:
                        list(session_w.run("RETURN 2 AS n"))

            self.assertEqual(expected_call_count, count)
            self._reader.done()
            self.post_script_assertions(self._reader)
            if routing_:
                self._writer.done()
                self.post_script_assertions(self._writer)
                self._router.done()
                self.post_script_assertions(self._router)

        for error in ("authorization_expired", "token_expired",
                      "unauthorized", "security"):
            for routing in (False, True):
                with self.subTest(error=error, routing=routing):
                    try:
                        _test(error, routing)
                    finally:
                        self._reader.reset()
                        self._writer.reset()
                        self._router.reset()

    def test_handles_unknown_auth(self):
        def _trigger_error(runner, error_):
            with self.assertRaises(types.DriverError) as exc:
                # connection fails, gets closed
                list(runner.run("RETURN 2.2 AS n"))
            if error_ == "token_expired":
                self.assert_is_token_error(
                    exc.exception, retryable=True
                )
            elif error == "unauthorized":
                self.assert_is_unauthorized_error(
                    exc.exception, retryable=True
                )
            elif error == "security":
                self.assert_is_security_error(
                    exc.exception
                )
            else:
                raise ValueError(f"Unknown error {error_}")

        def _test(error_, routing_):
            count = 0

            def provider():
                nonlocal count
                count += 1
                credentials = "pass++" if count > 1 else "pass"
                principal = "neo5j" if count > 1 else "neo4j"

                return types.AuthTokenAndExpiration(
                    types.AuthorizationToken(
                        scheme="basic",
                        principal=principal,
                        credentials=credentials
                    ),
                    10_000
                )

            auth_manager = BearerAuthTokenManager(self._backend, provider)

            if error_ in ("unauthorized", "token_expired"):
                reader_script = self.script_fn_with_features(
                    "reader_reauth_handled.script"
                )
                writer_script = self.script_fn_with_features(
                    "writer_reauth_handled.script"
                )
                vars_ = self.get_vars()
                expected_call_count = 2
                if error_ == "token_expired":
                    vars_["#ERROR#"] = self._TOKEN_EXPIRED
                elif error_ == "unauthorized":
                    vars_["#ERROR#"] = self._UNAUTHORIZED
            elif error_ in ("security",):
                reader_script = "reader_reauth_unhandled.script"
                writer_script = "writer_reauth_unhandled.script"
                expected_call_count = 1
                vars_ = self.get_vars()
                vars_["#ERROR#"] = self._SECURITY_EXC
            else:
                self.fail(f"Unknown error type {error_}")
            self.start_server(self._reader, reader_script, vars_=vars_)
            if routing_:
                self.start_server(self._writer, writer_script, vars_=vars_)
                self.start_server(self._router, "router_single_reader.script")

            with self.driver(auth_manager, routing=routing_,
                             max_connection_pool_size=2) as driver:
                if routing_:
                    with self.session(driver, "w") as session_w:
                        list(session_w.run("RETURN 1 AS n"))

                with self.session(driver) as session_r1:
                    with self.session(driver) as session_r2:
                        # bind connection 1
                        s1_tx = session_r1.begin_transaction()
                        list(s1_tx.run("RETURN 2.1 AS n"))

                        self.assertEqual(1, count)

                        # bind connection 2
                        s2_tx = session_r2.begin_transaction()
                        list(s2_tx.run("RETURN 2.1 AS n"))

                        s2_tx.commit()
                        s1_tx.commit()

                        s1_tx = session_r1.begin_transaction()

                        self.assertEqual(1, count)

                        _trigger_error(session_r2, error_)
                        _trigger_error(s1_tx, error_)

                        # bind connection 2
                        s2_tx = session_r2.begin_transaction()
                        self.assertEqual(expected_call_count, count)
                        list(s2_tx.run("RETURN 2.3 AS n"))

                        # bind connection 1
                        s1_tx = session_r1.begin_transaction()
                        list(s1_tx.run("RETURN 2.3 AS n"))

                        # free all connections
                        s1_tx.commit()
                        s2_tx.commit()

                if routing_:
                    with self.session(driver, "w") as session_w:
                        list(session_w.run("RETURN 2 AS n"))

            self.assertEqual(expected_call_count, count)
            self._reader.done()
            self.post_script_assertions(self._reader)
            if routing_:
                self._writer.done()
                self.post_script_assertions(self._writer)
                self._router.done()
                self.post_script_assertions(self._router)

        for error in ("token_expired", "unauthorized", "security"):
            for routing in (False, True):
                with self.subTest(error=error, routing=routing):
                    try:
                        _test(error, routing)
                    finally:
                        self._reader.reset()
                        self._writer.reset()
                        self._router.reset()


class TestBearerAuthManager5x0(TestBearerAuthManager5x1):

    required_features = (types.Feature.BOLT_5_0,
                         types.Feature.AUTH_MANAGED)

    def get_vars(self):
        return {**super().get_vars(), "#VERSION#": "5.0"}

    def test_static_provider(self):
        super().test_static_provider()

    def test_static_provider_long_time(self):
        super().test_static_provider_long_time()

    def test_expiring_token_deadline(self):
        super().test_expiring_token_deadline()

    def test_renewing_on_expiration_error(self):
        super().test_renewing_on_expiration_error()

    def test_handles_unknown_auth(self):
        super().test_handles_unknown_auth()
