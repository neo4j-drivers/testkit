from contextlib import contextmanager

from nutkit.frontend import (
    Driver,
    FakeTime,
)
import nutkit.protocol as types
from tests.shared import driver_feature
from tests.stub.authorization.test_authorization import AuthorizationBase
from tests.stub.shared import StubServer


class _TestVerifyAuthenticationBase(AuthorizationBase):

    required_features = types.Feature.API_DRIVER_VERIFY_AUTHENTICATION,

    VERIFY_AUTH_NEGATIVE_ERRORS = (
        "Neo.ClientError.Security.CredentialsExpired",
        "Neo.ClientError.Security.Forbidden",
        "Neo.ClientError.Security.TokenExpired",
        "Neo.ClientError.Security.Unauthorized",
    )

    VERIFY_AUTH_PROPAGATE_ERRORS = (
        # Don't include AuthorizationExpired as it's explicitly handled to not
        # fail fast during discovery. Hence, it does not behave like other
        # security errors when returned from the router.
        # "Neo.ClientError.Security.AuthorizationExpired",
        "Neo.ClientError.Security.MadeUp",
        "Neo.ClientError.Security.AuthenticationRateLimit",
    )

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader = StubServer(9010)
        self._auth1 = types.AuthorizationToken("basic", principal="neo4j",
                                               credentials="pass")
        self._auth2 = types.AuthorizationToken("basic", principal="neo5j",
                                               credentials="pass++")

    def tearDown(self):
        self._router.reset()
        self._reader.reset()
        super().tearDown()

    def get_vars(self):
        return {
            "#HOST#": self._router.host,
            "#VERSION#": "5.1"
        }

    @contextmanager
    def driver(self, routing=False):
        auth = self._auth1
        if routing:
            uri = f"neo4j://{self._router.address}"
        else:
            uri = f"bolt://{self._reader.address}"
        driver = Driver(
            self._backend, uri, auth
        )
        try:
            yield driver
        finally:
            driver.close()

    def vars_with_auth(self, driver_auth, verify_auth):
        return {
            **self.get_vars(),
            "#DRIVER_AUTH#": f'"principal": "{driver_auth.principal}", '
                             f'"credentials": "{driver_auth.credentials}"',
            "#VERIFY_AUTH#": f'"principal": "{verify_auth.principal}", '
                             f'"credentials": "{verify_auth.credentials}"',
        }

    def _test_successful_authentication(self):
        def test(routing_, warm_, auth_):
            suffix = "_warm" if warm_ else ""
            vars_ = self.vars_with_auth(self._auth1, auth_)
            if routing_:
                self.start_server(self._router, f"router{suffix}.script",
                                  vars_=vars_)
            self.start_server(self._reader, f"reader{suffix}.script",
                              vars_=vars_)

            with self.driver(routing=routing_) as driver:
                if warm_:
                    session = driver.session("r", database="system")
                    list(session.run("TESTKIT WARMUP"))
                    session.close()
                res = driver.verify_authentication(auth_)

            self.assertIs(res, True)

            if routing_:
                self._router.done()
            self._reader.done()

            if self.driver_supports_features(
                types.Feature.OPT_MINIMAL_VERIFY_AUTHENTICATION
            ):
                if routing_:
                    self.assertEqual(self._router.count_requests("LOGON"), 1)
                logon_count = self._reader.count_requests("LOGON")
                run_count = self._reader.count_requests("RUN")
                expected_logon_count = 2 if warm_ else 1
                expected_run_count = 1 if warm_ else 0
                self.assertEqual(logon_count, expected_logon_count)
                self.assertEqual(run_count, expected_run_count)

        for auth in (self._auth1, self._auth2):
            for routing in (False, True):
                for warm in (False, True):
                    with self.subTest(routing=routing, warm=warm,
                                      auth=auth.__dict__):
                        test(routing, warm, auth)
                    self._router.reset()
                    self._reader.reset()

    def _test_router_failure(self):
        # only works with cold routing driver

        def test(error_, raises_, router_script_, auth_):
            vars_ = {
                **self.vars_with_auth(self._auth1, auth_),
                "#ERROR#": error_,
            }
            self.start_server(self._router, router_script_, vars_=vars_)
            with self.driver(routing=True) as driver:
                if raises_:
                    with self.assertRaises(types.DriverError) as exc:
                        driver.verify_authentication(auth_)
                    self.assertEqual(exc.exception.code, error_)
                else:
                    res = driver.verify_authentication(auth_)
                    self.assertIs(res, False)
            self._router.done()

        for auth in (self._auth1, self._auth2):
            for (error, raises) in (
                *((e, False) for e in self.VERIFY_AUTH_NEGATIVE_ERRORS),
                *((e, True) for e in self.VERIFY_AUTH_PROPAGATE_ERRORS),
            ):
                for router_script in ("router_error_logon.script",
                                      "router_error_route.script"):
                    with self.subTest(error=error, raises=raises,
                                      script=router_script,
                                      auth=auth.__dict__):
                        test(error, raises, router_script, auth)
                    self._router.done()
                    self._router.reset()

    @driver_feature(types.Feature.BACKEND_MOCK_TIME)
    def _test_warm_router_failure(self):
        # only works with routing driver

        def test(error_, raises_, router_script_, auth_):
            with FakeTime(self._backend) as time:
                vars_ = {
                    **self.vars_with_auth(self._auth1, auth_),
                    "#ERROR#": error_,
                }
                self.start_server(self._router, router_script_, vars_=vars_)
                self.start_server(self._reader, "reader_warm.script",
                                  vars_=vars_)
                with self.driver(routing=True) as driver:
                    # warm up driver
                    session = driver.session("r", database="system")
                    list(session.run("TESTKIT WARMUP"))
                    session.close()
                    # make routing table expire
                    time.tick(1_001_000)
                    if raises_:
                        with self.assertRaises(types.DriverError) as exc:
                            driver.verify_authentication(auth_)
                        self.assertEqual(exc.exception.code, error_)
                    else:
                        res = driver.verify_authentication(auth_)
                        self.assertIs(res, False)
                self._router.done()

        router_scripts = ["router_error_route_warm.script"]
        for auth in (self._auth1, self._auth2):
            for (error, raises) in (
                *((e, False) for e in self.VERIFY_AUTH_NEGATIVE_ERRORS),
                *((e, True) for e in self.VERIFY_AUTH_PROPAGATE_ERRORS),
            ):
                for router_script in router_scripts:
                    with self.subTest(error=error, raises=raises,
                                      script=router_script,
                                      auth=auth.__dict__):
                        test(error, raises, router_script, auth)
                    self._router.reset()
                    self._reader.reset()

    def _test_reader_failure(self):
        def test(routing_, warm_, error_, raises_, auth_):
            suffix = "_warm" if warm_ else ""
            vars_ = {
                **self.vars_with_auth(self._auth1, auth_),
                "#ERROR#": error_,
            }
            if routing_:
                self.start_server(self._router, f"router{suffix}.script",
                                  vars_=vars_)
            self.start_server(self._reader, f"reader_error{suffix}.script",
                              vars_=vars_)
            with self.driver(routing=routing_) as driver:
                if warm_:
                    session = driver.session("r", database="system")
                    list(session.run("TESTKIT WARMUP"))
                    session.close()
                if raises_:
                    with self.assertRaises(types.DriverError) as exc:
                        driver.verify_authentication(auth_)
                    self.assertEqual(exc.exception.code, error_)
                else:
                    res = driver.verify_authentication(auth_)
                    self.assertIs(res, False)
            if routing_:
                self._router.done()
            self._reader.done()

        for auth in (self._auth1, self._auth2):
            for (error, raises) in (
                *((e, False) for e in self.VERIFY_AUTH_NEGATIVE_ERRORS),
                *((e, True) for e in self.VERIFY_AUTH_PROPAGATE_ERRORS),
                ("Neo.ClientError.Security.AuthorizationExpired", True),
            ):
                for routing in (False, True):
                    for warm in (False, True):
                        with self.subTest(routing=routing, warm=warm,
                                          error=error, raises=raises,
                                          auth=auth.__dict__):
                            test(routing, warm, error, raises, auth)
                        self._router.reset()
                        self._reader.reset()


class TestVerifyAuthenticationV5x1(_TestVerifyAuthenticationBase):

    required_features = (*_TestVerifyAuthenticationBase.required_features,
                         types.Feature.BOLT_5_1)

    def test_successful_authentication(self):
        super()._test_successful_authentication()

    def test_router_failure(self):
        super()._test_router_failure()

    def test_warm_router_failure(self):
        super()._test_warm_router_failure()

    def test_reader_failure(self):
        super()._test_reader_failure()


class TestVerifyAuthenticationV5x0(_TestVerifyAuthenticationBase):

    required_features = (*_TestVerifyAuthenticationBase.required_features,
                         types.Feature.BOLT_5_0)

    def get_vars(self):
        return {
            **super().get_vars(),
            "#VERSION#": "5.0"
        }

    def test_is_not_supported(self):
        def test(routing_, warm_):
            if routing_:
                self.start_server(self._router, "router.script")
            self.start_server(self._reader, "reader.script")

            with self.driver(routing=routing_) as driver:
                if warm_:
                    session = driver.session("r", database="system")
                    list(session.run("TESTKIT WARMUP"))
                    session.close()
                with self.assertRaises(types.DriverError) as exc:
                    driver.verify_authentication(self._auth1)
                self.assert_re_auth_unsupported_error(exc.exception)

            self._router.reset()
            self._reader.reset()

        for routing in (False, True):
            for warm in (False, True):
                with self.subTest(routing=routing, warm=warm):
                    test(routing, warm)
                self._router.reset()
                self._reader.reset()
