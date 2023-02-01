from contextlib import contextmanager

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import get_driver_name
from tests.stub.authorization.test_authorization import AuthorizationBase
from tests.stub.shared import StubServer


class TestUserSwitchingV5x1(AuthorizationBase):

    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.API_SESSION_AUTH_CONFIG)

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
    def driver(self, routing=False, auth=None):
        if auth is None:
            auth = self._auth1
        if routing:
            uri = f"neo4j://{self._router.address}"
        else:
            uri = f"bolt://{self._reader.address}"
        driver = Driver(self._backend, uri, auth)
        try:
            yield driver
        finally:
            driver.close()

    @contextmanager
    def session(self, auth=None, routing=False, driver=None):
        if driver is None:
            with self.driver(routing) as driver:
                session = driver.session("r", auth_token=auth)
                try:
                    yield session
                finally:
                    session.close()
        else:
            session = driver.session("r", auth_token=auth)
            try:
                yield session
            finally:
                session.close()

    def post_test_assertions(self):
        if not self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            return
        self.assertEqual(0, self._reader.count_requests("RESET"))
        self.assertEqual(0, self._router.count_requests("RESET"))

    def test_supports_session_auth(self):
        self.start_server(
            self._router,
            self.script_fn_with_features("router_user_switch.script")
        )
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver() as driver:
            res = driver.supports_session_auth()
        self.assertIs(res, True)

    def test_read_with_switch(self):
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver() as driver:
            with self.session(driver=driver) as session:
                session.run("RETURN 1 AS n").consume()
            with self.session(driver=driver, auth=self._auth2) as session:
                session.run("RETURN 2 AS n").consume()
            with self.session(driver=driver, auth=self._auth2) as session:
                # same auth token, no re-auth
                session.run("RETURN 3 AS n").consume()
            with self.session(driver=driver) as session:
                # back to original auth token, re-auth required
                session.run("RETURN 4 AS n").consume()
        self._reader.done()
        self.post_test_assertions()

    def test_read_with_switch_inverse(self):
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver(auth=self._auth2) as driver:
            with self.session(driver=driver, auth=self._auth1) as session:
                session.run("RETURN 1 AS n").consume()
            with self.session(driver=driver) as session:
                session.run("RETURN 2 AS n").consume()
            with self.session(driver=driver) as session:
                # same auth token, no re-auth
                session.run("RETURN 3 AS n").consume()
            with self.session(driver=driver, auth=self._auth1) as session:
                # back to original auth token, re-auth required
                session.run("RETURN 4 AS n").consume()
        self._reader.done()
        self.post_test_assertions()

    def test_read_with_switch_routing(self):
        self.start_server(
            self._router,
            self.script_fn_with_features("router_user_switch.script")
        )
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver(routing=True) as driver:
            with self.session(driver=driver) as session:
                session.run("RETURN 1 AS n").consume()
            with self.session(driver=driver, auth=self._auth2) as session:
                session.run("RETURN 2 AS n").consume()
            with self.session(driver=driver, auth=self._auth2) as session:
                # same auth token, no re-auth
                session.run("RETURN 3 AS n").consume()
            with self.session(driver=driver, auth=self._auth1) as session:
                # back to original auth token, re-auth required
                session.run("RETURN 4 AS n").consume()
        self._reader.done()
        self.post_test_assertions()

    def test_read_with_switch_inverse_routing(self):
        self.start_server(
            self._router,
            self.script_fn_with_features("router_user_switch.script")
        )
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver(routing=True, auth=self._auth2) as driver:
            with self.session(driver=driver, auth=self._auth1) as session:
                session.run("RETURN 1 AS n").consume()
            with self.session(driver=driver) as session:
                session.run("RETURN 2 AS n").consume()
            with self.session(driver=driver) as session:
                # same auth token, no re-auth
                session.run("RETURN 3 AS n").consume()
            with self.session(driver=driver, auth=self._auth1) as session:
                # back to original auth token, re-auth required
                session.run("RETURN 4 AS n").consume()
        self._reader.done()


class TestUserSwitchingV5x0(TestUserSwitchingV5x1):

    required_features = (types.Feature.BOLT_5_0,
                         types.Feature.API_SESSION_AUTH_CONFIG)

    def get_vars(self):
        return {
            **super().get_vars(),
            "#VERSION#": "5.0"
        }

    def script_fn_with_features(self, script_fn):
        # no minimal scripts because we want them to be as permissive as
        # possible and expect them to still fail
        return script_fn

    def assert_re_auth_unsupported_error(self, error):
        self.assertIsInstance(error, types.DriverError)
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.ConfigurationError'>",
                error.errorType
            )
            self.assertIn(
                "session level authentication is not supported for bolt "
                "protocol version(5, 0)",
                error.msg.lower()
            )
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def test_supports_session_auth(self):
        self.start_server(
            self._router,
            self.script_fn_with_features("router_user_switch.script")
        )
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver() as driver:
            res = driver.supports_session_auth()
        self.assertIs(res, False)

    def test_read_with_switch(self):
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver() as driver:
            with self.session(driver=driver) as session:
                session.run("RETURN 1 AS n").consume()
            with self.assertRaises(types.DriverError) as exc:
                with self.session(driver=driver, auth=self._auth2) as session:
                    session.run("RETURN 2 AS n").consume()
            self.assert_re_auth_unsupported_error(exc.exception)

    def test_read_with_switch_inverse(self):
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver(auth=self._auth2) as driver:
            with self.assertRaises(types.DriverError) as exc:
                with self.session(driver=driver, auth=self._auth1) as session:
                    session.run("RETURN 1 AS n").consume()
            self.assert_re_auth_unsupported_error(exc.exception)

    def test_read_with_switch_routing(self):
        self.start_server(
            self._router,
            self.script_fn_with_features("router_user_switch.script")
        )
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver(routing=True) as driver:
            with self.session(driver=driver) as session:
                session.run("RETURN 1 AS n").consume()
            with self.assertRaises(types.DriverError) as exc:
                with self.session(driver=driver, auth=self._auth2) as session:
                    session.run("RETURN 2 AS n").consume()
            self.assert_re_auth_unsupported_error(exc.exception)

    def test_read_with_switch_inverse_routing(self):
        self.start_server(
            self._router,
            self.script_fn_with_features("router_user_switch.script")
        )
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver(routing=True, auth=self._auth2) as driver:
            with self.assertRaises(types.DriverError) as exc:
                with self.session(driver=driver, auth=self._auth1) as session:
                    session.run("RETURN 1 AS n").consume()
            self.assert_re_auth_unsupported_error(exc.exception)
