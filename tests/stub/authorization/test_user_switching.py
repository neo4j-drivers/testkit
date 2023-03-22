from contextlib import contextmanager

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import driver_feature
from tests.stub.authorization.test_authorization import AuthorizationBase
from tests.stub.shared import StubServer


class TestUserSwitchingV5x1(AuthorizationBase):

    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.API_SESSION_AUTH_CONFIG)

    supports_session_auth = True

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

    @staticmethod
    def inverted_script(script_fn):
        return script_fn

    @contextmanager
    def driver(self, routing=False, auth=None, **kwargs):
        if auth is None:
            auth = self._auth1
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

    def post_test_assertions(self, driver_auth, session_auth):
        if not self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            return
        self.assertEqual(0, self._reader.count_requests("RESET"))
        self.assertEqual(0, self._router.count_requests("RESET"))

    @driver_feature(types.Feature.API_DRIVER_SUPPORTS_SESSION_AUTH)
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
        self.assertIs(res, self.supports_session_auth)

    def test_read_with_switch(self):
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver() as driver:
            with self.session(driver=driver) as session:
                list(session.run("RETURN 1 AS n"))
            with self.session(driver=driver, auth=self._auth2) as session:
                list(session.run("RETURN 2 AS n"))
            with self.session(driver=driver, auth=self._auth2) as session:
                # same auth token, no re-auth
                list(session.run("RETURN 3 AS n"))
            with self.session(driver=driver) as session:
                # back to original auth token, re-auth required
                list(session.run("RETURN 4 AS n"))
        self._reader.done()
        self.post_test_assertions(self._auth1, self._auth2)

    def test_read_with_switch_inverse(self):
        self.start_server(
            self._reader,
            self.script_fn_with_features(
                self.inverted_script("reader_user_switch.script")
            )
        )
        with self.driver(auth=self._auth2) as driver:
            with self.session(driver=driver, auth=self._auth1) as session:
                list(session.run("RETURN 1 AS n"))
            with self.session(driver=driver) as session:
                list(session.run("RETURN 2 AS n"))
            with self.session(driver=driver) as session:
                # same auth token, no re-auth
                list(session.run("RETURN 3 AS n"))
            with self.session(driver=driver, auth=self._auth1) as session:
                # back to original auth token, re-auth required
                list(session.run("RETURN 4 AS n"))
        self._reader.done()
        self.post_test_assertions(self._auth2, self._auth1)

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
                list(session.run("RETURN 1 AS n"))
            with self.session(driver=driver, auth=self._auth2) as session:
                list(session.run("RETURN 2 AS n"))
            with self.session(driver=driver, auth=self._auth2) as session:
                # same auth token, no re-auth
                list(session.run("RETURN 3 AS n"))
            with self.session(driver=driver, auth=self._auth1) as session:
                # back to original auth token, re-auth required
                list(session.run("RETURN 4 AS n"))
        self._reader.done()
        self.post_test_assertions(self._auth1, self._auth2)

    def test_read_with_switch_inverse_routing(self):
        self.start_server(
            self._router,
            self.script_fn_with_features(
                self.inverted_script("router_user_switch.script")
            )
        )
        self.start_server(
            self._reader,
            self.script_fn_with_features(
                self.inverted_script("reader_user_switch.script")
            )
        )
        with self.driver(routing=True, auth=self._auth2) as driver:
            with self.session(driver=driver, auth=self._auth1) as session:
                list(session.run("RETURN 1 AS n"))
            with self.session(driver=driver) as session:
                list(session.run("RETURN 2 AS n"))
            with self.session(driver=driver) as session:
                # same auth token, no re-auth
                list(session.run("RETURN 3 AS n"))
            with self.session(driver=driver, auth=self._auth1) as session:
                # back to original auth token, re-auth required
                list(session.run("RETURN 4 AS n"))
        self._reader.done()
        self.post_test_assertions(self._auth2, self._auth1)


class TestUserSwitchingV5x0(TestUserSwitchingV5x1):

    required_features = (types.Feature.BOLT_5_0,
                         types.Feature.API_SESSION_AUTH_CONFIG)

    supports_session_auth = False

    def get_vars(self):
        return {
            **super().get_vars(),
            "#VERSION#": "5.0"
        }

    def script_fn_with_features(self, script_fn):
        # no minimal scripts because we want them to be as permissive as
        # possible and expect them to still fail
        return script_fn

    def test_read_with_switch(self):
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver() as driver:
            with self.session(driver=driver) as session:
                list(session.run("RETURN 1 AS n"))
            with self.assertRaises(types.DriverError) as exc:
                with self.session(driver=driver, auth=self._auth2) as session:
                    list(session.run("RETURN 2 AS n"))
            self.assert_re_auth_unsupported_error(exc.exception)

    def test_read_with_switch_inverse(self):
        self.start_server(
            self._reader,
            self.script_fn_with_features("reader_user_switch.script")
        )
        with self.driver(auth=self._auth2) as driver:
            with self.assertRaises(types.DriverError) as exc:
                with self.session(driver=driver, auth=self._auth1) as session:
                    list(session.run("RETURN 1 AS n"))
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
                list(session.run("RETURN 1 AS n"))
            with self.assertRaises(types.DriverError) as exc:
                with self.session(driver=driver, auth=self._auth2) as session:
                    list(session.run("RETURN 2 AS n"))
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
                    list(session.run("RETURN 1 AS n"))
            self.assert_re_auth_unsupported_error(exc.exception)
