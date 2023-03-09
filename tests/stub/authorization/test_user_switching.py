from collections import Counter
from contextlib import contextmanager
import re

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import driver_feature
from tests.stub.authorization.test_authorization import AuthorizationBase
from tests.stub.shared import StubServer


class TestUserSwitchingV5x1(AuthorizationBase):

    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.API_SESSION_AUTH_CONFIG)

    supports_session_auth = True
    backwards_compatible_auth = None

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
        driver_kwargs = {
            "backwards_compatible_auth": self.backwards_compatible_auth,
        }
        driver_kwargs.update(kwargs)
        driver = Driver(self._backend, uri, auth, **driver_kwargs)
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


class TestUserSwitchingV5x0BackwardsCompatibility(TestUserSwitchingV5x1):

    required_features = (types.Feature.BOLT_5_0,
                         types.Feature.API_SESSION_AUTH_CONFIG,
                         types.Feature.INTERNAL_USER_SWITCH_POLYFILL)

    supports_session_auth = False
    backwards_compatible_auth = True

    def get_vars(self):
        return {
            **super().get_vars(),
            "#VERSION#": "5.0"
        }

    @staticmethod
    def inverted_script(script_fn):
        parts = script_fn.rsplit(".", 1)
        return f"{parts[0]}_inverse.{parts[1]}"

    def script_fn_with_features(self, script_fn):
        parts = script_fn.rsplit(".", 1)
        script_fn = f"{parts[0]}_backwards_compat.{parts[1]}"
        return super().script_fn_with_features(script_fn)

    def post_test_assertions(self, driver_auth, session_auth):
        super().post_test_assertions(driver_auth, session_auth)

        self.assertEqual(self._reader.count_responses("<ACCEPT>"), 3)
        self.assertEqual(self._reader.count_responses("<HANGUP>"), 3)

        in_throwaway_connection = False
        throwaway_connection_count = 0
        last_run = 0
        saw_run = False
        conversation = self._reader.get_conversation()
        for message in conversation:
            if (
                message.startswith("S:  HELLO")
                and re.match(r'.*"principal": "([^"]*)".*', message).group(1)
                    == session_auth.principal
            ):
                in_throwaway_connection = True
                throwaway_connection_count += 1
                saw_run = False
            elif message.startswith("S:  RUN"):
                last_run += 1
                self.assertIn(f"RETURN {last_run}", message)
                if in_throwaway_connection:
                    self.assertFalse(saw_run)
                    saw_run = True
            elif (
                in_throwaway_connection
                and message.startswith("C:  <HANGUP>")
            ):
                in_throwaway_connection = False
                self.assertTrue(saw_run)

        self.assertEqual(throwaway_connection_count, 2)

        if self._router.get_conversation():
            # only for routing tests
            self.assertEqual(self._router.count_responses("<ACCEPT>"), 3)
            self.assertEqual(self._router.count_responses("<HANGUP>"), 3)

            conversation = self._router.get_requests("HELLO")
            self.assertEqual(len(conversation), 3)
            principals = Counter(
                re.match(r'.*"principal": "([^"]*)".*', message).group(1)
                for message in conversation
            )
            self.assertEqual(
                Counter({session_auth.principal: 2, driver_auth.principal: 1}),
                principals,
            )

    def test_supports_session_auth(self):
        super().test_supports_session_auth()

    def test_read_with_switch(self):
        super().test_read_with_switch()

    def test_read_with_switch_inverse(self):
        super().test_read_with_switch_inverse()

    def test_read_with_switch_routing(self):
        super().test_read_with_switch_routing()

    def test_read_with_switch_inverse_routing(self):
        super().test_read_with_switch_inverse_routing()

    def test_pool_closes_idle_connection_for_throwaway_connection(self):
        self.start_server(
            self._reader,
            self.script_fn_with_features(
                "reader_user_switch_pool_limit.script"
            )
        )
        with self.driver(max_connection_pool_size=2) as driver:
            with self.session(driver=driver) as session1:
                tx1 = session1.begin_transaction()
                list(tx1.run("RETURN 1/2 AS n"))

                with self.session(driver=driver) as session2:
                    list(session2.run("RETURN 2 AS n"))

                self.assertEqual(self._reader.count_responses("<HANGUP>"), 0)

                with self.session(driver=driver, auth=self._auth2) as session3:
                    list(session3.run("RETURN 3 AS n"))

                # client closes idle connection of session 2 to make room for
                # throwaway connection + closes throwaway connection
                self.assertEqual(self._reader.count_responses("<HANGUP>"), 2)

                # session 1 is still alive
                list(tx1.run("RETURN 2/2 AS n"))
                tx1.commit()


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
