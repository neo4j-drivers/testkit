import json
from contextlib import contextmanager
from dataclasses import dataclass

import nutkit.protocol as types
from nutkit.frontend import (
    AuthTokenManager,
    Driver,
)
from tests.shared import get_driver_name
from tests.stub.authorization.test_authorization import AuthorizationBase
from tests.stub.shared import StubServer


@dataclass(frozen=True)
class HandleSecurityExceptionArgs:
    auth: types.AuthorizationToken
    error_code: str


class TrackingAuthTokenManager:
    def __init__(self, backend):
        self._backend = backend
        self._get_auth_count = 0
        self._handle_security_exception_args = []
        self._manager = AuthTokenManager(
            backend, self.get_auth, self.handle_security_exception
        )

    def get_auth(self):
        self._get_auth_count += 1
        return self.raw_get_auth()

    def raw_get_auth(self):
        return types.AuthorizationToken(
            scheme="basic",
            principal="neo4j",
            credentials="pass"
        )

    def handle_security_exception(
        self, auth: types.AuthorizationToken, code: str
    ) -> bool:
        args = HandleSecurityExceptionArgs(auth, code)
        self._handle_security_exception_args.append(args)
        return self._handles_security_exception(code)

    def _handles_security_exception(self, code: str) -> bool:
        return False

    @property
    def get_auth_count(self):
        return self._get_auth_count

    @property
    def handle_security_exception_args(self):
        return self._handle_security_exception_args

    @property
    def handle_security_exception_count(self):
        return len(self._handle_security_exception_args)

    @property
    def manager(self):
        return self._manager


class TestAuthTokenManager5x1(AuthorizationBase):

    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.AUTH_MANAGED)
    backwards_compatible = False

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
    def session(self, driver, access_mode="r", database="adb",
                auth_token=None):
        session = driver.session(access_mode, database=database,
                                 auth_token=auth_token)
        try:
            yield session
        finally:
            session.close()

    def post_script_assertions(self, server):
        # add OPT_MINIMAL_RESETS assertion (if driver claims to support it)
        if self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            self.assertEqual(server.count_requests("RESET"), 0)

    def test_static_auth_manager(self):
        def _test(routing_):
            auth_manager = TrackingAuthTokenManager(self._backend)

            self.start_server(
                self._reader,
                self.script_fn_with_features("reader_no_reauth.script")
            )
            if routing_:
                self.start_server(self._router, "router_single_reader.script")

            with self.driver(auth_manager.manager, routing=routing_) as driver:
                with self.session(driver) as session:
                    list(session.run("RETURN 1 AS n"))

            self._reader.done()
            if routing_:
                self._router.done()
            self.assertEqual(auth_manager.get_auth_count, 2 if routing_ else 1)
            self.assertEqual(auth_manager.handle_security_exception_count, 0)
            self.post_script_assertions(self._reader)

        for routing in (False, True):
            with self.subTest(routing=routing):
                try:
                    _test(routing)
                finally:
                    self._reader.reset()
                    self._router.reset()

    def test_dynamic_auth_manager(self):
        def _test(routing_):
            get_auth_count = 0
            current_auth = ("neo4j", "pass")
            handle_security_exception_count = 0

            def get_auth():
                nonlocal get_auth_count
                get_auth_count += 1
                user, password = current_auth
                return types.AuthorizationToken(
                    scheme="basic",
                    principal=user,
                    credentials=password
                )

            def handle_security_exception(auth_token, code):
                nonlocal handle_security_exception_count
                handle_security_exception_count += 1
                return False

            auth_manager = AuthTokenManager(self._backend, get_auth,
                                            handle_security_exception)

            self.start_server(
                self._reader,
                self.script_fn_with_features("reader_reauth.script")
            )
            if routing_:
                self.start_server(
                    self._router,
                    "router_reauth.script"
                )

            with self.driver(auth_manager, routing=routing_) as driver:
                for i, auth in enumerate((
                    ("neo4j", "pass"),
                    ("neo4j", "pass"),
                    ("neo5j", "pass++"),
                    ("neo5j", "pass++"),
                )):
                    current_auth = auth
                    # database=None to force home db resolution
                    with self.session(driver, database=None) as session:
                        list(session.run(f"RETURN {i + 1} AS n"))

            self._reader.done()
            if routing_:
                self._router.done()
            expected_get_auth_count = 4
            if self.backwards_compatible:
                # each time the credentials change, the driver will issue an
                # extra getAuth query to authenticate the new connection that
                # replaces the one that got closed as a means of providing
                # backwards compatibility with older servers
                expected_get_auth_count += 1
            if routing_:
                expected_get_auth_count *= 2
            self.assertEqual(get_auth_count, expected_get_auth_count)
            self.assertEqual(handle_security_exception_count, 0)
            self.post_script_assertions(self._reader)
            logon_message = "HELLO" if self.backwards_compatible else "LOGON"
            if routing_:
                hellos = self._router.get_requests(logon_message)
                self.assertEqual(len(hellos), 2)
                assert '"credentials": "pass"' in hellos[0]
                assert '"credentials": "pass++"' in hellos[1]

        for routing in (False, True):
            with self.subTest(routing=routing):
                try:
                    _test(routing)
                finally:
                    self._reader.reset()
                    self._router.reset()

    def _get_error_assertion(self, error, handled):
        def retryable(f, can_retry):
            def inner(*args, **kwargs):
                kwargs["retryable"] = can_retry
                f(*args, **kwargs)
            return inner

        if error == self._AUTH_EXPIRED:
            return retryable(self.assert_is_authorization_error, handled)
        elif error == self._TOKEN_EXPIRED:
            return retryable(self.assert_is_token_error, handled)
        elif error == self._UNAUTHORIZED:
            return retryable(self.assert_is_unauthorized_error, handled)
        elif error == self._SECURITY_EXC:
            return retryable(self.assert_is_security_error, handled)
        elif error == self._TRANSIENT_EXC:
            return self.assert_is_transient_error
        elif error == self._RANDOM_EXC:
            return self.assert_is_random_error
        else:
            raise ValueError(f"Unknown error: {error}")

    def _test_notify(self, script, session_cb):
        def _test(routing_, session_auth_, should_notify_, handled_):
            if session_auth_:
                session_auth_ = types.AuthorizationToken(
                    scheme="basic",
                    principal="neo4j-session",
                    credentials="pass"
                )
            else:
                session_auth_ = None

            class AuthManager(TrackingAuthTokenManager):
                def _handles_security_exception(self, code: str) -> bool:
                    return handled_

            manager = AuthManager(self._backend)
            if routing_:
                self.start_server(self._router, "router_single_reader.script")
            vars_ = self.get_vars()
            vars_["#ERROR#"] = error
            self.start_server(self._reader, script, vars_=vars_)
            with self.driver(manager.manager, routing=routing_) as driver:
                with self.session(driver, auth_token=session_auth_) as session:
                    exc = session_cb(session)
                    error_assertion = self._get_error_assertion(
                        error, handled_
                    )
                    error_assertion(exc.exception)
            self._reader.done()
            if routing_:
                self._router.done()
            if session_auth_:
                self.assertEqual(manager.get_auth_count, 0)
            else:
                if routing_:
                    self.assertEqual(manager.get_auth_count, 2)
                else:
                    self.assertEqual(manager.get_auth_count, 1)
            if should_notify_:
                self.assertEqual(manager.handle_security_exception_count, 1)
                self.assertEqual(
                    manager.handle_security_exception_args,
                    [HandleSecurityExceptionArgs(manager.raw_get_auth(),
                                                 error_code)]
                )
            else:
                self.assertEqual(manager.handle_security_exception_count, 0)

            self.post_script_assertions(self._reader)

        session_auths = [False]
        if not self.backwards_compatible:
            session_auths.append(True)

        for session_auth in session_auths:
            for routing in (False, True):
                for error in (
                    self._AUTH_EXPIRED,
                    self._TOKEN_EXPIRED,
                    self._UNAUTHORIZED,
                    self._SECURITY_EXC,
                    self._TRANSIENT_EXC,
                    self._RANDOM_EXC,
                ):
                    error_code = self._get_error_code(error)
                    should_notify = (
                        error_code.startswith("Neo.ClientError.Security.")
                        and not session_auth
                    )
                    handles = [False]
                    if should_notify:
                        handles.append(True)
                    for handled in handles:
                        with self.subTest(
                            routing=routing, session_auth=session_auth,
                            error=error_code, handled=handled
                        ):
                            try:
                                _test(routing, session_auth, should_notify,
                                      handled)
                            finally:
                                self._reader.reset()
                                self._router.reset()

    @staticmethod
    def _get_error_code(error):
        return json.loads(error)["code"]

    def test_error_on_pull_using_session_run(self):
        def session_cb(session):
            result = session.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.next()
            return exc

        self._test_notify("reader_yielding_error_on_pull.script", session_cb)

    def test_error_on_begin_using_tx_run(self):
        def session_cb(session):
            if not self.driver_supports_features(
                types.Feature.OPT_EAGER_TX_BEGIN
            ):
                tx = session.begin_transaction()
                with self.assertRaises(types.DriverError) as exc:
                    tx.run("cypher").next()
            else:
                # this is what all drivers should do
                with self.assertRaises(types.DriverError) as exc:
                    session.begin_transaction()
            return exc

        self._test_notify("reader_tx_yielding_error_on_begin.script",
                          session_cb)

    def test_error_on_run_using_tx_run(self):
        def session_cb(session):
            tx = session.begin_transaction()
            with self.assertRaises(types.DriverError) as exc:
                result = tx.run("RETURN 1 AS n")
                # TODO:
                #   remove consume() once all drivers report the error on run
                if get_driver_name() in ["javascript", "dotnet"]:
                    result.consume()
            return exc

        self._test_notify("reader_tx_yielding_error_on_run.script", session_cb)

    def test_error_on_pull_using_tx_run(self):
        def session_cb(session):
            tx = session.begin_transaction()
            with self.assertRaises(types.DriverError) as exc:
                result = tx.run("RETURN 1 AS n")
                result.next()
            return exc

        self._test_notify("reader_tx_yielding_error_on_pull.script",
                          session_cb)

    def test_error_on_commit_using_tx_run(self):
        def session_cb(session):
            tx = session.begin_transaction()
            tx.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                tx.commit()
            return exc

        self._test_notify(
            "reader_tx_yielding_error_on_commit_with_pull_or_discard.script",
            session_cb
        )

    def test_error_on_rollback_using_tx_run(self):
        def session_cb(session):
            tx = session.begin_transaction()
            tx.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                tx.rollback()
            return exc

        self._test_notify(
            "reader_tx_yielding_error_on_rollback_with_pull_or_discard.script",
            session_cb
        )


class TestAuthTokenManager5x0(TestAuthTokenManager5x1):

    required_features = (types.Feature.BOLT_5_0,
                         types.Feature.AUTH_MANAGED)
    backwards_compatible = True

    def get_vars(self):
        return {**super().get_vars(), "#VERSION#": "5.0"}

    def test_static_auth_manager(self):
        super().test_static_auth_manager()

    def test_dynamic_auth_manager(self):
        super().test_dynamic_auth_manager()

    def test_error_on_pull_using_session_run(self):
        super().test_error_on_pull_using_session_run()

    def test_error_on_begin_using_tx_run(self):
        super().test_error_on_begin_using_tx_run()

    def test_error_on_run_using_tx_run(self):
        super().test_error_on_run_using_tx_run()

    def test_error_on_pull_using_tx_run(self):
        super().test_error_on_pull_using_tx_run()

    def test_error_on_commit_using_tx_run(self):
        super().test_error_on_commit_using_tx_run()

    def test_error_on_rollback_using_tx_run(self):
        super().test_error_on_rollback_using_tx_run()
