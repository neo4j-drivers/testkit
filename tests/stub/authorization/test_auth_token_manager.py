from contextlib import contextmanager

from nutkit.frontend import (
    AuthTokenManager,
    Driver,
)
import nutkit.protocol as types
from tests.shared import get_driver_name
from tests.stub.authorization.test_authorization import AuthorizationBase
from tests.stub.shared import StubServer


class TrackingAuthTokenManager:
    def __init__(self, backend):
        self._backend = backend
        self._get_auth_count = 0
        self._on_auth_expired_args = []
        self._manager = AuthTokenManager(
            backend, self.get_auth, self.on_auth_expired
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

    def on_auth_expired(self, auth):
        self._on_auth_expired_args.append(auth)

    @property
    def get_auth_count(self):
        return self._get_auth_count

    @property
    def on_auth_expired_args(self):
        return self._on_auth_expired_args

    @property
    def on_auth_expired_count(self):
        return len(self._on_auth_expired_args)

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
            self.assertEqual(auth_manager.on_auth_expired_count, 0)
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
            on_auth_expired_count = 0

            def get_auth():
                nonlocal get_auth_count
                get_auth_count += 1
                user, password = current_auth
                return types.AuthorizationToken(
                    scheme="basic",
                    principal=user,
                    credentials=password
                )

            def on_auth_expired(auth_token):
                nonlocal on_auth_expired_count
                on_auth_expired_count += 1

            auth_manager = AuthTokenManager(self._backend,
                                            get_auth, on_auth_expired)

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
                    ("neo4j", "password"),
                    ("neo4j", "password"),
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
            self.assertEqual(on_auth_expired_count, 0)
            self.post_script_assertions(self._reader)
            logon_message = "HELLO" if self.backwards_compatible else "LOGON"
            if routing_:
                hellos = self._router.get_requests(logon_message)
                self.assertEqual(len(hellos), 2)
                assert '"credentials": "pass"' in hellos[0]
                assert '"credentials": "password"' in hellos[1]

        for routing in (False, True):
            with self.subTest(routing=routing):
                try:
                    _test(routing)
                finally:
                    self._reader.reset()
                    self._router.reset()

    def _test_notify(self, error, error_assertion, script, session_cb):
        def _test(routing_, session_auth_):
            if session_auth_:
                session_auth_ = types.AuthorizationToken(
                    scheme="basic",
                    principal="neo4j-session",
                    credentials="pass"
                )
            else:
                session_auth_ = None
            manager = TrackingAuthTokenManager(self._backend)
            if routing_:
                self.start_server(self._router, "router_single_reader.script")
            vars_ = self.get_vars()
            vars_["#ERROR#"] = error
            self.start_server(self._reader, script, vars_=vars_)
            with self.driver(manager.manager, routing=routing_) as driver:
                with self.session(driver, auth_token=session_auth_) as session:
                    exc = session_cb(session)
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
            if error == self._TOKEN_EXPIRED and not session_auth_:
                self.assertEqual(manager.on_auth_expired_count, 1)
                self.assertEqual(manager.on_auth_expired_args,
                                 [manager.raw_get_auth()])
            else:
                self.assertEqual(manager.on_auth_expired_count, 0)

            self.post_script_assertions(self._reader)

        session_auths = [False]
        if not self.backwards_compatible:
            session_auths.append(True)

        for session_auth in session_auths:
            for routing in (False, True):
                with self.subTest(routing=routing, session_auth=session_auth):
                    try:
                        _test(routing, session_auth)
                    finally:
                        self._reader.reset()
                        self._router.reset()

    def _notify_on_failed_pull_using_session_run(self, error, error_assertion):
        def session_cb(session):
            result = session.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.next()
            return exc

        self._test_notify(
            error, error_assertion,
            "reader_yielding_error_on_pull.script",
            session_cb
        )

    def test_not_notify_on_auth_expired_pull_using_session_run(self):
        self._notify_on_failed_pull_using_session_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    def test_notify_on_token_expired_pull_using_session_run(self):
        self._notify_on_failed_pull_using_session_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    def _notify_on_failed_begin_using_tx_run(self, error, error_assertion):
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

        self._test_notify(
            error, error_assertion,
            "reader_tx_yielding_error_on_begin.script",
            session_cb
        )

    def test_not_notify_on_auth_expired_begin_using_tx_run(self):
        if get_driver_name() in ["javascript"]:
            self.skipTest("Fails on sending RESET after auth-error and "
                          "surfaces SessionExpired instead.")
        self._notify_on_failed_begin_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    def test_notify_on_token_expired_begin_using_tx_run(self):
        self._notify_on_failed_begin_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    def _notify_on_failed_run_using_tx_run(self, error, error_assertion):
        def session_cb(session):
            tx = session.begin_transaction()
            with self.assertRaises(types.DriverError) as exc:
                result = tx.run("RETURN 1 AS n")
                # TODO:
                #   remove consume() once all drivers report the error on run
                if get_driver_name() in ["javascript", "dotnet"]:
                    result.consume()
            return exc

        self._test_notify(
            error, error_assertion,
            "reader_tx_yielding_error_on_run.script",
            session_cb
        )

    def test_not_notify_on_auth_expired_run_using_tx_run(self):
        self._notify_on_failed_run_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    def test_notify_on_token_expired_run_using_tx_run(self):
        self._notify_on_failed_run_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    def _notify_on_failed_pull_using_tx_run(self, error, error_assertion):
        def session_cb(session):
            tx = session.begin_transaction()
            with self.assertRaises(types.DriverError) as exc:
                result = tx.run("RETURN 1 AS n")
                result.next()
            return exc

        self._test_notify(
            error, error_assertion,
            "reader_tx_yielding_error_on_pull.script",
            session_cb
        )

    def test_not_notify_on_auth_expired_pull_using_tx_run(self):
        self._notify_on_failed_pull_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    def test_notify_on_token_expired_pull_using_tx_run(self):
        self._notify_on_failed_pull_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    def _notify_on_failed_commit_using_tx_run(self, error, error_assertion):
        def session_cb(session):
            tx = session.begin_transaction()
            tx.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                tx.commit()
            return exc

        self._test_notify(
            error, error_assertion,
            "reader_tx_yielding_error_on_commit_with_pull_or_discard.script",
            session_cb
        )

    def test_not_notify_on_auth_expired_commit_using_tx_run(self):
        self._notify_on_failed_commit_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    def test_notify_on_token_expired_commit_using_tx_run(self):
        self._notify_on_failed_commit_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    def _notify_on_failed_rollback_using_tx_run(self, error, error_assertion):
        def session_cb(session):
            tx = session.begin_transaction()
            tx.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                tx.rollback()
            return exc

        self._test_notify(
            error, error_assertion,
            "reader_tx_yielding_error_on_rollback_with_pull_or_discard.script",
            session_cb
        )

    def test_not_notify_on_auth_expired_rollback_using_tx_run(self):
        self._notify_on_failed_rollback_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    def test_notify_on_token_expired_rollback_using_tx_run(self):
        self._notify_on_failed_rollback_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
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

    def test_not_notify_on_auth_expired_pull_using_session_run(self):
        super().test_not_notify_on_auth_expired_pull_using_session_run()

    def test_notify_on_token_expired_pull_using_session_run(self):
        super().test_notify_on_token_expired_pull_using_session_run()

    def test_not_notify_on_auth_expired_begin_using_tx_run(self):
        super().test_not_notify_on_auth_expired_begin_using_tx_run()

    def test_notify_on_token_expired_begin_using_tx_run(self):
        super().test_notify_on_token_expired_begin_using_tx_run()

    def test_not_notify_on_auth_expired_run_using_tx_run(self):
        super().test_not_notify_on_auth_expired_run_using_tx_run()

    def test_notify_on_token_expired_run_using_tx_run(self):
        super().test_notify_on_token_expired_run_using_tx_run()

    def test_not_notify_on_auth_expired_pull_using_tx_run(self):
        super().test_not_notify_on_auth_expired_pull_using_tx_run()

    def test_notify_on_token_expired_pull_using_tx_run(self):
        super().test_notify_on_token_expired_pull_using_tx_run()

    def test_not_notify_on_auth_expired_commit_using_tx_run(self):
        super().test_not_notify_on_auth_expired_commit_using_tx_run()

    def test_notify_on_token_expired_commit_using_tx_run(self):
        super().test_notify_on_token_expired_commit_using_tx_run()

    def test_not_notify_on_auth_expired_rollback_using_tx_run(self):
        super().test_not_notify_on_auth_expired_rollback_using_tx_run()

    def test_notify_on_token_expired_rollback_using_tx_run(self):
        super().test_notify_on_token_expired_rollback_using_tx_run()
