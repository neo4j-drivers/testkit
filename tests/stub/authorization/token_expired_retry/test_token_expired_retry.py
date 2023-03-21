from contextlib import contextmanager

from nutkit.frontend import (
    AuthTokenManager,
    Driver,
    TemporalAuthTokenManager,
)
import nutkit.protocol as types
from tests.shared import driver_feature
from tests.stub.authorization.test_authorization import AuthorizationBase
from tests.stub.shared import StubServer


class _TestTokenExpiredRetryBase(AuthorizationBase):

    required_features = types.Feature.API_DRIVER_VERIFY_AUTHENTICATION,

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
            "#VERSION#": "5.1",
        }

    @contextmanager
    def driver(self, auth, routing):
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
    def session(self, driver):
        session = driver.session("r", database="adb")
        try:
            yield session
        finally:
            session.close()

    def _test_retry(self, auth, routing):
        count = 0

        def work(tx):
            nonlocal count
            count += 1
            if count == 1:
                with self.assertRaises(types.DriverError) as exc:
                    res = tx.run("RETURN 1 AS n")
                    list(res)
                self.assert_is_retryable_token_error(exc.exception)
                raise exc.exception
            else:
                res = tx.run(f"RETURN {count} AS n")
                list(res)

        if routing:
            self.start_server(self._router, "router.script")
        self.start_server(self._reader, "reader_retry.script")
        with self.driver(auth, routing) as driver:
            with self.session(driver) as session:
                session.execute_read(work)

        if routing:
            self._router.done()
        self._reader.done()
        self.assertEqual(count, 2)

    def _test_no_retry(self, auth, routing):
        count = 0

        def work(tx):
            nonlocal count
            count += 1
            res = tx.run(f"RETURN {count} AS n")
            list(res)

        if routing:
            self.start_server(self._router, "router.script")
        self.start_server(self._reader, "reader_no_retry.script")
        with self.driver(auth, routing) as driver:
            with self.session(driver) as session:
                with self.assertRaises(types.DriverError) as exc:
                    session.execute_read(work)
        self.assert_is_token_error(exc.exception)

        if routing:
            self._router.done()
        self._reader.done()
        self.assertEqual(count, 1)


class TestTokenExpiredRetryV5x1(_TestTokenExpiredRetryBase):
    def test_no_retry_with_static_token(self):
        for routing in (True, False):
            with self.subTest(routing=routing):
                self._test_no_retry(self._auth1, routing=routing)
            self._reader.reset()
            self._router.reset()

    @driver_feature(types.Feature.AUTH_MANAGED)
    def test_retry_with_temporal_token(self):
        count = 0

        def provider():
            nonlocal count
            count += 1
            if count == 1:
                return types.TemporalAuthToken(self._auth1, None)
            return types.TemporalAuthToken(self._auth2, None)

        for routing in (True, False):
            with self.subTest(routing=routing):
                auth = TemporalAuthTokenManager(self._backend, provider)
                self._test_retry(auth, routing=routing)
                self.assertEqual(count, 2)
            self._reader.reset()
            self._router.reset()
            count = 0

    @driver_feature(types.Feature.AUTH_MANAGED)
    def test_retry_with_static_token(self):
        expired_count = 0
        get_count = 0

        def get_auth():
            nonlocal get_count
            nonlocal expired_count
            get_count += 1
            if expired_count == 0:
                return self._auth1
            return self._auth2

        def on_auth_expired(auth_):
            nonlocal expired_count
            expired_count += 1
            assert auth_ == self._auth1

        for routing in (True, False):
            with self.subTest(routing=routing):
                auth = AuthTokenManager(self._backend, get_auth,
                                        on_auth_expired)
                self._test_retry(auth, routing=routing)
                self.assertEqual(expired_count, 1)
                self.assertEqual(get_count, 3 if routing else 2)
            self._reader.reset()
            self._router.reset()
            expired_count = 0
            get_count = 0


class TestTokenExpiredRetryV5x0(TestTokenExpiredRetryV5x1):
    def get_vars(self):
        return {
            **super().get_vars(),
            "#VERSION#": "5.0",
        }

    def test_no_retry_with_static_token(self):
        super().test_no_retry_with_static_token()

    def test_retry_with_temporal_token(self):
        super().test_retry_with_temporal_token()

    def test_retry_with_static_token(self):
        super().test_retry_with_static_token()
