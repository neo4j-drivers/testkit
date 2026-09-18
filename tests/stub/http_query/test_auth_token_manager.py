from __future__ import annotations

import json
import typing as t
from contextlib import contextmanager

import nutkit.protocol as types
from nutkit.frontend import AuthTokenManager
from tests.shared import (
    get_driver_name,
    Potential,
)
from tests.stub.authorization.base import (
    AuthorizationBase,
    HandleSecurityExceptionArgs,
    TrackingAuthTokenManager,
)
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoint_builders import (
    TxEndpointBuilder,
)
from tests.stub.http_query.shared.http_endpoints import (
    CustomAuthToken,
    HttpEndpoint,
    HttpQueryEndpoint,
    HttpSequenceEndpoint,
)
from tests.stub.http_query.shared.http_server import (
    HandlerType,
    HTTPServer,
)

if t.TYPE_CHECKING:
    from collections.abc import Mapping

    from nutkit.frontend import (
        Driver,
        Session,
    )

    class ErrorAssertionRetryable(t.Protocol):
        def __call__(
            self,
            error: types.DriverError,
            retryable: bool = ...,
        ) -> None: ...

    T = t.TypeVar("T")
    TExc = t.TypeVar("TExc", bound=Exception)


DB = "dba"
FIELDS = ["n"]
ANY_AUTH = CustomAuthToken()


def query(i: int) -> str:
    return f"RETURN {i} AS n"


def records(i: int) -> list[list[http_types.HttpType]]:
    return [[http_types.Int(i)]]


def values(i: int) -> list:
    return [types.CypherInt(i)]


class TestAuthTokenManager(HttpTestCase, AuthorizationBase):
    required_features = (
        *HttpTestCase.required_features,
        *AuthorizationBase.required_features,
        types.Feature.AUTH_MANAGED,
    )
    backwards_compatible = False

    @contextmanager
    def session(
        self,
        driver: Driver,
        access_mode: str = "r",
        database: str = DB,
        auth_token: t.Any = None,
    ) -> t.Generator[Session]:
        session = driver.session(
            access_mode, database=database, auth_token=auth_token
        )
        try:
            yield session
        finally:
            session.close()

    _AUTH_EXPIRED = AuthorizationBase._RAW_AUTH_EXPIRED
    _TOKEN_EXPIRED = AuthorizationBase._RAW_TOKEN_EXPIRED
    _UNAUTHORIZED = AuthorizationBase._RAW_UNAUTHORIZED
    _SECURITY_EXC = AuthorizationBase._RAW_SECURITY_EXC
    _TRANSIENT_EXC = AuthorizationBase._RAW_TRANSIENT_EXC
    _RANDOM_EXC = AuthorizationBase._RAW_RANDOM_EXC

    @staticmethod
    def _make_query_endpoint(
        i: int,
        auth: types.AuthorizationToken | CustomAuthToken,
    ) -> HttpQueryEndpoint:
        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                db=DB,
                auth=auth,
                query=query(i),
            ),
            HttpQueryEndpoint.ResponseData(
                fields=FIELDS,
                records=records(i),
            ),
        )

    @staticmethod
    def _make_failing_query_endpoint(
        i: int,
        auth: types.AuthorizationToken | CustomAuthToken,
        error: dict[str, str],
        *,
        send_header: bool = False,
        send_records: bool = False,
    ) -> HttpQueryEndpoint:
        response = HttpQueryEndpoint.ResponseData(
            errors=[dict(error)],
        )
        if send_header:
            response.fields = FIELDS
        if send_records:
            response.records = records(i)

        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                db=DB,
                auth=auth,
                query=query(i),
            ),
            response,
        )

    @staticmethod
    def _make_explicit_tx_endpoint(
        i: int,
        auth: types.AuthorizationToken | CustomAuthToken,
        commit: bool = False,
        rollback: bool = False,
    ) -> HttpEndpoint:
        builder = TxEndpointBuilder(
            db=DB,
            auth=auth,
            pipeline_begin=Potential.NO,
        ).with_query(
            query(i),
            FIELDS,
            records(i),
        )
        if commit:
            builder = builder.with_commit()
        if rollback:
            builder = builder.with_rollback()
        return builder.build()

    @staticmethod
    def _make_failing_explicit_tx_endpoint(
        i: int,
        auth: types.AuthorizationToken | CustomAuthToken,
        error: dict[str, str],
        *,
        fail_tx_creation: bool = False,
        send_query_header: bool = False,
        send_query_records: bool = False,
        fail_commit: bool = False,
        fail_rollback: bool = False,
    ) -> HttpEndpoint:
        fail_close = fail_commit or fail_rollback

        errors: list[dict[str, object]] = [dict(error)]
        tx_errors = errors if fail_tx_creation else None
        query_errors = None if fail_tx_creation or fail_close else errors
        fields = FIELDS if send_query_header else None
        records_ = records(i) if send_query_records else None

        builder = TxEndpointBuilder(
            db=DB,
            auth=auth,
            pipeline_begin=Potential.NO,
            tx_errors=tx_errors,
        ).with_query(
            query(i),
            fields,
            records_,
            query_errors=query_errors,
        )

        if fail_commit:
            builder = builder.with_commit(errors=errors)
        if fail_rollback:
            builder = builder.with_rollback(errors=errors)

        return builder.build()

    def _assert_result(self, i: int, records: list[types.Record]) -> None:
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, values(i))

    def test_static_auth_manager(self) -> None:
        auth_manager = TrackingAuthTokenManager(self._backend)

        with self.server() as server:
            server.install_discovery_endpoint()
            query_endpoints = (
                self._make_query_endpoint(i, auth_manager.raw_get_auth())
                for i in range(1, 5)
            )
            server.install_endpoint(
                HttpSequenceEndpoint(*query_endpoints),
                handler_type=HandlerType.PERMANENT,
            )

            with (
                self.driver(server, auth_manager.manager) as driver,
                self.session(driver) as session,
            ):
                for i in range(1, 5):
                    records = list(session.run(query(i)))
                    self._assert_result(i, records)
                    self.assertEqual(auth_manager.get_auth_count, i)
            self.assertEqual(auth_manager.handle_security_exception_count, 0)

    def test_dynamic_auth_manager(self) -> None:
        tokens = [
            ("neo4j", "pass"),
            ("neo4j", "pass"),
            ("neo5j", "pass++"),
            ("neo5j", "pass++"),
            ("neo4j", "pass"),
            ("neo6j", "supersecret"),
            ("neo5j", "pass"),
        ]
        get_auth_count = 0
        current_auth = tokens[0]
        handle_security_exception_count = 0

        def get_auth() -> types.AuthorizationToken:
            nonlocal get_auth_count
            get_auth_count += 1
            user, password = current_auth
            return types.AuthorizationToken(
                scheme="basic", principal=user, credentials=password
            )

        def handle_security_exception(
            auth_token: types.AuthorizationToken,
            code: str,
        ) -> bool:
            nonlocal handle_security_exception_count
            handle_security_exception_count += 1
            return False

        auth_manager = AuthTokenManager(
            self._backend, get_auth, handle_security_exception
        )

        with self.server() as server:
            server.install_discovery_endpoint()
            query_endpoints = (
                self._make_query_endpoint(
                    i + 1,
                    types.AuthorizationToken(
                        scheme="basic", principal=user, credentials=password
                    ),
                )
                for i, (user, password) in enumerate(tokens)
            )
            server.install_endpoint(
                HttpSequenceEndpoint(*query_endpoints),
                handler_type=HandlerType.PERMANENT,
            )

            with (
                self.driver(server, auth_manager) as driver,
                self.session(driver) as session,
            ):
                for i, auth in enumerate(tokens):
                    current_auth = auth
                    records = list(session.run(query(i + 1)))
                    self._assert_result(i + 1, records)
                    self.assertEqual(get_auth_count, i + 1)
            self.assertEqual(get_auth_count, len(tokens))
            self.assertEqual(handle_security_exception_count, 0)

    def _get_error_assertion(
        self,
        error: Mapping[str, object],
        handled: bool,
    ) -> t.Callable[[types.DriverError], None]:

        def retryable(
            f: ErrorAssertionRetryable,
            can_retry: bool,
        ) -> t.Callable[[types.DriverError], None]:
            def inner(error: types.DriverError):
                f(error, retryable=can_retry)

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

    def _test_notify(
        self,
        server_setup: t.Callable[[HTTPServer, dict[str, str]], None],
        session_cb: t.Callable[[Session], types.DriverError],
    ) -> None:
        def _test(
            server_: HTTPServer,
            session_auth_: bool,
            should_notify_: bool,
            handled_: bool,
        ):
            if session_auth_:
                session_token = types.AuthorizationToken(
                    scheme="basic",
                    principal="neo4j-session",
                    credentials="pass",
                )
            else:
                session_token = None

            class AuthManager(TrackingAuthTokenManager):
                def _handles_security_exception(self, code: str) -> bool:
                    return handled_

            manager = AuthManager(self._backend)
            with self.driver(server_, manager.manager) as driver:
                with self.session(driver, auth_token=session_token) as session:
                    exc = session_cb(session)
                    error_assertion = self._get_error_assertion(
                        error, handled_
                    )
                    error_assertion(exc)
            if session_auth_:
                self.assertEqual(manager.get_auth_count, 0)
            else:
                self.assertEqual(manager.get_auth_count, 1)
            if should_notify_:
                self.assertEqual(manager.handle_security_exception_count, 1)
                expected_args = HandleSecurityExceptionArgs(
                    manager.raw_get_auth(), error_code
                )
                self.assertEqual(
                    manager.handle_security_exception_args,
                    [expected_args],
                )
            else:
                self.assertEqual(manager.handle_security_exception_count, 0)

        with self.server() as server:
            for session_auth, error in (
                (session_auth, error)
                for session_auth in (False, True)
                for error in (
                    self._AUTH_EXPIRED,
                    self._TOKEN_EXPIRED,
                    self._UNAUTHORIZED,
                    self._SECURITY_EXC,
                    self._TRANSIENT_EXC,
                    self._RANDOM_EXC,
                )
            ):
                error_code = error["code"]
                should_notify = (
                    error_code.startswith("Neo.ClientError.Security.")
                    and not session_auth
                )
                handles = [False]
                if should_notify:
                    handles.append(True)
                for handled in handles:
                    with (
                        self.subTest(
                            session_auth=session_auth,
                            error=error_code,
                            handled=handled,
                        ),
                        self.server_session(server),
                    ):
                        server_setup(server, error)
                        _test(server, session_auth, should_notify, handled)

    @staticmethod
    def _get_error_code(error):
        return json.loads(error)["code"]

    def _test_error_session_run(
        self,
        session_cb: t.Callable[[Session], types.DriverError],
        *,
        send_header: bool,
        send_records: bool,
    ) -> None:
        def server_setup(server: HTTPServer, error: dict[str, str]) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                self._make_failing_query_endpoint(
                    1,
                    ANY_AUTH,
                    error,
                    send_header=send_header,
                    send_records=send_records,
                )
            )

        self._test_notify(server_setup, session_cb)

    def test_error_session_run(self) -> None:
        def session_cb(session: Session) -> types.DriverError:
            with self.assertRaises(types.DriverError) as exc:
                result = session.run(query(1))
                if get_driver_name() in ["javascript"]:
                    result.next()
            return exc.exception

        self._test_error_session_run(
            session_cb,
            send_header=False,
            send_records=False,
        )

    def test_error_session_run_with_header(self) -> None:
        def session_cb(session: Session) -> types.DriverError:
            result = session.run(query(1))
            with self.assertRaises(types.DriverError) as exc:
                result.next()
            return exc.exception

        self._test_error_session_run(
            session_cb,
            send_header=True,
            send_records=False,
        )

    def test_error_session_run_with_records(self) -> None:
        def session_cb(session: Session) -> types.DriverError:
            result = session.run(query(1))
            record = result.next()
            self.assertEqual(record.values, values(1))
            with self.assertRaises(types.DriverError) as exc:
                result.next()
            return exc.exception

        self._test_error_session_run(
            session_cb,
            send_header=True,
            send_records=True,
        )

    def _test_error_explicit_tx(
        self,
        session_cb: t.Callable[[Session], types.DriverError],
        *,
        fail_tx_creation: bool,
        send_query_header: bool,
        send_query_records: bool,
        fail_commit: bool = False,
        fail_rollback: bool = False,
    ) -> None:
        def server_setup(server: HTTPServer, error: dict[str, str]) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                self._make_failing_explicit_tx_endpoint(
                    1,
                    ANY_AUTH,
                    error,
                    fail_tx_creation=fail_tx_creation,
                    send_query_header=send_query_header,
                    send_query_records=send_query_records,
                    fail_commit=fail_commit,
                    fail_rollback=fail_rollback,
                ),
                handler_type=HandlerType.PERMANENT,
            )

        self._test_notify(server_setup, session_cb)

    def test_error_explicit_tx(self) -> None:
        def session_cb(session: Session) -> types.DriverError:
            with self.assertRaises(types.DriverError) as exc:
                session.begin_transaction()
            return exc.exception

        self._test_error_explicit_tx(
            session_cb,
            fail_tx_creation=True,
            send_query_header=False,
            send_query_records=False,
        )

    def test_error_explicit_tx_no_headers(self) -> None:
        def session_cb(session: Session) -> types.DriverError:
            with (
                session.begin_transaction() as tx,
                self.assertRaises(types.DriverError) as exc,
            ):
                res = tx.run(query(1))
                if get_driver_name() in ["javascript"]:
                    res.next()
            return exc.exception

        self._test_error_explicit_tx(
            session_cb,
            fail_tx_creation=False,
            send_query_header=False,
            send_query_records=False,
        )

    def test_error_explicit_tx_with_headers(self) -> None:
        def session_cb(session: Session) -> types.DriverError:
            with session.begin_transaction() as tx:
                res = tx.run(query(1))
                with self.assertRaises(types.DriverError) as exc:
                    res.next()
            return exc.exception

        self._test_error_explicit_tx(
            session_cb,
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=False,
        )

    def test_error_explicit_tx_with_records(self) -> None:
        def session_cb(session: Session) -> types.DriverError:
            with session.begin_transaction() as tx:
                res = tx.run(query(1))
                record = res.next()
                self.assertEqual(record.values, values(1))
                with self.assertRaises(types.DriverError) as exc:
                    res.next()
            return exc.exception

        self._test_error_explicit_tx(
            session_cb,
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=True,
        )

    def test_error_explicit_tx_on_commit(self) -> None:
        def session_cb(session: Session) -> types.DriverError:
            with session.begin_transaction() as tx:
                res = tx.run(query(1))
                records = list(res)
                self._assert_result(1, records)
                with self.assertRaises(types.DriverError) as exc:
                    tx.commit()
            return exc.exception

        self._test_error_explicit_tx(
            session_cb,
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=True,
            fail_commit=True,
        )

    def test_error_explicit_tx_on_rollback(self) -> None:
        def session_cb(session: Session) -> types.DriverError:
            with session.begin_transaction() as tx:
                res = tx.run(query(1))
                records = list(res)
                self._assert_result(1, records)
                with self.assertRaises(types.DriverError) as exc:
                    tx.rollback()
            return exc.exception

        self._test_error_explicit_tx(
            session_cb,
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=True,
            fail_rollback=True,
        )
