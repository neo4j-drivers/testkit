from __future__ import annotations

import typing as t

from nutkit import protocol as types
from tests.shared import get_driver_name
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoints import (
    CustomAuthToken,
    HttpQueryEndpoint,
)
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    T = t.TypeVar("T")
    TExc = t.TypeVar("TExc", bound=Exception)


DB = "dba"
QUERY = "RETURN 1 AS n"
FIELDS = ["n"]
RECORDS: list[list[http_types.HttpType]] = [[http_types.Int(1)]]


def _make_query_endpoint(
    auth: types.AuthorizationToken | CustomAuthToken,
) -> HttpQueryEndpoint:
    return HttpQueryEndpoint(
        HttpQueryEndpoint.RequestData(
            db=DB,
            auth=auth,
            query=QUERY,
        ),
        HttpQueryEndpoint.ResponseData(
            fields=FIELDS,
            records=RECORDS,
        ),
    )


class TestAuth(HttpTestCase):
    @t.overload
    def _test_auth(
        self,
        auth: types.AuthorizationToken,
        expected_failure: None = None,
    ) -> None: ...

    @t.overload
    def _test_auth(
        self,
        auth: types.AuthorizationToken,
        expected_failure: type[TExc],
    ) -> TExc: ...

    def _test_auth(
        self,
        auth: types.AuthorizationToken,
        expected_failure: type[Exception] | None = None,
    ) -> Exception | None:
        query_auth: types.AuthorizationToken | CustomAuthToken
        if expected_failure is not None:
            query_auth = CustomAuthToken()
        else:
            query_auth = auth
        query_endpoint = _make_query_endpoint(query_auth)

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.ONESHOT
            )
            with (
                self.driver(server, auth) as driver,
                driver.session("w", database=DB) as session,
            ):
                if expected_failure is not None:
                    with self.assertRaises(expected_failure) as exc:
                        list(session.run(QUERY))
                    return exc.exception
                result = session.run(QUERY)
                keys = result.keys()
                records = list(result)

        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])
        return None

    def _assert_invalid_auth_error(self, exc: types.DriverError) -> None:
        self.assertFalse(exc.retryable)
        msg_lower = str(exc).lower()
        self.assertTrue("query api" in msg_lower or "http" in msg_lower)

        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual(
                exc.errorType, "<class 'neo4j.exceptions.ConfigurationError'>"
            )

    def test_basic_auth(self):
        auth = types.AuthorizationToken(
            "basic", principal="neo4j", credentials="pass 🔐"
        )
        self._test_auth(auth)

    def test_basic_auth_with_realm(self):
        auth = types.AuthorizationToken(
            "basic", principal="neo4j", credentials="pass", realm="myRealm"
        )

        exc = self._test_auth(auth, expected_failure=types.DriverError)

        self.assertIn("realm", exc.msg.lower())
        self._assert_invalid_auth_error(exc)

    def test_bearer_auth(self):
        auth = types.AuthorizationToken(
            "bearer", credentials="mySuperCoolSecretToken 🤫"
        )
        self._test_auth(auth)

    def test_kerberos_auth(self):
        auth = types.AuthorizationToken(
            "kerberos", credentials="corporateTicket4U! 🎟️️"
        )

        exc = self._test_auth(auth, expected_failure=types.DriverError)

        self.assertIn("kerberos", exc.msg.lower())
        self._assert_invalid_auth_error(exc)

    def test_custom(self):
        auth = types.AuthorizationToken(
            "magic_auth", principal="neo4j", credentials="pass", realm="realm"
        )

        exc = self._test_auth(auth, expected_failure=types.DriverError)

        self.assertIn("magic_auth", exc.msg.lower())
        self._assert_invalid_auth_error(exc)
