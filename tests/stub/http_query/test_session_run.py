from __future__ import annotations

import typing as t

from nutkit import protocol as types
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoints import (
    AnyValue,
    HttpQueryEndpoint,
    HttpSequenceEndpoint,
    MaybeNull,
)
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    T = t.TypeVar("T")

    from tests.stub.http_query.shared.http_endpoints import TOptionalValue

_ANY_VALUE = AnyValue()


def _make_query_endpoint(
    db: str,
    auth: types.AuthorizationToken,
    query: str,
    fields: list[str],
    records: list[list[http_types.HttpType]],
    impersonated_user: str | None = None,
    access_mode: TOptionalValue[str] | AnyValue = _ANY_VALUE,
    bookmarks_in: list[str] | None = None,
    bookmarks_out: list[str] | None = None,
) -> HttpQueryEndpoint:
    if bookmarks_in is None:
        bookmarks_in = []

    return HttpQueryEndpoint(
        HttpQueryEndpoint.RequestData(
            db=db,
            auth=auth,
            query=query,
            impersonated_user=impersonated_user,
            access_mode=access_mode,
            bookmarks=bookmarks_in,
        ),
        HttpQueryEndpoint.ResponseData(
            fields=fields,
            records=records,
            bookmarks=bookmarks_out,
        ),
    )


class TestSessionRun(HttpTestCase):
    def test_transaction_write(self) -> None:
        db = "dba"
        query = "RETURN 1 AS n"
        fields = ["n"]

        query_endpoint = _make_query_endpoint(
            db,
            self.AUTH,
            query,
            fields,
            [[http_types.Int(1)]],
            access_mode=MaybeNull("Write"),
        )

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.ONESHOT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session("w", database=db) as session,
            ):
                result = session.run(query)
                keys = result.keys()
                records = list(result)

        self.assertEqual(keys, fields)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def test_transaction_read(self) -> None:
        db = "dba"
        query = "RETURN 1 AS n"
        fields = ["n"]

        query_endpoint = _make_query_endpoint(
            db,
            self.AUTH,
            query,
            fields,
            [[http_types.Int(1)]],
            access_mode="Read",
        )

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.ONESHOT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session("r", database=db) as session,
            ):
                result = session.run(query)
                keys = result.keys()
                records = list(result)

        self.assertEqual(keys, fields)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def test_impersonation(self) -> None:
        db = "dba"
        impersonated_user = "banana_bob"
        query = "RETURN 1 AS n"
        fields = ["n"]

        query_endpoint = _make_query_endpoint(
            db,
            self.AUTH,
            query,
            fields,
            [[http_types.Int(1)]],
            impersonated_user=impersonated_user,
        )
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.ONESHOT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session(
                    "w", database=db, impersonated_user=impersonated_user
                ) as session,
            ):
                result = session.run(query)
                keys = result.keys()
                records = list(result)

        self.assertEqual(keys, fields)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def test_session_auth(self) -> None:
        db = "dba"
        query = "RETURN 1 AS n"
        fields = ["n"]

        query_endpoint = _make_query_endpoint(
            db, self.AUTH2, query, fields, [[http_types.Int(1)]]
        )
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.ONESHOT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session(
                    "w", database=db, auth_token=self.AUTH2
                ) as session,
            ):
                result = session.run(query)
                keys = result.keys()
                records = list(result)

        self.assertEqual(keys, fields)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def test_bookmarks(self) -> None:
        db = "dba"
        query1 = "RETURN 1 AS n"
        query2 = "RETURN 2 AS n"
        fields = ["n"]
        bookmarks1 = ["bookmark:123", "bookmark:abc"]
        bookmarks2 = ["bookmark:foo"]
        bookmarks3 = ["done"]

        queries_endpoint = HttpSequenceEndpoint(
            _make_query_endpoint(
                db,
                self.AUTH,
                query1,
                fields,
                [[http_types.Int(1)]],
                bookmarks_in=bookmarks1,
                bookmarks_out=bookmarks2,
            ),
            _make_query_endpoint(
                db,
                self.AUTH,
                query2,
                fields,
                [[http_types.Int(2)]],
                bookmarks_in=bookmarks2,
                bookmarks_out=bookmarks3,
            ),
        )
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                queries_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session(
                    "w", database=db, bookmarks=bookmarks1
                ) as session,
            ):
                result = session.run(query1)
                keys1 = result.keys()
                records1 = list(result)
                result = session.run(query2)
                keys2 = result.keys()
                records2 = list(result)
                received_bookmarks = session.last_bookmarks()

        self.assertEqual(keys1, fields)
        self.assertEqual(len(records1), 1)
        self.assertEqual(records1[0].values, [types.CypherInt(1)])
        self.assertEqual(keys2, fields)
        self.assertEqual(len(records2), 1)
        self.assertEqual(records2[0].values, [types.CypherInt(2)])
        self.assertEqual(received_bookmarks, bookmarks3)
