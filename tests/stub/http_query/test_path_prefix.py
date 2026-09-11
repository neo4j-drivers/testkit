from __future__ import annotations

import typing as t

from nutkit import protocol as types
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoint_builders import (
    TxEndpointBuilder,
)
from tests.stub.http_query.shared.http_endpoints import (
    HttpEndpoint,
    HttpQueryEndpoint,
)
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    from tests.stub.http_query.shared.http_server import HTTPServer

    T = t.TypeVar("T")
    TExc = t.TypeVar("TExc", bound=Exception)


DB = "dba"
QUERY = "RETURN 1 AS n"
FIELDS = ["n"]
RECORDS: list[list[http_types.HttpType]] = [[http_types.Int(1)]]
AUTH = HttpTestCase.AUTH
SERVER_VERSION = "2025.13.99999"


def _make_query_endpoint() -> HttpQueryEndpoint:
    return HttpQueryEndpoint(
        HttpQueryEndpoint.RequestData(
            db=DB,
            auth=AUTH,
            query=QUERY,
        ),
        HttpQueryEndpoint.ResponseData(
            fields=FIELDS,
            records=RECORDS,
        ),
    )


def _make_tx_endpoint() -> HttpEndpoint:
    return (
        TxEndpointBuilder(
            db=DB,
            auth=AUTH,
        )
        .with_query(QUERY, FIELDS, RECORDS)
        .with_commit()
        .build()
    )


class TestPathPrefix(HttpTestCase):
    def _test_session_run(self, server: HTTPServer, path: str) -> None:
        query_endpoint = _make_query_endpoint()

        server.install_discovery_endpoint(version=SERVER_VERSION)
        server.install_endpoint(
            query_endpoint, handler_type=HandlerType.ONESHOT
        )
        with (
            self.driver(server, AUTH, relative_path=path) as driver,
            driver.session("w", database=DB) as session,
        ):
            result = session.run(QUERY)
            keys = result.keys()
            records = list(result)
            summary = result.consume()

        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])
        self.assertEqual(summary.server_info.agent, f"Neo4j/{SERVER_VERSION}")

    def test_session_run(self) -> None:
        with self.server() as server:
            for prefix in ("", "/foo", "/foo/", "foo/bar/baz/../oh_boy"):
                with (
                    self.subTest(prefix=prefix),
                    self.server_session(server),
                ):
                    self._test_session_run(server, prefix)

    def _test_explicit_tx(self, server: HTTPServer, path: str) -> None:
        query_endpoint = _make_tx_endpoint()

        server.install_discovery_endpoint(version=SERVER_VERSION)
        server.install_endpoint(
            query_endpoint, handler_type=HandlerType.PERMANENT
        )
        with (
            self.driver(server, AUTH, relative_path=path) as driver,
            driver.session("w", database=DB) as session,
            session.begin_transaction() as tx,
        ):
            result = tx.run(QUERY)
            keys = result.keys()
            records = list(result)
            summary = result.consume()
            tx.commit()

        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])
        self.assertEqual(summary.server_info.agent, f"Neo4j/{SERVER_VERSION}")

    def test_explicit_tx(self) -> None:
        with self.server() as server:
            for prefix in ("", "/foo", "/foo/", "foo/bar/baz/../oh_boy"):
                with (
                    self.subTest(prefix=prefix),
                    self.server_session(server),
                ):
                    self._test_explicit_tx(server, prefix)

    def _test_execute_query(self, server: HTTPServer, path: str) -> None:
        query_endpoint = _make_tx_endpoint()

        server.install_discovery_endpoint(version=SERVER_VERSION)
        server.install_endpoint(
            query_endpoint, handler_type=HandlerType.PERMANENT
        )
        with self.driver(server, AUTH, relative_path=path) as driver:
            result = driver.execute_query(QUERY, database=DB)
            keys = result.keys
            records = result.records
            summary = result.summary

        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])
        self.assertEqual(summary.server_info.agent, f"Neo4j/{SERVER_VERSION}")

    def test_execute_query(self) -> None:
        with self.server() as server:
            for prefix in ("", "/foo", "/foo/", "foo/bar/baz/../oh_boy"):
                with (
                    self.subTest(prefix=prefix),
                    self.server_session(server),
                ):
                    self._test_execute_query(server, prefix)
