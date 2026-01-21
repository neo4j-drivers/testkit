from __future__ import annotations

import sys
import traceback
import typing as t

from pytest_httpserver import HTTPServer as _HTTPServer
from pytest_httpserver.httpserver import HandlerType

if t.TYPE_CHECKING:
    from werkzeug import (
        Request,
        Response,
    )

    from .http_endpoints import HttpEndpoint


class HTTPServer:
    _server: _HTTPServer

    def __init__(self) -> None:
        self._server = _HTTPServer()
        self._server.handlers

    def start(self) -> None:
        self._server.start()

    def stop(self) -> None:
        self._server.stop()

    def check_assertions(self) -> None:
        self._server.check_assertions()

    def url_for(self, prefix: str) -> str:
        return self._server.url_for(prefix)

    def install_discovery_endpoint(
        self,
        version: str = "2025.10.1",
        edition: str = "enterprise",
    ) -> None:
        query_url = self._server.url_for("/db/{databaseName}/query/v2")
        dbms_cluster_url = self._server.url_for("/dbms/cluster")
        db_cluster_url = self._server.url_for("/db/{databaseName}/cluster")
        tx_url = self._server.url_for("/db/{databaseName}/tx")

        request_expectation = self._server.expect_request(
            "/",
            method="GET",
            data="",
            headers={"Accept": "application/json"},
        )
        request_expectation.respond_with_json(
            {
                # "bolt_routing": "neo4j://localhost:7687",
                "query": query_url,
                "dbms/cluster": dbms_cluster_url,
                "db/cluster": db_cluster_url,
                "transaction": tx_url,
                # "bolt_direct": "bolt://localhost:7687",
                "neo4j_version": version,
                "neo4j_edition": edition,
                # "auth_config": {"oidc_providers": []},
            }
        )

    def install_endpoint(
        self,
        endpoint: HttpEndpoint,
        handler_type: HandlerType = HandlerType.ONESHOT,
    ) -> None:
        endpoint.install(self._server, handler_type=handler_type)

    def clear(self) -> None:
        self._server.clear()

    @property
    def log(self) -> list[tuple[Request, Response]]:
        return self._server.log

    def dump_log(self) -> None:
        print(f">>>> Captured http stub {self._server.url_for('/')} exchange")
        print(self._server.format_matchers(), end="\n\n")
        for req, res in self._server.log:
            print(_dump_request(req))
            print(_dump_response(res), end="\n\n")
        print(f"<<<< Captured http stub {self._server.url_for('/')} exchange")
        self._dump_assertion_errors()
        sys.stdout.flush()

    def _dump_assertion_errors(self) -> None:
        if not self._server.assertions:
            return
        print(f"\n>>>> Errors http stub {self._server.url_for('/')}")
        for assertion in self._server.assertions:
            if isinstance(assertion, Exception):
                traceback.print_exception(assertion)
            else:
                print(assertion)
            print()
        print(f"<<<< Errors http stub {self._server.url_for('/')}")


def _dump_request(req: Request) -> str:
    method = req.method
    url = req.url
    headers = req.headers
    body = req.get_data()
    return f"C: {method} {url} {headers!r} {body!r}"


def _dump_response(res: Response) -> str:
    code = res.status_code
    headers = res.headers
    body = res.get_data()
    return f"S: {code} {headers!r} {body!r}"
