from __future__ import annotations

import os
import sys
import traceback
import typing as t

from pytest_httpserver import HTTPServer as _HTTPServer
from pytest_httpserver.httpserver import HandlerType
from werkzeug import Response

from tests.stub.http_query.shared.http_endpoints import TestKitRequestMatcher

if t.TYPE_CHECKING:
    from pytest_httpserver.httpserver import RequestHandler
    from werkzeug import Request

    from tests.stub.http_query.shared.http_endpoints import HttpEndpoint


class TestKitStubHttpServer(_HTTPServer):
    _path_prefix: str = ""

    @property
    def path_prefix(self) -> str:
        return self._path_prefix

    @path_prefix.setter
    def path_prefix(self, value: str) -> None:
        if value.endswith("/"):
            value = value[:-1]
        self._path_prefix = value

    def prefix_path(self, path: str) -> str:
        if path and not path.startswith("/"):
            path = f"/{path}"
        return self.path_prefix + path

    def clear(self):
        super().clear()
        self._path_prefix = type(self)._path_prefix

    def respond_nohandler(
        self,
        request: Request,
        extra_message: str = "",
    ) -> Response:
        super().respond_nohandler(request, extra_message)

        content_type = request.headers.getlist("Accept")
        if any(
            (
                ct == "application/json"
                or ct.startswith("application/vnd.neo4j.query")
            )
            for ct in content_type
        ):
            response = Response(
                '{"error": "no handler for this request"}' + extra_message,
                self.no_handler_status_code,
                mimetype="application/json",
            )
        else:
            response = Response(
                "no handler for this request" + extra_message,
                self.no_handler_status_code,
            )
        return response

    def format_matchers(self) -> str:
        lines: list[str] = []
        lines.append("Ordered matchers:")
        lines.extend(_format_handlers(self.ordered_handlers))
        lines.append("")
        lines.append("Oneshot matchers:")
        lines.extend(_format_handlers(self.oneshot_handlers))
        lines.append("")
        lines.append("Persistent matchers:")
        lines.extend(_format_handlers(self.handlers))

        return "\n".join(lines)


def _format_handlers(handlers: list[RequestHandler]) -> list[str]:
    if handlers:
        return [
            TestKitRequestMatcher.format_matcher(handler.matcher, "  ")
            for handler in handlers
        ]
    else:
        return ["  none"]


class HTTPServer:
    _server: TestKitStubHttpServer

    def __init__(self) -> None:
        host = os.environ.get("TEST_STUB_HOST", "127.0.0.1")
        self._server = TestKitStubHttpServer(host=host)
        self._server.handlers

    def start(self) -> None:
        self._server.start()

    def stop(self) -> None:
        self._server.stop()

    def check_assertions(self) -> None:
        self._server.check_assertions()

    @property
    def path_prefix(self) -> str:
        return self._server.path_prefix

    @path_prefix.setter
    def path_prefix(self, value: str) -> None:
        self._server.path_prefix = value

    def url_for(self, suffix: str) -> str:
        return self._server.url_for(self._server.prefix_path(suffix))

    @property
    def host(self) -> str:
        return self._server.host

    @property
    def port(self) -> int:
        return self._server.port

    def install_discovery_endpoint(
        self,
        version: str = "2025.10.1",
        edition: str = "enterprise",
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        if extra_headers is None:
            extra_headers = {}

        query_url = self._server.url_for("/db/{databaseName}/query/v2")
        dbms_cluster_url = self._server.url_for("/dbms/cluster")
        db_cluster_url = self._server.url_for("/db/{databaseName}/cluster")
        tx_url = self._server.url_for("/db/{databaseName}/tx")

        request_expectation = self._server.expect_request(
            self._server.prefix_path("/"),
            method="GET",
            data="",
            headers={"Accept": "application/json", **extra_headers},
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
