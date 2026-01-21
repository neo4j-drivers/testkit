from __future__ import annotations

import typing as t
from contextlib import (
    contextmanager,
    suppress,
)

from nutkit import protocol
from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.http_query.shared.http_endpoints import HttpQueryEndpoint
from tests.stub.http_query.shared.http_server import HTTPServer

if t.TYPE_CHECKING:
    from tests.stub.http_query.shared import http_types

AUTH = types.AuthorizationToken("basic", principal="neo4j", credentials="pass")
AUTH2 = types.AuthorizationToken("basic", principal="neo5j", credentials="pw")


class HttpTestCase(TestkitTestCase):
    _servers: list[HTTPServer]
    AUTH: t.Final = AUTH
    AUTH2: t.Final = AUTH2

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.required_features += (protocol.Feature.HTTP_QUERY_API_1_0,)

    @contextmanager
    def server(self) -> t.Generator[HTTPServer]:
        server = HTTPServer()
        server.start()
        try:
            with self.server_session(server):
                yield server
        finally:
            server.stop()

    @classmethod
    @contextmanager
    def server_session(cls, server: HTTPServer):
        server.clear()
        try:
            yield
            server.check_assertions()
        except Exception:
            with suppress(Exception):
                server.dump_log()
            raise

    @contextmanager
    def driver(
        self,
        server: HTTPServer,
        auth: t.Any,
        **driver_kwargs: t.Any,
    ) -> t.Generator[Driver]:
        url = server.url_for("")
        driver = Driver(self._backend, url, auth, **driver_kwargs)
        try:
            yield driver
        finally:
            driver.close()

    def _echo_session_run(
        self,
        server: HTTPServer,
        cypher_value: t.Any,
        http_value: http_types.HttpType,
        *,
        http_value_out: http_types.HttpType | None = None,
        cypher_value_out: t.Any = None,
    ) -> None:
        if http_value_out is None:
            http_value_out = http_value
        if cypher_value_out is None:
            cypher_value_out = cypher_value
        with (
            self.server_session(server),
            self.driver(server, AUTH) as driver,
        ):
            server.install_discovery_endpoint()
            server.install_endpoint(
                HttpQueryEndpoint(
                    HttpQueryEndpoint.RequestData(
                        db="neo4j",
                        auth=AUTH,
                        query="RETURN $x AS x",
                        parameters={"x": http_value},
                    ),
                    HttpQueryEndpoint.ResponseData(
                        fields=["x"],
                        records=[[http_value_out]],
                    ),
                )
            )
            with driver.session("r", database="neo4j") as session:
                result = session.run(
                    "RETURN $x AS x",
                    params={"x": cypher_value},
                )
                self.assertEqual(result.keys(), ["x"])
                records = list(result)
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0].values, [cypher_value_out])

    def _receive_session_run(
        self,
        server: HTTPServer,
        cypher_value: t.Any,
        http_value: http_types.HttpType,
    ) -> None:
        with (
            self.server_session(server),
            self.driver(server, AUTH) as driver,
        ):
            server.install_discovery_endpoint()
            server.install_endpoint(
                HttpQueryEndpoint(
                    HttpQueryEndpoint.RequestData(
                        db="neo4j",
                        auth=AUTH,
                        query="RETURN x",
                    ),
                    HttpQueryEndpoint.ResponseData(
                        fields=["x"],
                        records=[[http_value]],
                    ),
                )
            )
            with driver.session("r", database="neo4j") as session:
                result = session.run("RETURN x")
                self.assertEqual(result.keys(), ["x"])
                records = list(result)
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0].values, [cypher_value])
