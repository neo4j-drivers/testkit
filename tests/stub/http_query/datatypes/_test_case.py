from __future__ import annotations

import typing as t

from tests.stub.http_query.shared import HttpTestCase
from tests.stub.http_query.shared.http_endpoints import HttpQueryEndpoint

if t.TYPE_CHECKING:
    from tests.stub.http_query.shared import http_types
    from tests.stub.http_query.shared.http_server import HTTPServer


class HttpDataTypeTestCase(HttpTestCase):
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
            self.driver(server, self.AUTH) as driver,
        ):
            server.install_discovery_endpoint()
            server.install_endpoint(
                HttpQueryEndpoint(
                    HttpQueryEndpoint.RequestData(
                        db="neo4j",
                        auth=self.AUTH,
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
            self.driver(server, self.AUTH) as driver,
        ):
            server.install_discovery_endpoint()
            server.install_endpoint(
                HttpQueryEndpoint(
                    HttpQueryEndpoint.RequestData(
                        db="neo4j",
                        auth=self.AUTH,
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
