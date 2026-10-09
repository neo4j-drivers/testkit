from __future__ import annotations

import typing as t
from collections import Counter
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
        cls.required_features += (protocol.Feature.HTTP_QUERY_API_2_0,)

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
    def server_session(cls, server: HTTPServer) -> t.Generator[None]:
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
        relative_path: str = "",
        **driver_kwargs: t.Any,
    ) -> t.Generator[Driver]:
        url = server.url_for(relative_path)
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
                self._assert_http_value_equal(
                    cypher_value,
                    records[0].values[0],
                )

    def _assert_http_value_equal(
        self,
        expected: t.Any,
        actual: t.Any,
    ) -> None:
        if isinstance(expected, types.CypherNode):
            # This ignores id comparison until there is a decision on how to
            # deal with it.
            actual_labels = Counter(
                label.value for label in actual.labels.value
            )
            expected_labels = Counter(
                label.value for label in expected.labels.value
            )
            self.assertEqual(actual_labels, expected_labels)
            self.assertEqual(actual.props, expected.props)
            self.assertEqual(actual.elementId, expected.elementId)
            return

        if isinstance(expected, types.CypherRelationship):
            # This ignores id comparison until there is a decision on how to
            # deal with it.
            self.assertEqual(actual.startNodeElementId,
                             expected.startNodeElementId)
            self.assertEqual(actual.endNodeElementId,
                             expected.endNodeElementId)
            self.assertEqual(actual.type, expected.type)
            self.assertEqual(actual.props, expected.props)
            self.assertEqual(actual.elementId, expected.elementId)
            return

        if isinstance(expected, types.CypherPath):
            self.assertIsInstance(actual, types.CypherPath)

            self.assertEqual(
                len(actual.nodes.value),
                len(expected.nodes.value),
            )
            self.assertEqual(
                len(actual.relationships.value),
                len(expected.relationships.value),
            )

            for expected_node, actual_node in zip(
                expected.nodes.value,
                actual.nodes.value,
            ):
                self._assert_http_value_equal(expected_node, actual_node)

            for expected_relationship, actual_relationship in zip(
                expected.relationships.value,
                actual.relationships.value,
            ):
                self._assert_http_value_equal(
                    expected_relationship,
                    actual_relationship,
                )

            return

        self.assertEqual(actual, expected)
