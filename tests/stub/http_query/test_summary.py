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
    CountersMap,
    HttpEndpoint,
    HttpQueryEndpoint,
)
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    T = t.TypeVar("T")


DB = "neo4j"
AUTH = HttpTestCase.AUTH
QUERY = "RETURN 1 AS n"
DEFAULT_COUNTERS = CountersMap()


def _make_query_endpoint(
    counters: CountersMap | None = DEFAULT_COUNTERS,
) -> HttpQueryEndpoint:

    return HttpQueryEndpoint(
        HttpQueryEndpoint.RequestData(
            db=DB,
            auth=AUTH,
            query=QUERY,
            include_counters=True,
        ),
        HttpQueryEndpoint.ResponseData(
            fields=["n"],
            records=[[http_types.Int(1)]],
            counters=counters,
        ),
    )


def _make_tx_endpoint(
    counters: CountersMap | None = DEFAULT_COUNTERS,
) -> HttpEndpoint:

    return (
        TxEndpointBuilder(DB, AUTH)
        .with_query(
            QUERY,
            ["n"],
            [[http_types.Int(1)]],
            include_counters=True,
            counters=counters,
        )
        .with_commit()
        .build()
    )


class TestSummaryCounters(HttpTestCase):
    def _assert_counters(
        self,
        summary: types.Summary,
        expected: CountersMap,
    ) -> None:
        def assert_counter(received: object, expected: object) -> None:
            self.assertIsInstance(received, type(expected))
            self.assertEqual(received, expected)

        counters = summary.counters

        assert_counter(counters.constraints_added, expected.constraints_added)
        assert_counter(
            counters.constraints_removed, expected.constraints_removed
        )
        assert_counter(counters.indexes_added, expected.indexes_added)
        assert_counter(counters.indexes_removed, expected.indexes_removed)
        assert_counter(counters.labels_added, expected.labels_added)
        assert_counter(counters.labels_removed, expected.labels_removed)
        assert_counter(counters.nodes_created, expected.nodes_created)
        assert_counter(counters.nodes_deleted, expected.nodes_deleted)
        assert_counter(counters.properties_set, expected.properties_set)
        assert_counter(
            counters.relationships_created, expected.relationships_created
        )
        assert_counter(
            counters.relationships_deleted, expected.relationships_deleted
        )
        assert_counter(counters.system_updates, expected.system_updates)
        assert_counter(counters.contains_updates, expected.contains_updates)
        assert_counter(
            counters.contains_system_updates, expected.contains_system_updates
        )

    def _get_summary_session_run(self, counters: CountersMap) -> types.Summary:
        query_endpoint = _make_query_endpoint(counters)
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.ONESHOT
            )
            with (
                self.driver(server, AUTH) as driver,
                driver.session("w", database=DB) as session,
            ):
                result = session.run(QUERY)
                keys = result.keys()
                records = list(result)
                summary = result.consume()

        self.assertEqual(keys, ["n"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])
        self.assertIsInstance(summary, types.Summary)
        return summary

    def _get_summary_tx(self, counters: CountersMap) -> types.Summary:
        tx_endpoint = _make_tx_endpoint(counters)
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                tx_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, AUTH) as driver,
                driver.session("w", database=DB) as session,
                session.begin_transaction() as tx,
            ):
                result = tx.run(QUERY)
                keys = result.keys()
                records = list(result)
                summary = result.consume()
                tx.commit()

        self.assertEqual(keys, ["n"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])
        self.assertIsInstance(summary, types.Summary)
        return summary

    def test_empty_default_session_run(self):
        counters = DEFAULT_COUNTERS
        summary = self._get_summary_session_run(counters)
        self._assert_counters(summary, counters)

    def test_empty_default_tx(self):
        counters = DEFAULT_COUNTERS
        summary = self._get_summary_tx(counters)
        self._assert_counters(summary, counters)

    def test_full_summary_session_run(self):
        counters = CountersMap(
            constraints_added=1001,
            constraints_removed=1002,
            indexes_added=1003,
            indexes_removed=1004,
            labels_added=1005,
            labels_removed=1006,
            nodes_created=1007,
            nodes_deleted=1008,
            properties_set=1009,
            relationships_created=1010,
            relationships_deleted=1011,
            system_updates=1012,
            contains_updates=True,
            contains_system_updates=True,
        )
        summary = self._get_summary_session_run(counters)
        self._assert_counters(summary, counters)

    def test_full_summary_tx(self):
        counters = CountersMap(
            constraints_added=1001,
            constraints_removed=1002,
            indexes_added=1003,
            indexes_removed=1004,
            labels_added=1005,
            labels_removed=1006,
            nodes_created=1007,
            nodes_deleted=1008,
            properties_set=1009,
            relationships_created=1010,
            relationships_deleted=1011,
            system_updates=1012,
            contains_updates=True,
            contains_system_updates=True,
        )
        summary = self._get_summary_tx(counters)
        self._assert_counters(summary, counters)
