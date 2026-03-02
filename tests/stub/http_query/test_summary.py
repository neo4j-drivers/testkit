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
    from tests.stub.http_query.shared.http_server import HTTPServer

    T = t.TypeVar("T")


DEFAULT_DB = "neo4j"
AUTH = HttpTestCase.AUTH
QUERY = "RETURN 1 AS n"
DEFAULT_COUNTERS = CountersMap()


class _SummaryTestBase(HttpTestCase):
    def _get_summary_session_run(
        self,
        server_setup: t.Callable[[HTTPServer], None],
        db: str = DEFAULT_DB,
    ) -> types.Summary:
        with self.server() as server:
            server.install_discovery_endpoint()
            server_setup(server)
            with (
                self.driver(server, AUTH) as driver,
                driver.session("w", database=db) as session,
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

    def _get_summary_tx(
        self,
        server_setup: t.Callable[[HTTPServer], None],
        db: str = DEFAULT_DB,
    ) -> types.Summary:
        with self.server() as server:
            server.install_discovery_endpoint()
            server_setup(server)
            with (
                self.driver(server, AUTH) as driver,
                driver.session("w", database=db) as session,
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


class TestSummaryCounters(_SummaryTestBase):
    @staticmethod
    def _make_query_endpoint(
        counters: CountersMap | None = DEFAULT_COUNTERS,
    ) -> HttpQueryEndpoint:

        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                db=DEFAULT_DB,
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

    @staticmethod
    def _make_tx_endpoint(
        counters: CountersMap | None = DEFAULT_COUNTERS,
    ) -> HttpEndpoint:

        return (
            TxEndpointBuilder(DEFAULT_DB, AUTH)
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

    @staticmethod
    def _make_session_server_setup(
        counters: CountersMap,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_endpoint(
                TestSummaryCounters._make_query_endpoint(counters),
                handler_type=HandlerType.ONESHOT,
            )

        return setup

    @staticmethod
    def _make_tx_server_setup(
        counters: CountersMap,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryCounters._make_tx_endpoint(counters),
                handler_type=HandlerType.PERMANENT,
            )

        return setup

    def _get_summary_with_counters_session_run(
        self, counters: CountersMap
    ) -> types.Summary:
        return super()._get_summary_session_run(
            self._make_session_server_setup(counters)
        )

    def _get_summary_with_counters_tx(
        self, counters: CountersMap
    ) -> types.Summary:
        return super()._get_summary_tx(self._make_tx_server_setup(counters))

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

    def test_empty_default_session_run(self):
        counters = DEFAULT_COUNTERS
        summary = self._get_summary_with_counters_session_run(counters)
        self._assert_counters(summary, counters)

    def test_empty_default_tx(self):
        counters = DEFAULT_COUNTERS
        summary = self._get_summary_with_counters_tx(counters)
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
        summary = self._get_summary_with_counters_session_run(counters)
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
        summary = self._get_summary_with_counters_tx(counters)
        self._assert_counters(summary, counters)


class TestSummaryDatabase(_SummaryTestBase):
    @staticmethod
    def _make_query_endpoint(
        db: str = DEFAULT_DB,
    ) -> HttpQueryEndpoint:

        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                db=db,
                auth=AUTH,
                query=QUERY,
                include_counters=True,
            ),
            HttpQueryEndpoint.ResponseData(
                fields=["n"],
                records=[[http_types.Int(1)]],
            ),
        )

    @staticmethod
    def _make_tx_endpoint(
        db: str = DEFAULT_DB,
    ) -> HttpEndpoint:

        return (
            TxEndpointBuilder(db, AUTH)
            .with_query(
                QUERY,
                ["n"],
                [[http_types.Int(1)]],
                include_counters=True,
            )
            .with_commit()
            .build()
        )

    @staticmethod
    def _make_session_server_setup(
        db: str = DEFAULT_DB,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_endpoint(
                TestSummaryDatabase._make_query_endpoint(db),
                handler_type=HandlerType.ONESHOT,
            )

        return setup

    @staticmethod
    def _make_tx_server_setup(
        db: str = DEFAULT_DB,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryDatabase._make_tx_endpoint(db),
                handler_type=HandlerType.PERMANENT,
            )

        return setup

    def _get_summary_with_database_session_run(
        self, db: str = DEFAULT_DB
    ) -> types.Summary:
        return super()._get_summary_session_run(
            self._make_session_server_setup(db),
            db=db,
        )

    def _get_summary_with_database_tx(
        self, db: str = DEFAULT_DB
    ) -> types.Summary:
        return super()._get_summary_tx(
            self._make_tx_server_setup(db),
            db=db,
        )

    def _assert_database(
        self,
        summary: types.Summary,
        expected: str,
    ) -> None:
        self.assertEqual(summary.database, expected)

    def test_session_run(self):
        db = "🦹🏼‍♀️ \t\n\x00%-db"
        summary = self._get_summary_with_database_session_run(db)
        self._assert_database(summary, db)

    def test_tx(self):
        db = "🦹🏼‍♀️ \t\n\x00%-db"
        summary = self._get_summary_with_database_tx(db)
        self._assert_database(summary, db)
