from __future__ import annotations

import dataclasses
import typing as t

from nutkit import protocol as types
from tests.shared import get_dns_resolved_server_address
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
    HttpSequenceEndpoint,
    MaybeNull,
)
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    from tests.stub.http_query.shared.http_endpoints import TOptionalValue
    from tests.stub.http_query.shared.http_server import HTTPServer

    T = t.TypeVar("T")


DEFAULT_DB = "neo4j"
AUTH = HttpTestCase.AUTH
DEFAULT_QUERY_TEXT = "RETURN 1 AS n"
DEFAULT_COUNTERS = CountersMap()
DEFAULT_VERSION = "2025.10.1"


@dataclasses.dataclass
class Query:
    text: str
    params: dict[str, t.Any] = dataclasses.field(default_factory=dict)
    params_http: dict[str, http_types.HttpType] | None = None


DEFAULT_QUERY = Query(DEFAULT_QUERY_TEXT)


@dataclasses.dataclass
class QueryResult:
    keys: list[str]
    records: list[types.Record]
    summary: types.Summary


class _SummaryTestBase(HttpTestCase):
    def _get_summary_session_run(
        self,
        server: HTTPServer,
        # server_setup: t.Callable[[HTTPServer], None],
        db: str = DEFAULT_DB,
        queries: t.Iterable[Query] = (DEFAULT_QUERY,),
    ) -> tuple[types.Summary, ...]:
        results = []

        with (
            self.driver(server, AUTH) as driver,
            driver.session("w", database=db) as session,
        ):
            for query in queries:
                result = session.run(query.text, params=query.params)
                keys = result.keys()
                records = list(result)
                summary = result.consume()
                results.append(QueryResult(keys, records, summary))

        for result in results:
            self.assertEqual(result.keys, ["n"])
            self.assertEqual(len(result.records), 1)
            self.assertEqual(result.records[0].values, [types.CypherInt(1)])
            self.assertIsInstance(result.summary, types.Summary)

        return tuple(result.summary for result in results)

    def _get_summary_tx(
        self,
        server: HTTPServer,
        # server_setup: t.Callable[[HTTPServer], None],
        db: str = DEFAULT_DB,
        queries: t.Iterable[Query] = (DEFAULT_QUERY,),
    ) -> tuple[types.Summary, ...]:
        results = []

        with (
            self.driver(server, AUTH) as driver,
            driver.session("w", database=db) as session,
            session.begin_transaction() as tx,
        ):
            for query in queries:
                result = tx.run(query.text, params=query.params)
                keys = result.keys()
                records = list(result)
                summary = result.consume()
                results.append(QueryResult(keys, records, summary))
            tx.commit()

        for result in results:
            self.assertEqual(result.keys, ["n"])
            self.assertEqual(len(result.records), 1)
            self.assertEqual(result.records[0].values, [types.CypherInt(1)])
            self.assertIsInstance(result.summary, types.Summary)

        return tuple(result.summary for result in results)


class TestSummaryCounters(_SummaryTestBase):
    @staticmethod
    def _make_query_endpoint(
        counters: CountersMap | None = DEFAULT_COUNTERS,
    ) -> HttpQueryEndpoint:

        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                db=DEFAULT_DB,
                auth=AUTH,
                query=DEFAULT_QUERY_TEXT,
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
                DEFAULT_QUERY_TEXT,
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
            server.install_discovery_endpoint()
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
        with self.server() as server:
            server.install_discovery_endpoint()
            self._make_session_server_setup(counters)(server)
            summaries = super()._get_summary_session_run(server)
        assert len(summaries) == 1
        return summaries[0]

    def _get_summary_with_counters_tx(
        self, counters: CountersMap
    ) -> types.Summary:
        with self.server() as server:
            server.install_discovery_endpoint()
            self._make_tx_server_setup(counters)(server)
            summaries = super()._get_summary_tx(server)
        assert len(summaries) == 1
        return summaries[0]

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
                query=DEFAULT_QUERY_TEXT,
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
                DEFAULT_QUERY_TEXT,
                ["n"],
                [[http_types.Int(1)]],
            )
            .with_commit()
            .build()
        )

    @staticmethod
    def _make_session_server_setup(
        db: str = DEFAULT_DB,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
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
        with self.server() as server:
            server.install_discovery_endpoint()
            self._make_session_server_setup(db)(server)
            summaries = super()._get_summary_session_run(server, db=db)
        assert len(summaries) == 1
        return summaries[0]

    def _get_summary_with_database_tx(
        self, db: str = DEFAULT_DB
    ) -> types.Summary:
        with self.server() as server:
            server.install_discovery_endpoint()
            self._make_tx_server_setup(db)(server)
            summaries = super()._get_summary_tx(server, db=db)
        assert len(summaries) == 1
        return summaries[0]

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


class TestSummaryQuery(_SummaryTestBase):
    @staticmethod
    def _make_query_endpoints(
        queries: t.Collection[Query] = (DEFAULT_QUERY,),
    ) -> HttpEndpoint:
        assert len(queries) > 0
        query_endpoints = []
        for query in queries:
            parameters: TOptionalValue[dict[str, http_types.HttpType]]
            if query.params_http is None:
                parameters = MaybeNull({})
            else:
                parameters = query.params_http
            query_endpoints.append(
                HttpQueryEndpoint(
                    HttpQueryEndpoint.RequestData(
                        db=DEFAULT_DB,
                        auth=AUTH,
                        query=query.text,
                        parameters=parameters,
                    ),
                    HttpQueryEndpoint.ResponseData(
                        fields=["n"],
                        records=[[http_types.Int(1)]],
                    ),
                )
            )

        if len(query_endpoints) == 1:
            return query_endpoints[0]
        return HttpSequenceEndpoint(*query_endpoints)

    @staticmethod
    def _make_tx_endpoint(
        queries: t.Collection[Query] = (DEFAULT_QUERY,),
    ) -> HttpEndpoint:
        assert len(queries) > 0

        builder = TxEndpointBuilder(DEFAULT_DB, AUTH)

        for query in queries:
            parameters: TOptionalValue[dict[str, http_types.HttpType]]
            if query.params_http is None:
                parameters = MaybeNull({})
            else:
                parameters = query.params_http
            builder = builder.with_query(
                query.text,
                ["n"],
                [[http_types.Int(1)]],
                parameters=parameters,
            )

        return builder.with_commit().build()

    @staticmethod
    def _make_session_server_setup(
        queries: t.Collection[Query] = (DEFAULT_QUERY,),
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryQuery._make_query_endpoints(queries),
                handler_type=HandlerType.PERMANENT,
            )

        return setup

    @staticmethod
    def _make_tx_server_setup(
        queries: t.Collection[Query] = (DEFAULT_QUERY,),
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryQuery._make_tx_endpoint(queries),
                handler_type=HandlerType.PERMANENT,
            )

        return setup

    def _get_summary_with_queries_session_run(
        self,
        queries: t.Collection[Query] = (DEFAULT_QUERY,),
    ) -> tuple[types.Summary, ...]:
        with self.server() as server:
            server.install_discovery_endpoint()
            self._make_session_server_setup(queries)(server)
            return super()._get_summary_session_run(server, queries=queries)

    def _get_summary_with_queries_tx(
        self,
        queries: t.Collection[Query] = (DEFAULT_QUERY,),
    ) -> tuple[types.Summary, ...]:
        with self.server() as server:
            server.install_discovery_endpoint()
            self._make_tx_server_setup(queries)(server)
            return super()._get_summary_tx(server, queries=queries)

    def _assert_queries(
        self,
        summaries: t.Collection[types.Summary],
        expected: t.Collection[Query],
    ) -> None:
        self.assertEqual(len(summaries), len(expected))
        for summary, expected_query in zip(summaries, expected, strict=True):
            summary_query: types.SummaryQuery = summary.query
            self.assertEqual(summary_query.text, expected_query.text)
            self.assertEqual(summary_query.parameters, expected_query.params)

    def test_session_run_single(self):
        queries = (Query(" \t\n \x00%🔍 query"),)
        summary = self._get_summary_with_queries_session_run(queries)
        self._assert_queries(summary, queries)

    def test_session_run_multiple(self):
        queries = (
            Query(" \t\n \x00%🔍 query"),
            Query("RETURN 1 AS n"),
            Query("Cypher🥷"),
        )
        summary = self._get_summary_with_queries_session_run(queries)
        self._assert_queries(summary, queries)

    def test_session_run_params_single(self):
        queries = (
            Query(
                " \t\n \x00%🔍 query",
                params={"x": types.CypherInt(1)},
                params_http={"x": http_types.Int(1)},
            ),
        )
        summary = self._get_summary_with_queries_session_run(queries)
        self._assert_queries(summary, queries)

    def test_session_run_params_multiple(self):
        queries = (
            Query(
                " \t\n \x00%🔍 query",
                params={"x": types.CypherInt(1)},
                params_http={"x": http_types.Int(1)},
            ),
            Query(
                "RETURN 1 AS n",
                params={"x": types.CypherInt(2)},
                params_http={"x": http_types.Int(2)},
            ),
            Query(
                "Cypher🥷",
                params={"x": types.CypherInt(3)},
                params_http={"x": http_types.Int(3)},
            ),
        )
        summary = self._get_summary_with_queries_session_run(queries)
        self._assert_queries(summary, queries)

    def test_tx_single(self):
        queries = (Query(" \t\n \x00%🔍 query"),)
        summary = self._get_summary_with_queries_tx(queries)
        self._assert_queries(summary, queries)

    def test_tx_multiple(self):
        queries = (
            Query(" \t\n \x00%🔍 query"),
            Query("RETURN 1 AS n"),
            Query("Cypher🥷"),
        )
        summary = self._get_summary_with_queries_tx(queries)
        self._assert_queries(summary, queries)

    def test_tx_params_single(self):
        queries = (
            Query(
                " \t\n \x00%🔍 query",
                params={"x": types.CypherInt(1)},
                params_http={"x": http_types.Int(1)},
            ),
        )
        summary = self._get_summary_with_queries_tx(queries)
        self._assert_queries(summary, queries)

    def test_tx_params_multiple(self):
        queries = (
            Query(
                " \t\n \x00%🔍 query",
                params={"x": types.CypherInt(1)},
                params_http={"x": http_types.Int(1)},
            ),
            Query(
                "RETURN 1 AS n",
                params={"x": types.CypherInt(2)},
                params_http={"x": http_types.Int(2)},
            ),
            Query(
                "Cypher🥷",
                params={"x": types.CypherInt(3)},
                params_http={"x": http_types.Int(3)},
            ),
        )
        summary = self._get_summary_with_queries_tx(queries)
        self._assert_queries(summary, queries)


class Address(t.NamedTuple):
    host: str
    port: int


class TestSummaryServer(_SummaryTestBase):
    @staticmethod
    def _make_query_endpoints() -> HttpQueryEndpoint:
        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                query=DEFAULT_QUERY_TEXT,
                db=DEFAULT_DB,
                auth=AUTH,
            ),
            HttpQueryEndpoint.ResponseData(
                fields=["n"],
                records=[[http_types.Int(1)]],
            ),
        )

    @staticmethod
    def _make_tx_endpoint() -> HttpEndpoint:
        return (
            TxEndpointBuilder(DEFAULT_DB, AUTH)
            .with_query(
                DEFAULT_QUERY_TEXT,
                ["n"],
                [[http_types.Int(1)]],
            )
            .with_commit()
            .build()
        )

    @staticmethod
    def _make_session_server_setup(
        version: str = DEFAULT_VERSION,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint(version=version)
            server.install_endpoint(
                TestSummaryQuery._make_query_endpoints(),
                handler_type=HandlerType.ONESHOT,
            )

        return setup

    @staticmethod
    def _make_tx_server_setup(
        version: str = DEFAULT_VERSION,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint(version=version)
            server.install_endpoint(
                TestSummaryQuery._make_tx_endpoint(),
                handler_type=HandlerType.PERMANENT,
            )

        return setup

    def _get_summary_with_version_session_run(
        self,
        version: str = DEFAULT_VERSION,
    ) -> tuple[types.Summary, Address]:
        with self.server() as server:
            self._make_session_server_setup(version)(server)
            summaries = super()._get_summary_session_run(server)
            assert len(summaries) == 1
            return summaries[0], Address(server.host, server.port)

    def _get_summary_with_version_tx(
        self,
        version: str = DEFAULT_VERSION,
    ) -> tuple[types.Summary, Address]:
        with self.server() as server:
            self._make_tx_server_setup(version)(server)
            summaries = super()._get_summary_tx(server)
            assert len(summaries) == 1
            server._server.port
            return summaries[0], Address(server.host, server.port)

    def _assert_summary_server(
        self,
        summary: types.Summary,
        expected_version: str,
        server_address: Address,
    ) -> None:
        self.assertEqual(
            summary.server_info.agent,
            f"Neo4j/{expected_version}",
        )
        get_dns_resolved_server_address(server_address)
        self.assertIn(
            summary.server_info.address,
            [
                get_dns_resolved_server_address(server_address),
                f"{server_address.host}:{server_address.port}",
            ],
        )

    def test_session_run(self):
        summary, server_address = self._get_summary_with_version_session_run(
            version=DEFAULT_VERSION
        )
        self._assert_summary_server(summary, DEFAULT_VERSION, server_address)

    def test_session_run_custom_version(self):
        summary, server_address = self._get_summary_with_version_session_run(
            version="🐒 <3 🍌"
        )
        self._assert_summary_server(summary, "🐒 <3 🍌", server_address)

    def test_tx(self):
        summary, server_address = self._get_summary_with_version_tx(
            version=DEFAULT_VERSION
        )
        self._assert_summary_server(summary, DEFAULT_VERSION, server_address)

    def test_tx_custom_version(self):
        summary, server_address = self._get_summary_with_version_tx(
            version="🐒 <3 🍌"
        )
        self._assert_summary_server(summary, "🐒 <3 🍌", server_address)
