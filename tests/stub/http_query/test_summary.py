from __future__ import annotations

import dataclasses
import typing as t

from nutkit import protocol as types
from tests.shared import (
    get_dns_resolved_server_address,
    MAX_INT64,
)
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
    Notification,
    Plan,
    Position,
    Profile,
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

    def test_empty_default_session_run(self) -> None:
        counters = DEFAULT_COUNTERS
        summary = self._get_summary_with_counters_session_run(counters)
        self._assert_counters(summary, counters)

    def test_empty_default_tx(self) -> None:
        counters = DEFAULT_COUNTERS
        summary = self._get_summary_with_counters_tx(counters)
        self._assert_counters(summary, counters)

    def test_full_summary_session_run(self) -> None:
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

    def test_full_summary_tx(self) -> None:
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
        db = "myDb123"
        summary = self._get_summary_with_database_session_run(db)
        self._assert_database(summary, db)

    def test_tx(self):
        db = "myDb123"
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

    def test_session_run_single(self) -> None:
        queries = (Query(" \t\n \x00%🔍 query"),)
        summary = self._get_summary_with_queries_session_run(queries)
        self._assert_queries(summary, queries)

    def test_session_run_multiple(self) -> None:
        queries = (
            Query(" \t\n \x00%🔍 query"),
            Query("RETURN 1 AS n"),
            Query("Cypher🥷"),
        )
        summary = self._get_summary_with_queries_session_run(queries)
        self._assert_queries(summary, queries)

    def test_session_run_params_single(self) -> None:
        queries = (
            Query(
                " \t\n \x00%🔍 query",
                params={"x": types.CypherInt(1)},
                params_http={"x": http_types.Int(1)},
            ),
        )
        summary = self._get_summary_with_queries_session_run(queries)
        self._assert_queries(summary, queries)

    def test_session_run_params_multiple(self) -> None:
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

    def test_tx_single(self) -> None:
        queries = (Query(" \t\n \x00%🔍 query"),)
        summary = self._get_summary_with_queries_tx(queries)
        self._assert_queries(summary, queries)

    def test_tx_multiple(self) -> None:
        queries = (
            Query(" \t\n \x00%🔍 query"),
            Query("RETURN 1 AS n"),
            Query("Cypher🥷"),
        )
        summary = self._get_summary_with_queries_tx(queries)
        self._assert_queries(summary, queries)

    def test_tx_params_single(self) -> None:
        queries = (
            Query(
                " \t\n \x00%🔍 query",
                params={"x": types.CypherInt(1)},
                params_http={"x": http_types.Int(1)},
            ),
        )
        summary = self._get_summary_with_queries_tx(queries)
        self._assert_queries(summary, queries)

    def test_tx_params_multiple(self) -> None:
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

    def test_session_run(self) -> None:
        summary, server_address = self._get_summary_with_version_session_run(
            version=DEFAULT_VERSION
        )
        self._assert_summary_server(summary, DEFAULT_VERSION, server_address)

    def test_session_run_custom_version(self) -> None:
        summary, server_address = self._get_summary_with_version_session_run(
            version="🐒 <3 🍌"
        )
        self._assert_summary_server(summary, "🐒 <3 🍌", server_address)

    def test_tx(self) -> None:
        summary, server_address = self._get_summary_with_version_tx(
            version=DEFAULT_VERSION
        )
        self._assert_summary_server(summary, DEFAULT_VERSION, server_address)

    def test_tx_custom_version(self) -> None:
        summary, server_address = self._get_summary_with_version_tx(
            version="🐒 <3 🍌"
        )
        self._assert_summary_server(summary, "🐒 <3 🍌", server_address)


class TestSummaryTimers(_SummaryTestBase):
    @staticmethod
    def _make_query_endpoints(
        result_available_after: int | None,
        result_consumed_after: int | None,
    ) -> HttpQueryEndpoint:
        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                query=DEFAULT_QUERY_TEXT,
                db=DEFAULT_DB,
                auth=AUTH,
            ),
            HttpQueryEndpoint.ResponseData(
                fields=["n"],
                records=[[http_types.Int(1)]],
                result_available_after=result_available_after,
                result_consumed_after=result_consumed_after,
            ),
        )

    @staticmethod
    def _make_tx_endpoint(
        result_available_after: int | None,
        result_consumed_after: int | None,
    ) -> HttpEndpoint:
        return (
            TxEndpointBuilder(DEFAULT_DB, AUTH)
            .with_query(
                DEFAULT_QUERY_TEXT,
                ["n"],
                [[http_types.Int(1)]],
                result_available_after=result_available_after,
                result_consumed_after=result_consumed_after,
            )
            .with_commit()
            .build()
        )

    def _assert_expected_timers_in_summary(
        self,
        summary: types.Summary,
        result_available_after: int | None,
        result_consumed_after: int | None,
    ) -> None:
        actual_available_after = summary.result_available_after
        actual_consumed_after = summary.result_consumed_after

        if result_available_after is None:
            self.assertIn(actual_available_after, (-1, 0, None))
        else:
            self.assertEqual(actual_available_after, result_available_after)

        if result_consumed_after is None:
            self.assertIn(actual_consumed_after, (-1, 0, None))
        else:
            self.assertEqual(actual_consumed_after, result_consumed_after)

    @staticmethod
    def _make_session_server_setup(
        result_available_after: int | None,
        result_consumed_after: int | None,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryTimers._make_query_endpoints(
                    result_available_after,
                    result_consumed_after,
                ),
                handler_type=HandlerType.ONESHOT,
            )

        return setup

    @staticmethod
    def _make_tx_server_setup(
        result_available_after: int | None,
        result_consumed_after: int | None,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryTimers._make_tx_endpoint(
                    result_available_after,
                    result_consumed_after,
                ),
                handler_type=HandlerType.PERMANENT,
            )

        return setup

    def _get_summary_with_timers_session_run(
        self,
        result_available_after: int | None,
        result_consumed_after: int | None,
    ) -> types.Summary:
        with self.server() as server:
            self._make_session_server_setup(
                result_available_after, result_consumed_after
            )(server)
            summaries = super()._get_summary_session_run(server)
            assert len(summaries) == 1
            return summaries[0]

    def _get_summary_with_timers_tx(
        self,
        result_available_after: int | None,
        result_consumed_after: int | None,
    ) -> types.Summary:
        with self.server() as server:
            self._make_tx_server_setup(
                result_available_after,
                result_consumed_after,
            )(server)
            summaries = super()._get_summary_tx(server)
            assert len(summaries) == 1
            server._server.port
            return summaries[0]

    def test_fallback_timers(self):
        summary = self._get_summary_with_timers_session_run(None, None)
        self._assert_expected_timers_in_summary(summary, None, None)

    def test_fallback_timers_tx(self):
        summary = self._get_summary_with_timers_tx(None, None)
        self._assert_expected_timers_in_summary(summary, None, None)

    def test_timers(self):
        for available, consumed in (
            (0, 0),
            (1, 1),
            (1337, 1337),
            # max i64 nanoseconds, while the server *could* send up to i64,
            # it's unrealistic to ever encounter a query that slower than
            # what's tested here (which is roughly 300 years).
            (MAX_INT64 // 1_000_000, MAX_INT64 // 1_000_000),
        ):
            with self.subTest(
                avaiable_after=available, consumed_after=consumed
            ):
                summary = self._get_summary_with_timers_session_run(
                    available, consumed
                )
                self._assert_expected_timers_in_summary(
                    summary, available, consumed
                )

    def test_timers_tx(self):
        for available, consumed in (
            (0, 0),
            (1, 1),
            (1337, 1337),
            # max i64 nanoseconds, while the server *could* send up to i64,
            # it's unrealistic to ever encounter a query that slower than
            # what's tested here (which is roughly 300 years).
            (MAX_INT64 // 1_000_000, MAX_INT64 // 1_000_000),
        ):
            with self.subTest(
                avaiable_after=available, consumed_after=consumed
            ):
                summary = self._get_summary_with_timers_tx(available, consumed)
                self._assert_expected_timers_in_summary(
                    summary, available, consumed
                )


class TestSummaryPlan(_SummaryTestBase):
    @staticmethod
    def _make_query_endpoints(plan: Plan) -> HttpQueryEndpoint:
        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                query=DEFAULT_QUERY_TEXT,
                db=DEFAULT_DB,
                auth=AUTH,
            ),
            HttpQueryEndpoint.ResponseData(
                fields=["n"],
                records=[[http_types.Int(1)]],
                plan=plan,
            ),
        )

    @staticmethod
    def _make_tx_endpoint(plan: Plan) -> HttpEndpoint:
        return (
            TxEndpointBuilder(DEFAULT_DB, AUTH)
            .with_query(
                DEFAULT_QUERY_TEXT,
                ["n"],
                [[http_types.Int(1)]],
                plan=plan,
            )
            .with_commit()
            .build()
        )

    @staticmethod
    def _make_session_server_setup(
        plan: Plan,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryPlan._make_query_endpoints(plan),
                handler_type=HandlerType.ONESHOT,
            )

        return setup

    @staticmethod
    def _make_tx_server_setup(
        plan: Plan,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryPlan._make_tx_endpoint(plan),
                handler_type=HandlerType.PERMANENT,
            )

        return setup

    def _get_summary_with_plan_session_run(
        self,
        plan: Plan,
    ) -> types.Summary:
        with self.server() as server:
            self._make_session_server_setup(plan)(server)
            summaries = super()._get_summary_session_run(server)
            assert len(summaries) == 1
            return summaries[0]

    def _get_summary_with_plan_tx(
        self,
        plan: Plan,
    ) -> types.Summary:
        with self.server() as server:
            self._make_tx_server_setup(plan)(server)
            summaries = super()._get_summary_tx(server)
            assert len(summaries) == 1
            server._server.port
            return summaries[0]

    def _test_plan_1(
        self,
        get_summary: t.Callable[[Plan], types.Summary],
    ) -> None:
        summary = get_summary(
            Plan(
                arguments={
                    "planner-impl": http_types.Str("IDP"),
                    "Details": http_types.Str("n"),
                    "PipelineInfo": http_types.Str("Fused in Pipeline 0"),
                    "planner-version": http_types.Str("4.3"),
                    "runtime-version": http_types.Str("4.3"),
                    "runtime": http_types.Str("PIPELINED"),
                    "runtime-impl": http_types.Str("PIPELINED"),
                    "version": http_types.Str("CYPHER 4.3"),
                    "EstimatedRows": http_types.Float(1.5),
                    "planner": http_types.Str("COST"),
                },
                operator_type="ProduceResults@neo4j",
                identifiers=["n"],
                children=[
                    Plan(
                        arguments={
                            "Details": http_types.Str("(n)"),
                            "EstimatedRows": http_types.Float(1.5),
                            "PipelineInfo": http_types.Str(
                                "Fused in Pipeline 0"
                            ),
                        },
                        operator_type="Create@neo4j",
                        children=[],
                        identifiers=["n"],
                    ),
                ],
            )
        )
        plan = summary.plan
        self.assertEqual(
            plan,
            {
                "args": {
                    "planner-impl": "IDP",
                    "Details": "n",
                    "PipelineInfo": "Fused in Pipeline 0",
                    "planner-version": "4.3",
                    "runtime-version": "4.3",
                    "runtime": "PIPELINED",
                    "runtime-impl": "PIPELINED",
                    "version": "CYPHER 4.3",
                    "EstimatedRows": 1.5,
                    "planner": "COST",
                },
                "operatorType": "ProduceResults@neo4j",
                "identifiers": ["n"],
                "children": [
                    {
                        "args": {
                            "Details": "(n)",
                            "EstimatedRows": 1.5,
                            "PipelineInfo": "Fused in Pipeline 0",
                        },
                        "operatorType": "Create@neo4j",
                        "identifiers": ["n"],
                    },
                ],
            },
        )

    def test_session_plan_1(self) -> None:
        self._test_plan_1(self._get_summary_with_plan_session_run)

    def test_tx_plan_1(self) -> None:
        self._test_plan_1(self._get_summary_with_plan_tx)


class TestSummaryProfile(_SummaryTestBase):
    @staticmethod
    def _make_query_endpoints(profile: Profile) -> HttpQueryEndpoint:
        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                query=DEFAULT_QUERY_TEXT,
                db=DEFAULT_DB,
                auth=AUTH,
            ),
            HttpQueryEndpoint.ResponseData(
                fields=["n"],
                records=[[http_types.Int(1)]],
                profile=profile,
            ),
        )

    @staticmethod
    def _make_tx_endpoint(profile: Profile) -> HttpEndpoint:
        return (
            TxEndpointBuilder(DEFAULT_DB, AUTH)
            .with_query(
                DEFAULT_QUERY_TEXT,
                ["n"],
                [[http_types.Int(1)]],
                profile=profile,
            )
            .with_commit()
            .build()
        )

    @staticmethod
    def _make_session_server_setup(
        profile: Profile,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryProfile._make_query_endpoints(profile),
                handler_type=HandlerType.ONESHOT,
            )

        return setup

    @staticmethod
    def _make_tx_server_setup(
        profile: Profile,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryProfile._make_tx_endpoint(profile),
                handler_type=HandlerType.PERMANENT,
            )

        return setup

    def _get_summary_with_profile_session_run(
        self,
        profile: Profile,
    ) -> types.Summary:
        with self.server() as server:
            self._make_session_server_setup(profile)(server)
            summaries = super()._get_summary_session_run(server)
            assert len(summaries) == 1
            return summaries[0]

    def _get_summary_with_profile_tx(
        self,
        profile: Profile,
    ) -> types.Summary:
        with self.server() as server:
            self._make_tx_server_setup(profile)(server)
            summaries = super()._get_summary_tx(server)
            assert len(summaries) == 1
            server._server.port
            return summaries[0]

    def _test_profile_1(
        self,
        get_summary: t.Callable[[Profile], types.Summary],
    ) -> None:
        summary = get_summary(
            Profile(
                db_hits=1,
                records=1,
                has_page_cache_stats=False,
                page_cache_hits=0,
                page_cache_misses=0,
                page_cache_hit_ratio=0.0,
                time=0,
                identifiers=["n"],
                operator_type="ProduceResults@neo4j",
                arguments={
                    "GlobalMemory": http_types.Int(136),
                    "planner-impl": http_types.Str("IDP"),
                    "runtime": http_types.Str("PIPELINED"),
                    "runtime-impl": http_types.Str("PIPELINED"),
                    "version": http_types.Str("CYPHER 4.3"),
                    "DbHits": http_types.Int(1),
                    "Details": http_types.Str("n"),
                    "PipelineInfo": http_types.Str("Fused in Pipeline 0"),
                    "planner-version": http_types.Str("4.3"),
                    "runtime-version": http_types.Str("4.3"),
                    "EstimatedRows": http_types.Float(1.1),
                    "planner": http_types.Str("COST"),
                    "Rows": http_types.Int(1),
                },
                children=[
                    Profile(
                        db_hits=1,
                        records=1,
                        has_page_cache_stats=True,
                        page_cache_hits=0,
                        page_cache_misses=1,
                        page_cache_hit_ratio=0.1,
                        time=0,
                        identifiers=["n"],
                        operator_type="Create@neo4j",
                        arguments={
                            "Details": http_types.Str("(n)"),
                            "PipelineInfo": http_types.Str(
                                "Fused in Pipeline 0"
                            ),
                            "Time": http_types.Int(0),
                            "PageCacheMisses": http_types.Int(0),
                            "EstimatedRows": http_types.Float(1.1),
                            "DbHits": http_types.Int(1),
                            "Rows": http_types.Int(1),
                            "PageCacheHits": http_types.Int(0),
                        },
                        children=[],
                    ),
                ],
            )
        )
        profile = summary.profile
        self.assertEqual(
            profile,
            {
                "dbHits": 1,
                "rows": 1,
                "time": 0,
                "identifiers": ["n"],
                "operatorType": "ProduceResults@neo4j",
                "args": {
                    "GlobalMemory": 136,
                    "planner-impl": "IDP",
                    "runtime": "PIPELINED",
                    "runtime-impl": "PIPELINED",
                    "version": "CYPHER 4.3",
                    "DbHits": 1,
                    "Details": "n",
                    "PipelineInfo": "Fused in Pipeline 0",
                    "planner-version": "4.3",
                    "runtime-version": "4.3",
                    "EstimatedRows": 1.1,
                    "planner": "COST",
                    "Rows": 1,
                },
                "children": [
                    {
                        "dbHits": 1,
                        "rows": 1,
                        "pageCacheHits": 0,
                        "pageCacheMisses": 1,
                        "pageCacheHitRatio": 0.1,
                        "time": 0,
                        "identifiers": ["n"],
                        "operatorType": "Create@neo4j",
                        "args": {
                            "Details": "(n)",
                            "PipelineInfo": "Fused in Pipeline 0",
                            "Time": 0,
                            "PageCacheMisses": 0,
                            "EstimatedRows": 1.1,
                            "DbHits": 1,
                            "Rows": 1,
                            "PageCacheHits": 0,
                        },
                    },
                ],
            },
        )

    def test_session_profile_1(self) -> None:
        self._test_profile_1(self._get_summary_with_profile_session_run)

    def test_tx_profile_1(self) -> None:
        self._test_profile_1(self._get_summary_with_profile_tx)


class TestSummaryNotifications(_SummaryTestBase):
    @staticmethod
    def _make_query_endpoints(
        notifications: list[Notification] | None,
    ) -> HttpQueryEndpoint:
        return HttpQueryEndpoint(
            HttpQueryEndpoint.RequestData(
                query=DEFAULT_QUERY_TEXT,
                db=DEFAULT_DB,
                auth=AUTH,
            ),
            HttpQueryEndpoint.ResponseData(
                fields=["n"],
                records=[[http_types.Int(1)]],
                notifications=notifications,
            ),
        )

    @staticmethod
    def _make_tx_endpoint(
        notifications: list[Notification] | None,
    ) -> HttpEndpoint:
        return (
            TxEndpointBuilder(DEFAULT_DB, AUTH)
            .with_query(
                DEFAULT_QUERY_TEXT,
                ["n"],
                [[http_types.Int(1)]],
                notifications=notifications,
            )
            .with_commit()
            .build()
        )

    @staticmethod
    def _make_session_server_setup(
        notifications: list[Notification] | None,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryNotifications._make_query_endpoints(notifications),
                handler_type=HandlerType.ONESHOT,
            )

        return setup

    @staticmethod
    def _make_tx_server_setup(
        notifications: list[Notification] | None,
    ) -> t.Callable[[HTTPServer], None]:
        def setup(server: HTTPServer) -> None:
            server.install_discovery_endpoint()
            server.install_endpoint(
                TestSummaryNotifications._make_tx_endpoint(notifications),
                handler_type=HandlerType.PERMANENT,
            )

        return setup

    def _get_summary_with_notifications_session_run(
        self,
        notifications: list[Notification] | None,
    ) -> types.Summary:
        with self.server() as server:
            self._make_session_server_setup(notifications)(server)
            summaries = super()._get_summary_session_run(server)
            assert len(summaries) == 1
            return summaries[0]

    def _get_summary_with_notifications_tx(
        self,
        notifications: list[Notification] | None,
    ) -> types.Summary:
        with self.server() as server:
            self._make_tx_server_setup(notifications)(server)
            summaries = super()._get_summary_tx(server)
            assert len(summaries) == 1
            server._server.port
            return summaries[0]

    @staticmethod
    def _parsed_severity(severity_level: str) -> str:
        severity_mapping = {
            "INFORMATION": "INFORMATION",
            "WARNING": "WARNING",
        }
        return severity_mapping.get(severity_level, "UNKNOWN")

    @staticmethod
    def _parsed_category(category: str) -> str:
        category_mapping = {
            "HINT": "HINT",
            "UNRECOGNIZED": "UNRECOGNIZED",
            "UNSUPPORTED": "UNSUPPORTED",
            "PERFORMANCE": "PERFORMANCE",
            "DEPRECATION": "DEPRECATION",
            "GENERIC": "GENERIC",
            "SECURITY": "SECURITY",
            "TOPOLOGY": "TOPOLOGY",
            "SCHEMA": "SCHEMA",
        }
        return category_mapping.get(category, "UNKNOWN")

    def _assert_notification(
        self,
        summary: types.Summary,
        expected: Notification,
        position: int,
    ) -> None:
        notification = summary.notifications[position]
        self.assertEqual(
            notification.get("title"),
            expected.title or "",
        )
        self.assertEqual(
            notification.get("code"),
            expected.code or "",
        )
        self.assertEqual(
            notification.get("description"),
            expected.description or "",
        )
        self.assertEqual(
            notification.get("severityLevel"),
            self._parsed_severity(expected.severity or "UNKNOWN"),
        )
        self.assertEqual(
            notification.get("category"),
            self._parsed_category(expected.category or "UNKNOWN"),
        )
        self.assertEqual(
            notification.get("rawSeverityLevel"),
            expected.severity or "",
        )
        self.assertEqual(
            notification.get("rawCategory"),
            expected.category or "",
        )
        expected_keys = {
            "title",
            "code",
            "description",
            "severityLevel",
            "category",
            "rawSeverityLevel",
            "rawCategory",
        }
        if expected.position is not None:
            self.assertIn("position", notification)
            self.assertEqual(
                notification["position"],
                {
                    "offset": expected.position.offset,
                    "line": expected.position.line,
                    "column": expected.position.column,
                },
            )
            expected_keys.add("position")
        self.assertEqual(set(notification.keys()), expected_keys)

    def _assert_notification_as_gql_status(
        self,
        summary: types.Summary,
        expected: Notification,
        position: int,
    ) -> None:
        expected_diagnostic_record: dict[str, t.Any] = {
            "OPERATION": types.CypherString(""),
            "OPERATION_CODE": types.CypherString("0"),
            "CURRENT_SCHEMA": types.CypherString("/"),
        }
        is_warning = expected.severity == "WARNING"

        status = summary.gql_status_objects[position]
        self.assertTrue(status.is_notification)
        expected_status = "01N42" if is_warning else "03N42"
        self.assertEqual(
            status.gql_status,
            expected_status,
        )
        expected_description = expected.description or (
            "warn: unknown warning"
            if is_warning
            else "info: unknown notification"
        )
        self.assertEqual(
            status.status_description,
            expected_description,
        )
        if expected.category:
            expected_diagnostic_record["_classification"] = (
                types.as_cypher_type(expected.category)
            )
        self.assertEqual(
            status.raw_classification,
            expected.category or "",
        )
        self.assertEqual(
            status.classification,
            self._parsed_category(expected.category or "UNKNOWN"),
        )
        if expected.severity:
            expected_diagnostic_record["_severity"] = types.as_cypher_type(
                expected.severity
            )
        self.assertEqual(
            status.raw_severity,
            expected.severity or "",
        )
        self.assertEqual(
            status.severity,
            self._parsed_severity(expected.severity or "UNKNOWN"),
        )
        if expected.position is None:
            self.assertIs(
                status.position,
                None,
            )
        else:
            position_dict = {
                "column": expected.position.column,
                "line": expected.position.line,
                "offset": expected.position.offset,
            }
            expected_diagnostic_record["_position"] = types.as_cypher_type(
                position_dict
            )
            self.assertEqual(
                status.position,
                position_dict,
            )
        self.assertEqual(
            status.diagnostic_record,
            expected_diagnostic_record,
        )

    def _test_notification_1(
        self,
        get_summary: t.Callable[[list[Notification]], types.Summary],
    ) -> None:
        # testing WARNING severity without position
        notification = Notification(
            code="Neo.ClientNotification.Statement.JoinHintUnfulfillableWarning",  # noqa: E501
            description="The hinted join was not planned. This could happen because no generated plan contained the join key, please try using a different join key or restructure your query. (hinted join key identifier is: a)",  # noqa: E501
            severity="WARNING",
            title="The database was unable to plan a hinted join.",
            position=None,
            category="HINT",
        )

        summary = get_summary([notification])
        self.assertEqual(len(summary.notifications), 1)
        self.assertEqual(len(summary.gql_status_objects), 2)
        self._assert_notification(
            summary,
            notification,
            position=0,
        )
        self._assert_notification_as_gql_status(
            summary,
            notification,
            position=0,
        )

    def _test_notification_2(
        self,
        get_summary: t.Callable[[list[Notification]], types.Summary],
    ) -> None:
        # testing INFORMATION severity with position
        notification = Notification(
            code="Neo.ClientNotification.Statement.UnboundedVariableLengthPattern",  # noqa: E501
            description="Using shortest path with an unbounded pattern will likely result in long execution times. It is recommended to use an upper limit to the number of node hops in your pattern.",  # noqa: E501
            severity="INFORMATION",
            title="The provided pattern is unbounded, consider adding an upper limit to the number of node hops.",  # noqa: E501
            position=Position(offset=21, line=1, column=22),
            category="PERFORMANCE",
        )
        summary = get_summary([notification])
        self.assertEqual(len(summary.notifications), 1)
        self.assertEqual(len(summary.gql_status_objects), 2)
        self._assert_notification(
            summary,
            notification,
            position=0,
        )
        self._assert_notification_as_gql_status(
            summary,
            notification,
            position=1,
        )

    def _test_notification_3(
        self,
        get_summary: t.Callable[[list[Notification]], types.Summary],
    ) -> None:
        # testing multiple notifications
        notification_info = Notification(
            code="Neo.ClientNotification.Foo.Bar",
            description="Description",
            severity="INFORMATION",
            title="Title",
            position=Position(offset=-2, line=0, column=-1),
            category="MadeUp",
        )
        notification_warning = Notification(
            code="Neo.OhBoi.Foo.BaZ",
            description="Description2",
            severity="WARNING",
            title="Title2",
            position=Position(offset=-1, line=-1, column=-1),
            category="SECURITY",
        )

        summary = get_summary([notification_info, notification_warning])

        self.assertEqual(len(summary.notifications), 2)
        self.assertEqual(len(summary.gql_status_objects), 3)

        self._assert_notification(
            summary,
            notification_info,
            position=0,
        )
        self._assert_notification(
            summary,
            notification_warning,
            position=1,
        )

        self._assert_notification_as_gql_status(
            summary,
            notification_warning,
            position=0,
        )
        self._assert_notification_as_gql_status(
            summary,
            notification_info,
            position=2,
        )

    def test_session_notification_1(self) -> None:
        self._test_notification_1(
            self._get_summary_with_notifications_session_run
        )

    def test_tx_notification_1(self) -> None:
        self._test_notification_1(self._get_summary_with_notifications_tx)

    def test_session_notification_2(self) -> None:
        self._test_notification_2(
            self._get_summary_with_notifications_session_run
        )

    def test_tx_notification_2(self) -> None:
        self._test_notification_2(self._get_summary_with_notifications_tx)

    def test_session_notification_3(self) -> None:
        self._test_notification_3(
            self._get_summary_with_notifications_session_run
        )

    def test_tx_notification_3(self) -> None:
        self._test_notification_3(self._get_summary_with_notifications_tx)
