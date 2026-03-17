from __future__ import annotations

import typing as t
from contextlib import contextmanager

from nutkit import protocol as types
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoint_builders import (
    TxEndpointBuilder,
)
from tests.stub.http_query.shared.http_endpoints import HttpSequenceEndpoint
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    from nutkit.frontend import Transaction
    from tests.stub.http_query.shared.http_server import HTTPServer

    T = t.TypeVar("T")
    TExc = t.TypeVar("TExc", bound=Exception)


DB = "dba"
QUERY = "RETURN 1 AS n"
FIELDS = ["n"]
RECORDS1: list[list[http_types.HttpType]] = [[http_types.Int(1)]]
RECORDS2: list[list[http_types.HttpType]] = [[http_types.Int(2)]]
AUTH = HttpTestCase.AUTH


class TestClustering(HttpTestCase):
    def test_affinity_header_explicit_tx(self) -> None:
        with (
            self._affinity_test_setup() as server,
            self.driver(server, AUTH) as driver,
            driver.session("r", database=DB) as session,
        ):
            with session.begin_transaction() as tx:
                result = tx.run(QUERY)
                keys1 = result.keys()
                records1 = list(result)
                tx.commit()
            with session.begin_transaction() as tx:
                result = tx.run(QUERY)
                keys2 = result.keys()
                records2 = list(result)
                tx.commit()

        self._assert_query_1(keys1, records1)
        self._assert_query_2(keys2, records2)

    def test_affinity_header_tx_function(self) -> None:
        def work(tx: Transaction) -> tuple[list[str], list[types.Record]]:
            result = tx.run(QUERY)
            keys = result.keys()
            records = list(result)
            return keys, records

        with (
            self._affinity_test_setup() as server,
            self.driver(server, AUTH) as driver,
            driver.session("r", database=DB) as session,
        ):
            keys1, records1 = session.execute_read(work)
            keys2, records2 = session.execute_read(work)

        self._assert_query_1(keys1, records1)
        self._assert_query_2(keys2, records2)

    def test_affinity_header_execute_query(self) -> None:
        with (
            self._affinity_test_setup() as server,
            self.driver(server, AUTH) as driver,
        ):
            result1 = driver.execute_query(QUERY, database=DB, routing="r")
            result2 = driver.execute_query(QUERY, database=DB, routing="r")

        self._assert_query_1(result1.keys, result1.records)
        self._assert_query_2(result2.keys, result2.records)

    @contextmanager
    def _affinity_test_setup(self) -> t.Generator[HTTPServer]:
        endpoint = HttpSequenceEndpoint(
            (  # noqa: PAR001
                TxEndpointBuilder(
                    db=DB,
                    auth=AUTH,
                    tx_id="Tx01",
                    affinity_header="CoolClusterMember123==",
                )
                .with_query(QUERY, FIELDS, RECORDS1)
                .with_commit()
                .build()
            ),
            (  # noqa: PAR001
                TxEndpointBuilder(
                    db=DB,
                    auth=AUTH,
                    tx_id="2ndT",
                    affinity_header="C00l3rClusterM3mb3r123==",
                )
                .with_query(QUERY, FIELDS, RECORDS2)
                .with_commit()
                .build()
            ),
        )
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                endpoint, handler_type=HandlerType.PERMANENT
            )
            yield server

    def _assert_query_1(
        self, keys: list[str], records: list[types.Record]
    ) -> None:
        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def _assert_query_2(
        self, keys: list[str], records: list[types.Record]
    ) -> None:
        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(2)])
