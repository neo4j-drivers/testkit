from __future__ import annotations

import enum
import typing as t

from nutkit import protocol as types
from tests.shared import (
    get_driver_name,
    Potential,
)
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoint_builders import (
    TxEndpointBuilder,
)
from tests.stub.http_query.shared.http_endpoints import (
    HttpIncompleteEndpoint,
    HttpSequenceEndpoint,
)
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    from nutkit.frontend import Transaction
    from tests.stub.http_query.shared.http_endpoints import HttpEndpoint
    from tests.stub.http_query.shared.http_server import HTTPServer

DB = "dba"
AUTH = HttpTestCase.AUTH
QUERY = "RETURN 1 AS n"
FIELDS = ["n"]
RECORDS: list[list[http_types.HttpType]] = [[http_types.Int(1)]]


class _FailPoint(enum.Enum):
    PRE_TX_CREATION_INCOMPLETE = enum.auto()
    PRE_TX_CREATION = enum.auto()
    PRE_QUERY_HEADER = enum.auto()
    PRE_QUERY_RECORDS = enum.auto()
    POST_QUERY_RECORDS = enum.auto()
    TX_COMMIT_INCOMPLETE = enum.auto()


def _make_tx_endpoints(
    errors: list[tuple[dict[str, object], _FailPoint]],
    *,
    pipeline_begin: Potential,
) -> HttpEndpoint:

    if not errors:
        raise ValueError("At least one error must be provided")

    return HttpSequenceEndpoint(
        *(
            _make_tx_endpoint(
                [error], pipeline_begin, fail_point, tx_id=f"tx{i + 1:02}"
            )
            for i, (error, fail_point) in enumerate(errors)
        ),
        TxEndpointBuilder(
            db=DB,
            auth=AUTH,
            pipeline_begin=pipeline_begin,
            tx_id=f"tx{len(errors) + 1:02}",
        )
        .with_query(QUERY, FIELDS, RECORDS)
        .with_commit()
        .build(),
    )


def _make_tx_endpoint(
    errors: list[dict[str, object]],
    pipeline_begin: Potential,
    fail_point: _FailPoint,
    tx_id: str,
) -> HttpEndpoint:
    tx_errors = None
    query_errors = None
    fields = None
    records = None
    wrapper: t.Callable[[TxEndpointBuilder], HttpEndpoint] | None = None
    if fail_point == _FailPoint.PRE_TX_CREATION_INCOMPLETE:

        def wrapper(builder: TxEndpointBuilder) -> HttpEndpoint:  # noqa: F811
            return HttpIncompleteEndpoint(builder.build())

        fields = FIELDS
        records = RECORDS
    elif fail_point == _FailPoint.PRE_TX_CREATION:
        tx_errors = errors
    elif fail_point == _FailPoint.PRE_QUERY_HEADER:
        query_errors = errors
    elif fail_point == _FailPoint.PRE_QUERY_RECORDS:
        query_errors = errors
        fields = FIELDS
    elif fail_point == _FailPoint.POST_QUERY_RECORDS:
        query_errors = errors
        fields = FIELDS
        records = RECORDS
    elif fail_point == _FailPoint.TX_COMMIT_INCOMPLETE:

        def wrapper(builder: TxEndpointBuilder) -> HttpEndpoint:
            builder.with_commit()
            assert builder.finishing_handler is not None
            builder.finishing_handler = HttpIncompleteEndpoint(
                builder.finishing_handler
            )
            return builder.build()

        fields = FIELDS
        records = RECORDS
    else:
        t.assert_never(fail_point)

    builder = TxEndpointBuilder(
        db=DB,
        auth=AUTH,
        pipeline_begin=pipeline_begin,
        tx_id=tx_id,
        tx_errors=tx_errors,
    )

    if fail_point != _FailPoint.PRE_TX_CREATION:
        builder = builder.with_query(
            QUERY, fields, records, query_errors=query_errors
        )

    return builder.build() if wrapper is None else wrapper(builder)


class TestRetries(HttpTestCase):
    def test_transaction_retries(self) -> None:
        error: dict[str, object] = {
            "code": "Neo.TransientError.Completely.MadeUp",
            "message": "A thing™ went wrong...",
        }
        if self.driver_supports_features(types.Feature.OPT_EAGER_TX_BEGIN):
            pipeline_begin = Potential.NO
        else:
            pipeline_begin = Potential.MAYBE

        with self.server() as server:
            for fail_point in list(_FailPoint):
                if fail_point == _FailPoint.TX_COMMIT_INCOMPLETE:
                    continue  # not supposed to be retried
                with (
                    self.subTest(fail_point=fail_point),
                    self.server_session(server),
                ):
                    self._test_transaction_retries(
                        error, fail_point, server, pipeline_begin
                    )

    def _test_transaction_retries(
        self,
        error: dict[str, object],
        fail_point: _FailPoint,
        server: HTTPServer,
        pipeline_begin: Potential,
    ) -> None:
        def work(tx: Transaction) -> tuple[list[str], list[types.Record]]:
            res = tx.run(QUERY)
            keys = res.keys()
            return keys, list(res)

        server.install_discovery_endpoint()
        server.install_endpoint(
            _make_tx_endpoints(
                [(error, fail_point)],
                pipeline_begin=pipeline_begin,
            ),
            handler_type=HandlerType.PERMANENT,
        )
        with (
            self.driver(server, AUTH) as driver,
            driver.session("w", database=DB) as session,
        ):
            keys, records = session.execute_write(work)

        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def test_no_retry_on_disconnect_on_commit(self) -> None:
        tries = 0

        def work(tx: Transaction) -> tuple[list[str], list[types.Record]]:
            nonlocal tries
            tries += 1
            res = tx.run(QUERY)
            keys = res.keys()
            return keys, list(res)

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                _make_tx_endpoints(
                    [({}, _FailPoint.TX_COMMIT_INCOMPLETE)],
                    pipeline_begin=Potential.MAYBE,
                ),
                handler_type=HandlerType.PERMANENT,
            )
            with (
                self.driver(server, AUTH) as driver,
                driver.session("w", database=DB) as session,
                self.assertRaises(types.DriverError) as exc,
            ):
                session.execute_write(work)

            self._assert_is_incomplete_commit_error(exc.exception)

    def _assert_is_incomplete_commit_error(self, error: types.DriverError):
        driver_name = get_driver_name()

        if driver_name in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.IncompleteCommit'>",
                error.errorType,
            )
        else:
            raise NotImplementedError(f"Add error assertion for {driver_name}")
