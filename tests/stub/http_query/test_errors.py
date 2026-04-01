from __future__ import annotations

import typing as t
from random import shuffle

from nutkit import protocol as types
from tests.shared import (
    get_driver_name,
    Potential,
)
from tests.stub.errors.shared import (
    DEFAULT_DIAG_REC,
    default_gql_error_description,
    DEFAULT_GQL_ERROR_STATUS,
)
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoint_builders import (
    TxEndpointBuilder,
)
from tests.stub.http_query.shared.http_endpoints import (
    HttpEndpoint,
    HttpQueryEndpoint,
)
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    from nutkit.frontend import (
        Driver,
        Session,
        Transaction,
    )

    T = t.TypeVar("T")
    TExc = t.TypeVar("TExc", bound=Exception)


DB = "dba"
AUTH = HttpTestCase.AUTH
QUERY = "RETURN 1 AS n"
FIELDS = ["n"]
RECORDS: list[list[http_types.HttpType]] = [[http_types.Int(1)]]


def _make_query_endpoint(
    errors: list[dict[str, object]],
    send_header: bool = False,
    send_records: bool = False,
    status_code: int | None = None,
) -> HttpQueryEndpoint:
    response = HttpQueryEndpoint.ResponseData(
        errors=errors,
        status_code=status_code,
    )
    if send_header:
        response.fields = FIELDS
    if send_records:
        response.records = RECORDS

    return HttpQueryEndpoint(
        HttpQueryEndpoint.RequestData(
            db=DB,
            auth=AUTH,
            query=QUERY,
        ),
        response,
    )


def _make_tx_endpoint(
    errors: list[dict[str, object]],
    *,
    pipeline_begin: Potential,
    fail_tx_creation: bool = False,
    send_query_header: bool = False,
    send_query_records: bool = False,
    status_code: int | None = None,
) -> HttpEndpoint:
    tx_errors = errors if fail_tx_creation else None
    tx_status_code = status_code if fail_tx_creation else None
    query_errors = None if fail_tx_creation else errors
    query_status_code = None if fail_tx_creation else status_code
    fields = FIELDS if send_query_header else None
    records = RECORDS if send_query_records else None

    return (
        TxEndpointBuilder(
            db=DB,
            auth=AUTH,
            pipeline_begin=pipeline_begin,
            tx_errors=tx_errors,
            tx_status_code=tx_status_code,
        )
        .with_query(
            QUERY,
            fields,
            records,
            query_errors=query_errors,
            query_status_code=query_status_code,
        )
        .build()
    )


STATUS_CODES = [
    None,  # auto
    # Some early server versions sometimes return errors with 2xx status codes.
    # Drivers should pick up the error anyway and throw.
    200,
    400,
]


class TestErrors(HttpTestCase):
    def _test_session_run(
        self,
        *,
        send_header: bool,
        send_records: bool,
        code: str = "Neo.ClientError.Statement.ArithmeticError",
        msg: str = "/ by zero",
        is_retryable: bool = False,
    ) -> None:
        assert len(RECORDS) == 1, "needed to know when `result.next() fails"
        if send_records and not send_header:
            raise ValueError("Cannot send records without sending header")

        error: dict[str, object] = {"code": code, "message": msg}
        with self.server() as server:
            for status_code in STATUS_CODES:
                with (
                    self.subTest(status_code=status_code),
                    self.server_session(server),
                ):
                    query_endpoint = _make_query_endpoint(
                        [error],
                        send_header=send_header,
                        send_records=send_records,
                        status_code=status_code,
                    )
                    server.install_discovery_endpoint()
                    server.install_endpoint(
                        query_endpoint,
                        handler_type=HandlerType.ONESHOT,
                    )
                    with (
                        self.driver(server, AUTH) as driver,
                        driver.session("w", database=DB) as session,
                    ):
                        exc = self._get_session_run_exception(
                            send_header, send_records, session
                        )

                    self._assert_error(exc, msg, code, is_retryable)

    def _get_session_run_exception(
        self,
        send_header: bool,
        send_records: bool,
        session: Session,
    ) -> types.DriverError:
        driver_name = get_driver_name()

        if not send_header:
            if driver_name in ["javascript"]:
                # TODO: remove this block once all languages work
                result = session.run(QUERY)
                with self.assertRaises(types.DriverError) as exc_capture:
                    result.next()
            else:
                with self.assertRaises(types.DriverError) as exc_capture:
                    session.run(QUERY)
        else:
            result = session.run(QUERY)
            keys = result.keys()
            self.assertEqual(keys, FIELDS)
            if send_records:
                record = result.next()
                self.assertEqual(record.values, [types.CypherInt(1)])
            with self.assertRaises(types.DriverError) as exc_capture:
                result.next()

        return exc_capture.exception

    def _assert_error(
        self,
        exc: types.DriverError,
        msg: str,
        code: str,
        retryable: bool,
    ) -> None:
        supports_retryable_check = self.driver_supports_features(
            types.Feature.API_RETRYABLE_EXCEPTION
        )

        self.assertEqual(exc.msg, msg)
        self.assertEqual(exc.code, code)
        self.assertEqual(exc.gql_status, DEFAULT_GQL_ERROR_STATUS)
        self.assertEqual(
            exc.status_description,
            default_gql_error_description(msg),
        )
        self.assertEqual(
            exc.diagnostic_record,
            {k: types.as_cypher_type(v) for k, v in DEFAULT_DIAG_REC.items()},
        )
        self.assertEqual(exc.raw_classification, None)
        self.assertEqual(exc.classification, "UNKNOWN")
        self.assertIsNone(exc.cause)
        if supports_retryable_check:
            self.assertEqual(exc.retryable, retryable)

    def test_session_run(self) -> None:
        # In BOLT terms, this is equivalent to an auto-commit RUN
        # message receiving a FAILURE response.
        self._test_session_run(send_header=False, send_records=False)

    def test_session_run_with_header(self) -> None:
        # In BOLT terms, this is equivalent to the first auto-commit PULL
        # message receiving a FAILURE response.
        self._test_session_run(send_header=True, send_records=False)

    def test_session_run_with_records(self) -> None:
        # In BOLT terms, this is equivalent to some auto-commit PULL
        # message receiving a FAILURE response
        # after some RECORD responses.
        self._test_session_run(send_header=True, send_records=True)

    def test_retryable_error(self) -> None:
        self._test_session_run(
            send_header=False,
            send_records=False,
            code="Neo.TransientError.Completely.MadeUp",
            msg="This is a retryable error",
            is_retryable=True,
        )

    def test_only_last_error_is_considered(self) -> None:
        errors: list[dict[str, object]] = [
            {"code": "Neo.ClientError.Oh.Err1", "message": "Error message 1"},
            {"code": "Neo.ClientError.Oh.Err2", "message": "Error message 2"},
            {"code": "Neo.ClientError.Oh.Err3", "message": "Error message 3"},
        ]
        shuffle(errors)
        msg = t.cast(str, errors[-1]["message"])
        code = t.cast(str, errors[-1]["code"])
        with self.server() as server:
            for status_code in STATUS_CODES:
                with (
                    self.subTest(status_code=status_code),
                    self.server_session(server),
                ):
                    query_endpoint = _make_query_endpoint(
                        errors, status_code=status_code
                    )
                    server.install_discovery_endpoint()
                    server.install_endpoint(
                        query_endpoint,
                        handler_type=HandlerType.ONESHOT,
                    )
                    with (
                        self.driver(server, AUTH) as driver,
                        driver.session("w", database=DB) as session,
                    ):
                        exc = self._get_session_run_exception(
                            send_header=False,
                            send_records=False,
                            session=session,
                        )

                    self._assert_error(exc, msg, code, retryable=False)

    def _test_explicit_tx(
        self,
        *,
        fail_tx_creation: bool,
        send_query_header: bool,
        send_query_records: bool,
    ) -> None:
        assert len(RECORDS) == 1, "needed to know when `result.next() fails"
        if fail_tx_creation and (send_query_header or send_query_records):
            raise ValueError("Cannot expect query after failed transaction")
        if send_query_records and not send_query_header:
            raise ValueError("Cannot send records without sending header")

        code = "Neo.ClientError.Statement.ArithmeticError"
        msg = "/ by zero"

        error: dict[str, object] = {"code": code, "message": msg}
        with self.server() as server:
            for status_code in STATUS_CODES:
                with (
                    self.subTest(status_code=status_code),
                    self.server_session(server),
                ):
                    query_endpoint = _make_tx_endpoint(
                        [error],
                        pipeline_begin=Potential.NO,
                        fail_tx_creation=fail_tx_creation,
                        send_query_header=send_query_header,
                        send_query_records=send_query_records,
                        status_code=status_code,
                    )
                    server.install_discovery_endpoint()
                    server.install_endpoint(
                        query_endpoint,
                        handler_type=HandlerType.PERMANENT,
                    )
                    with (
                        self.driver(server, AUTH) as driver,
                        driver.session("w", database=DB) as session,
                    ):
                        exc = self._get_explicit_tx_exception(
                            fail_tx_creation,
                            send_query_header,
                            send_query_records,
                            session,
                        )

                    self._assert_error(exc, msg, code, retryable=False)

    def _get_explicit_tx_exception(
        self,
        fail_tx_creation: bool,
        send_query_header: bool,
        send_query_records: bool,
        session: Session,
    ) -> types.DriverError:
        driver_name = get_driver_name()

        if fail_tx_creation:
            with self.assertRaises(types.DriverError) as exc_capture:
                session.begin_transaction()
        elif not send_query_header:
            with session.begin_transaction() as tx:
                if driver_name in ["javascript"]:
                    # TODO: remove this block once all languages work
                    result = tx.run(QUERY)
                    with self.assertRaises(types.DriverError) as exc_capture:
                        result.next()
                else:
                    with self.assertRaises(types.DriverError) as exc_capture:
                        tx.run(QUERY)
        else:
            with session.begin_transaction() as tx:
                result = tx.run(QUERY)
                keys = result.keys()
                self.assertEqual(keys, FIELDS)
                if send_query_records:
                    record = result.next()
                    self.assertEqual(record.values, [types.CypherInt(1)])
                with self.assertRaises(types.DriverError) as exc_capture:
                    result.next()

        return exc_capture.exception

    def test_explicit_tx(self) -> None:
        # In BOLT terms, this is equivalent to an explicit transaction's
        # BEGIN message receiving a FAILURE response.
        self._test_explicit_tx(
            fail_tx_creation=True,
            send_query_header=False,
            send_query_records=False,
        )

    def test_explicit_tx_no_headers(self) -> None:
        # In BOLT terms, this is equivalent to an explicit transaction's
        # RUN message receiving a FAILURE response.
        self._test_explicit_tx(
            fail_tx_creation=False,
            send_query_header=False,
            send_query_records=False,
        )

    def test_explicit_tx_with_headers(self) -> None:
        # In BOLT terms, this is equivalent to an explicit transaction's
        # first PULL message receiving a FAILURE response.
        self._test_explicit_tx(
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=False,
        )

    def test_explicit_tx_with_records(self) -> None:
        # In BOLT terms, this is equivalent to some explicit transaction's
        # PULL message receiving a FAILURE response
        # after some RECORD responses.
        self._test_explicit_tx(
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=True,
        )

    def _test_tx_func(
        self,
        *,
        fail_tx_creation: bool,
        send_query_header: bool,
        send_query_records: bool,
    ) -> None:
        assert len(RECORDS) == 1, "needed to know when `result.next() fails"
        if fail_tx_creation and (send_query_header or send_query_records):
            raise ValueError("Cannot expect query after failed transaction")
        if send_query_records and not send_query_header:
            raise ValueError("Cannot send records without sending header")

        eager_begin = self.driver_supports_features(
            types.Feature.OPT_EAGER_TX_BEGIN
        )

        code = "Neo.ClientError.Statement.ArithmeticError"
        msg = "/ by zero"

        error: dict[str, object] = {"code": code, "message": msg}
        with self.server() as server:
            for status_code in STATUS_CODES:
                with (
                    self.subTest(status_code=status_code),
                    self.server_session(server),
                ):
                    query_endpoint = _make_tx_endpoint(
                        [error],
                        pipeline_begin=(
                            Potential.NO if eager_begin else Potential.MAYBE
                        ),
                        fail_tx_creation=fail_tx_creation,
                        send_query_header=send_query_header,
                        send_query_records=send_query_records,
                        status_code=status_code,
                    )
                    server.install_discovery_endpoint()
                    server.install_endpoint(
                        query_endpoint,
                        handler_type=HandlerType.PERMANENT,
                    )
                    with (
                        self.driver(server, AUTH) as driver,
                        driver.session("w", database=DB) as session,
                    ):
                        exc = self._get_tx_func_exception(
                            fail_tx_creation,
                            send_query_header,
                            send_query_records,
                            session,
                        )

                    self._assert_error(exc, msg, code, retryable=False)

    def _get_tx_func_exception(
        self,
        fail_tx_creation: bool,
        send_query_header: bool,
        send_query_records: bool,
        session: Session,
    ) -> types.DriverError:
        driver_name = get_driver_name()
        eager_begin = self.driver_supports_features(
            types.Feature.OPT_EAGER_TX_BEGIN
        )

        exc: types.DriverError | None = None

        if fail_tx_creation:

            def work(tx: Transaction) -> None:
                nonlocal exc

                if eager_begin:
                    self.fail("Driver should not have started tx function")
                with self.assertRaises(types.DriverError) as exc_capture:
                    tx.run(QUERY)
                exc = exc_capture.exception
                raise exc

            with self.assertRaises(types.DriverError) as exc_capture:
                session.execute_write(work)
            if exc is None:
                exc = exc_capture.exception
            self._assert_same_error(exc, exc_capture.exception)

        elif not send_query_header:

            def work(tx: Transaction) -> None:
                nonlocal exc

                if driver_name in ["javascript"]:
                    # TODO: remove this block once all languages work
                    result = tx.run(QUERY)
                    with self.assertRaises(types.DriverError) as exc_capture:
                        result.next()
                else:
                    with self.assertRaises(types.DriverError) as exc_capture:
                        tx.run(QUERY)
                exc = exc_capture.exception
                raise exc

            with self.assertRaises(types.DriverError) as exc_capture:
                session.execute_write(work)
            if exc is None:
                self.fail("Exception must be raised inside tx function")
            self._assert_same_error(exc, exc_capture.exception)

        else:

            def work(tx: Transaction) -> None:
                nonlocal exc

                result = tx.run(QUERY)
                keys = result.keys()
                self.assertEqual(keys, FIELDS)
                if send_query_records:
                    record = result.next()
                    self.assertEqual(record.values, [types.CypherInt(1)])
                if driver_name in ["javascript"]:
                    # TODO: remove this block once all languages work
                    result = tx.run(QUERY)
                    with self.assertRaises(types.DriverError) as exc_capture:
                        result.next()
                else:
                    with self.assertRaises(types.DriverError) as exc_capture:
                        tx.run(QUERY)
                exc = exc_capture.exception
                raise exc

            with self.assertRaises(types.DriverError) as exc_capture:
                session.execute_write(work)
            if exc is None:
                self.fail("Exception must be raised inside tx function")
            self._assert_same_error(exc, exc_capture.exception)

        return exc

    def _assert_same_error(
        self, a: types.DriverError, b: types.DriverError
    ) -> None:
        self.assertEqual(a.msg, b.msg)
        self.assertEqual(a.code, b.code)
        self.assertEqual(a.gql_status, b.gql_status)
        self.assertEqual(a.status_description, b.status_description)
        self.assertEqual(a.diagnostic_record, b.diagnostic_record)
        self.assertEqual(a.raw_classification, b.raw_classification)

    def test_tx_func(self) -> None:
        # In BOLT terms, this is equivalent to a transaction's
        # BEGIN message receiving a FAILURE response.
        self._test_tx_func(
            fail_tx_creation=True,
            send_query_header=False,
            send_query_records=False,
        )

    def test_tx_func_no_headers(self) -> None:
        # In BOLT terms, this is equivalent to a transaction's
        # RUN message receiving a FAILURE response.
        self._test_tx_func(
            fail_tx_creation=False,
            send_query_header=False,
            send_query_records=False,
        )

    def test_tx_func_with_headers(self) -> None:
        # In BOLT terms, this is equivalent to a transaction's
        # first PULL message receiving a FAILURE response.
        self._test_tx_func(
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=False,
        )

    def test_tx_func_with_records(self) -> None:
        # In BOLT terms, this is equivalent to some transaction's
        # PULL message receiving a FAILURE response
        # after some RECORD responses.
        self._test_tx_func(
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=True,
        )

    def _test_execute_query(
        self,
        *,
        fail_tx_creation: bool,
        send_query_header: bool,
        send_query_records: bool,
    ) -> None:
        assert len(RECORDS) == 1, "needed to know when `result.next() fails"
        if fail_tx_creation and (send_query_header or send_query_records):
            raise ValueError("Cannot expect query after failed transaction")
        if send_query_records and not send_query_header:
            raise ValueError("Cannot send records without sending header")

        code = "Neo.ClientError.Statement.ArithmeticError"
        msg = "/ by zero"

        error: dict[str, object] = {"code": code, "message": msg}
        with self.server() as server:
            for status_code in STATUS_CODES:
                with (
                    self.subTest(status_code=status_code),
                    self.server_session(server),
                ):
                    query_endpoint = _make_tx_endpoint(
                        [error],
                        pipeline_begin=Potential.MAYBE,
                        fail_tx_creation=fail_tx_creation,
                        send_query_header=send_query_header,
                        send_query_records=send_query_records,
                        status_code=status_code,
                    )
                    server.install_discovery_endpoint()
                    server.install_endpoint(
                        query_endpoint,
                        handler_type=HandlerType.PERMANENT,
                    )
                    with self.driver(server, AUTH) as driver:
                        exc = self._get_execute_query_exception(driver)

                    self._assert_error(exc, msg, code, retryable=False)

    def _get_execute_query_exception(
        self,
        driver: Driver,
    ) -> types.DriverError:
        with self.assertRaises(types.DriverError) as exc_capture:
            driver.execute_query(QUERY, routing="w", database=DB)
        return exc_capture.exception

    def test_execute_query(self) -> None:
        # In BOLT terms, this is equivalent to a transaction's
        # BEGIN message receiving a FAILURE response.
        self._test_execute_query(
            fail_tx_creation=True,
            send_query_header=False,
            send_query_records=False,
        )

    def test_execute_query_no_headers(self) -> None:
        # In BOLT terms, this is equivalent to a transaction's
        # RUN message receiving a FAILURE response.
        self._test_execute_query(
            fail_tx_creation=False,
            send_query_header=False,
            send_query_records=False,
        )

    def test_execute_query_with_headers(self) -> None:
        # In BOLT terms, this is equivalent to a transaction's
        # first PULL message receiving a FAILURE response.
        self._test_execute_query(
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=False,
        )

    def test_execute_query_with_records(self) -> None:
        # In BOLT terms, this is equivalent to some transaction's
        # PULL message receiving a FAILURE response
        # after some RECORD responses.
        self._test_execute_query(
            fail_tx_creation=False,
            send_query_header=True,
            send_query_records=True,
        )
