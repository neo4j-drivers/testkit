from __future__ import annotations

import re
import typing as t

from nutkit import protocol as types
from tests.shared import get_driver_name
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
    from werkzeug.datastructures import Headers

    from nutkit.frontend import Driver
    from tests.stub.http_query.shared.http_server import HTTPServer

    T = t.TypeVar("T")
    TExc = t.TypeVar("TExc", bound=Exception)


DB = "dba"
QUERY = "RETURN 1 AS n"
FIELDS = ["n"]
RECORDS: list[list[http_types.HttpType]] = [[http_types.Int(1)]]
AUTH = HttpTestCase.AUTH
USER_AGENT = "Eesaiph0 einuWee8 vooHo4ku"


def _make_user_agent_query_endpoint(
    expected_user_agent: str,
) -> HttpQueryEndpoint:
    def header_matcher(headers: Headers) -> bool:
        lower_user_agent_values = map(str.lower, headers.getlist("User-Agent"))
        return expected_user_agent.lower() not in lower_user_agent_values

    return HttpQueryEndpoint(
        HttpQueryEndpoint.RequestData(
            db=DB,
            auth=AUTH,
            query=QUERY,
        ),
        HttpQueryEndpoint.ResponseData(
            fields=FIELDS,
            records=RECORDS,
        ),
        extra_header_verification=(header_matcher,),
    )


def _make_user_agent_tx_endpoint(
    expected_user_agent: str,
) -> HttpEndpoint:
    def header_matcher(headers: Headers) -> bool:
        lower_user_agent_values = map(str.lower, headers.getlist("User-Agent"))
        return expected_user_agent.lower() not in lower_user_agent_values

    return (
        TxEndpointBuilder(
            db=DB,
            auth=AUTH,
            extra_header_verification=(header_matcher,),
        )
        .with_query(QUERY, FIELDS, RECORDS)
        .with_commit()
        .build()
    )


class TestUserAgent(HttpTestCase):
    # We expect the client not to send the configured user agent string,
    # because HTTP limits the characters that can be used in header values.
    #
    #     USER_AGENT = "BestDriverInTheWorld: '\"\t\x00\n\n\r\n\r\n🔥"
    #
    # for instance would totally break.

    def test_session_run(self) -> None:
        query_endpoint = _make_user_agent_query_endpoint(USER_AGENT)

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.ONESHOT
            )
            with (
                self.driver(server, AUTH, user_agent=USER_AGENT) as driver,
                driver.session("w", database=DB) as session,
            ):
                result = session.run(QUERY)
                keys = result.keys()
                records = list(result)

        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def test_explicit_tx(self) -> None:
        query_endpoint = _make_user_agent_tx_endpoint(USER_AGENT)

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, AUTH, user_agent=USER_AGENT) as driver,
                driver.session("w", database=DB) as session,
                session.begin_transaction() as tx,
            ):
                result = tx.run(QUERY)
                keys = result.keys()
                records = list(result)
                tx.commit()

        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def test_execute_query(self) -> None:
        query_endpoint = _make_user_agent_tx_endpoint(USER_AGENT)

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.PERMANENT
            )
            with self.driver(server, AUTH, user_agent=USER_AGENT) as driver:
                result = driver.execute_query(QUERY, database=DB)
                keys = result.keys
                records = result.records

        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])


class TestDatabase(HttpTestCase):
    # Drivers must limit the set of database names transmitted via HTTP.
    #
    # - This mitigates URL injections such as /query/v2/../
    # - The server does not react friendly to some encoded URLs such as
    #   `/` and `..` (even when URL encoded, even when multiple times).
    #   E.g., `%2F` (`%252F`) and `%2E%2E` (`%252E%252E`).
    #
    # The driver MUST reject any db name not matching `[a-zA-Z0-9-.]{3,}`
    # before the request hits the network.

    def test_database(self) -> None:
        with self.server() as server:
            for db, match_db_in_err in (
                ("aaa", True),
                ("aaaaa", True),
                ("-----", True),
                (".....", True),
                ("a-.AzZ", True),
                ("AabcdefghijklmnopqrstuvwxyzZ", True),
                ("aABCDEFGHIJKLMNOPQRSTUVWXYZz", True),
                ("aa_aa", True),
                ("aa/aa", True),
                ("aa~aa", True),
                ("aa,aa", True),
                ("aa,aa", True),
                ("aa;aa", True),
                ("aa=aa", True),
                ("aa+aa", True),
                ("aa?aa", True),
                ("aa!aa", True),
                ("aa'aa", False),
                ('aa"aa', False),
                ("aa`aa", False),
                ("aa%61aa", True),
                ("aa\\aa", False),
                ("aa\naa", False),
                ("aa\taa", False),
                ("aa\x00aa", False),
                ("aa aa", True),
                ("aa🔥aa", True),
                ("aaäaa", True),
                ("aa", True),
                ("a", True),
                (None, False),
            ):
                for api in (
                    "session_run",
                    "explicit_tx",
                    "execute_query",
                ):
                    with (
                        self.subTest(db=db, api=api),
                        self.server_session(server),
                    ):
                        self._test_database(server, db, match_db_in_err, api)

    ACCEPTED_DB_NAMES = re.compile(r"[a-zA-Z0-9.-]{3,}")

    def _test_database(
        self,
        server: HTTPServer,
        db: str | None,
        match_db_in_err: bool,
        api: t.Literal["session_run", "explicit_tx", "execute_query"],
    ) -> None:
        fails = db is None or not self.ACCEPTED_DB_NAMES.fullmatch(db)

        endpoint: HttpEndpoint
        if api == "session_run":
            endpoint = HttpQueryEndpoint(
                HttpQueryEndpoint.RequestData(
                    db=re.compile(r".*"),
                    auth=AUTH,
                    query=QUERY,
                ),
                HttpQueryEndpoint.ResponseData(
                    fields=FIELDS,
                    records=RECORDS,
                ),
            )
            handler_type = (
                HandlerType.ONESHOT if fails else HandlerType.PERMANENT
            )
        elif api == "explicit_tx" or api == "execute_query":
            endpoint = (
                TxEndpointBuilder(
                    db=re.compile(r".*"),
                    auth=AUTH,
                )
                .with_query(QUERY, FIELDS, RECORDS)
                .with_commit()
                .build()
            )
            handler_type = HandlerType.PERMANENT
        else:
            t.assert_never(api)

        server.install_discovery_endpoint()
        server.install_endpoint(endpoint, handler_type=handler_type)

        with self.driver(server, AUTH) as driver:
            if fails:
                if api == "session_run":
                    exc = self._session_run_fail(driver, db)
                elif api == "explicit_tx":
                    exc = self._explicit_tx_fail(driver, db)
                elif api == "execute_query":
                    exc = self._execute_query_fail(driver, db)
                else:
                    t.assert_never(api)
                self._assert_invalid_database_name_error(
                    exc,
                    db if match_db_in_err else None,
                )
            else:
                if api == "session_run":
                    keys, records = self._session_run(driver, db)
                elif api == "explicit_tx":
                    keys, records = self._explicit_tx(driver, db)
                elif api == "execute_query":
                    keys, records = self._execute_query(driver, db)
                else:
                    t.assert_never(api)

                self.assertEqual(keys, FIELDS)
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0].values, [types.CypherInt(1)])

    def _session_run_fail(
        self,
        driver: Driver,
        db: str | None,
    ) -> types.DriverError:
        with driver.session("r", database=db) as session:
            with self.assertRaises(types.DriverError) as exc:
                session.run(QUERY).consume()
        return exc.exception

    def _explicit_tx_fail(
        self,
        driver: Driver,
        db: str | None,
    ) -> types.DriverError:
        eager_begin = self.driver_supports_features(
            types.Feature.OPT_EAGER_TX_BEGIN
        )
        with driver.session("r", database=db) as session:
            if eager_begin:
                with self.assertRaises(types.DriverError) as exc:
                    session.begin_transaction()
            else:
                with (
                    session.begin_transaction() as tx,
                    self.assertRaises(types.DriverError) as exc,
                ):
                    tx.run(QUERY).consume()
        return exc.exception

    def _execute_query_fail(
        self,
        driver: Driver,
        db: str | None,
    ) -> types.DriverError:
        with self.assertRaises(types.DriverError) as exc:
            driver.execute_query(QUERY, database=db)
        return exc.exception

    def _session_run(
        self,
        driver: Driver,
        db: str | None,
    ) -> tuple[list[str], list[types.Record]]:
        with driver.session("r", database=db) as session:
            result = session.run(QUERY)
            keys = result.keys()
            records = list(result)
        return keys, records

    def _explicit_tx(
        self,
        driver: Driver,
        db: str | None,
    ) -> tuple[list[str], list[types.Record]]:
        with (
            driver.session("r", database=db) as session,
            session.begin_transaction() as tx,
        ):
            result = tx.run(QUERY)
            keys = result.keys()
            records = list(result)
            tx.commit()
        return keys, records

    def _execute_query(
        self,
        driver: Driver,
        db: str | None,
    ) -> tuple[list[str], list[types.Record]]:
        result = driver.execute_query(QUERY, database=db)
        return result.keys, result.records

    def _assert_invalid_database_name_error(
        self,
        exc: types.DriverError,
        db: str | None,
    ) -> None:
        self.assertFalse(exc.retryable)
        msg = exc.msg
        msg_lower = msg.lower()
        self.assertTrue("query api" in msg_lower or "http" in msg_lower)
        self.assertIn("database name", msg_lower)
        if db is not None:
            self.assertIn(db, msg)

        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual(
                exc.errorType, "<class 'neo4j.exceptions.ConfigurationError'>"
            )
