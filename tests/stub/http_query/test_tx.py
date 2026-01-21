from __future__ import annotations

import abc
import contextlib
import typing as t

from nutkit import protocol as types
from nutkit.frontend import (
    Session,
    Transaction,
)
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoint_builders import (
    TxEndpointBuilder,
)
from tests.stub.http_query.shared.http_endpoints import (
    HttpSequenceEndpoint,
    MaybeNull,
)
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    T = t.TypeVar("T")


class _TxTestCase(abc.ABC, HttpTestCase):
    @abc.abstractmethod
    def _run_in_tx(
        self,
        session: Session,
        work: t.Callable[[Transaction], T],
        commit: bool = True,
        access_mode: t.Literal["r", "w"] = "w",
    ) -> T: ...

    def _test_transaction_write(self):
        db = "dba"
        query = "RETURN 1 AS n"
        fields = ["n"]

        def work(tx: Transaction) -> t.Any:
            result = tx.run(query)
            keys = result.keys()
            records = list(result)

            return keys, records

        tx_endpoint = (
            TxEndpointBuilder(
                db=db,
                auth=self.AUTH,
                tx_id="Ab1l7kUOk3",
                access_mode=MaybeNull("Write"),
            )
            .with_query(query, fields, [[http_types.Int(1)]])
            .with_commit()
            .build()
        )

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                tx_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session("w", database=db) as session,
            ):
                keys, records = self._run_in_tx(session, work)

        self.assertEqual(keys, fields)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def _test_transaction_read(self) -> None:
        db = "dba"
        query = "RETURN 1 AS n"
        fields = ["n"]

        def work(tx: Transaction) -> t.Any:
            result = tx.run(query)
            keys = result.keys()
            records = list(result)

            return keys, records

        tx_endpoint = (
            TxEndpointBuilder(db=db, auth=self.AUTH, access_mode="Read")
            .with_query(query, fields, [[http_types.Int(1)]])
            .with_commit()
            .build()
        )

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                tx_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session("r", database=db) as session,
            ):
                keys, records = self._run_in_tx(session, work, access_mode="r")

        self.assertEqual(keys, fields)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def _test_transaction_rollback(self) -> None:
        db = "neo4j"
        query = "RETURN 'hello world' AS greeting"
        fields = ["greeting"]

        def work(tx: Transaction) -> t.Any:
            result = tx.run(query)
            keys = result.keys()
            records = list(result)

            return keys, records

        tx_endpoint = (
            TxEndpointBuilder(db=db, auth=self.AUTH, tx_id="Ab1l7kUOk3")
            .with_query(query, fields, [[http_types.Str("hello world")]])
            .with_rollback()
            .build()
        )

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                tx_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session("w", database=db) as session,
            ):
                keys, records = self._run_in_tx(session, work, commit=False)

        self.assertEqual(keys, fields)
        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0].values, [types.CypherString("hello world")]
        )

    def _test_transaction_multi_query(self) -> None:
        db = "dba"
        query1 = "RETURN 1 AS n"
        fields1 = ["n"]
        query2 = "UNWIND range(2, 3) AS n RETURN n, range(2, n) AS r"
        fields2 = ["n", "r"]

        def work(tx: Transaction) -> t.Any:
            result1 = tx.run(query1)
            keys1 = result1.keys()
            records1 = list(result1)
            result2 = tx.run(query2)
            keys2 = result2.keys()
            records2 = list(result2)

            return keys1, records1, keys2, records2

        tx_endpoint = (
            TxEndpointBuilder(db=db, auth=self.AUTH, tx_id="Ab1l7kUOk3")
            .with_query(query1, fields1, [[http_types.Int(1)]])
            .with_query(
                query2,
                fields2,
                [
                    list(map(http_types.HttpType.from_native, values))
                    for values in (
                        [2, [2]],
                        [3, [2, 3]],
                    )
                ],
            )
            .with_commit()
            .build()
        )

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                tx_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session("w", database=db) as session,
            ):
                keys1, records1, keys2, records2 = self._run_in_tx(
                    session, work
                )

        self.assertEqual(keys1, fields1)
        self.assertEqual(len(records1), 1)
        self.assertEqual(records1[0].values, [types.CypherInt(1)])

        self.assertEqual(keys2, fields2)
        self.assertEqual(len(records2), 2)
        for i, expected in enumerate(
            (
                (2, [2]),
                (3, [2, 3]),
            )
        ):
            self.assertEqual(
                records2[i].values,
                list(map(types.as_cypher_type, expected)),
            )

    def _test_impersonation(self) -> None:
        db = "dba"
        impersonated_user = "banana_bob"
        query = "RETURN 1 AS n"
        fields = ["n"]

        def work(tx: Transaction) -> t.Any:
            result = tx.run(query)
            keys = result.keys()
            records = list(result)

            return keys, records

        tx_endpoint = (
            TxEndpointBuilder(
                db=db, auth=self.AUTH, impersonated_user=impersonated_user
            )
            .with_query(query, fields, [[http_types.Int(1)]])
            .with_commit()
            .build()
        )
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                tx_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session(
                    "w", database=db, impersonated_user=impersonated_user
                ) as session,
            ):
                keys, records = self._run_in_tx(session, work)

        self.assertEqual(keys, fields)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def _test_session_auth(self) -> None:
        db = "dba"
        query = "RETURN 1 AS n"
        fields = ["n"]

        def work(tx: Transaction) -> t.Any:
            result = tx.run(query)
            keys = result.keys()
            records = list(result)

            return keys, records

        tx_endpoint = (
            TxEndpointBuilder(db=db, auth=self.AUTH2)
            .with_query(query, fields, [[http_types.Int(1)]])
            .with_commit()
            .build()
        )
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                tx_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session(
                    "w", database=db, auth_token=self.AUTH2
                ) as session,
            ):
                keys, records = self._run_in_tx(session, work)

        self.assertEqual(keys, fields)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])

    def _test_bookmarks(self) -> None:
        db = "dba"
        query1 = "RETURN 1 AS n"
        query2 = "RETURN 2 AS n"
        fields = ["n"]
        bookmarks1 = ["bookmark:123", "bookmark:abc"]
        bookmarks2 = ["bookmark:foo"]
        bookmarks3 = ["done"]

        def _work(tx: Transaction, query: str) -> t.Any:
            result = tx.run(query)
            keys = result.keys()
            records = list(result)

            return keys, records

        def work1(tx: Transaction) -> t.Any:
            return _work(tx, query1)

        def work2(tx: Transaction) -> t.Any:
            return _work(tx, query2)

        tx1_endpoint = (
            TxEndpointBuilder(db=db, auth=self.AUTH, bookmarks=bookmarks1)
            .with_query(query1, fields, [[http_types.Int(1)]])
            .with_commit(bookmarks=bookmarks2)
            .build()
        )
        tx2_endpoint = (
            TxEndpointBuilder(db=db, auth=self.AUTH, bookmarks=bookmarks2)
            .with_query(query2, fields, [[http_types.Int(2)]])
            .with_commit(bookmarks=bookmarks3)
            .build()
        )
        queries_endpoint = HttpSequenceEndpoint(tx1_endpoint, tx2_endpoint)
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                queries_endpoint, handler_type=HandlerType.PERMANENT
            )
            with (
                self.driver(server, self.AUTH) as driver,
                driver.session(
                    "w", database=db, bookmarks=bookmarks1
                ) as session,
            ):
                keys1, records1 = self._run_in_tx(session, work1)
                keys2, records2 = self._run_in_tx(session, work2)
                received_bookmarks = session.last_bookmarks()

        self.assertEqual(keys1, fields)
        self.assertEqual(len(records1), 1)
        self.assertEqual(records1[0].values, [types.CypherInt(1)])
        self.assertEqual(keys2, fields)
        self.assertEqual(len(records2), 1)
        self.assertEqual(records2[0].values, [types.CypherInt(2)])
        self.assertEqual(received_bookmarks, bookmarks3)


class TestExplicitTx(_TxTestCase):
    def _run_in_tx(
        self,
        session: Session,
        work: t.Callable[[Transaction], T],
        commit: bool = True,
        access_mode: t.Literal["r", "w"] = "w",
    ) -> T:
        tx = session.begin_transaction()
        result = work(tx)
        if commit:
            tx.commit()
        else:
            tx.rollback()

        return result

    def test_transaction_write(self) -> None:
        super()._test_transaction_write()

    def test_transaction_read(self) -> None:
        super()._test_transaction_read()

    def test_transaction_rollback(self) -> None:
        super()._test_transaction_rollback()

    def test_transaction_multi_query(self) -> None:
        super()._test_transaction_multi_query()

    def test_impersonation(self) -> None:
        super()._test_impersonation()

    def test_session_auth(self) -> None:
        super()._test_session_auth()

    def test_bookmarks(self) -> None:
        super()._test_bookmarks()


class TestTxFunc(_TxTestCase):
    def _run_in_tx(
        self,
        session: Session,
        work: t.Callable[[Transaction], T],
        commit: bool = True,
        access_mode: t.Literal["r", "w"] = "w",
    ) -> T:
        result: T | None = None

        def tx_func(tx: Transaction) -> None:
            nonlocal result
            result = work(tx)
            if not commit:
                raise _RollbackException

        with contextlib.suppress(_RollbackException):
            if access_mode == "r":
                session.execute_read(tx_func)
            else:
                session.execute_write(tx_func)

        assert result is not None, "Work function did not set result"
        return result

    def test_transaction_write(self) -> None:
        super()._test_transaction_write()

    def test_transaction_read(self) -> None:
        super()._test_transaction_read()

    def test_transaction_rollback(self) -> None:
        super()._test_transaction_rollback()

    def test_transaction_multi_query(self) -> None:
        super()._test_transaction_multi_query()

    def test_impersonation(self) -> None:
        super()._test_impersonation()

    def test_session_auth(self) -> None:
        super()._test_session_auth()

    def test_bookmarks(self) -> None:
        super()._test_bookmarks()


class _RollbackException(Exception):
    pass
