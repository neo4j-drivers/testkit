from nutkit.frontend.session import ApplicationCodeError
import nutkit.protocol as types
from tests.neo4j.shared import (
    get_driver,
    get_server_info,
)
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)


class TestTxFuncRun(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._driver = get_driver(self._backend)
        self._session1 = None
        self._session2 = None

    def tearDown(self):
        for session in (self._session1, self._session2):
            if session:
                session.close()
        self._driver.close()
        super().tearDown()

    def test_simple_query(self):
        def work(tx):
            result = tx.run("UNWIND [1, 2, 3, 4] AS x RETURN x")
            if consume:
                summary = result.consume()
                self.assertIsInstance(summary, types.Summary)
            else:
                self.assertEqual(list(result), [
                    types.Record([types.CypherInt(i)]) for i in range(1, 5)
                ])

        def _test():
            self._driver.close()
            self._driver = get_driver(self._backend, user_agent="test")
            self._session1 = self._driver.session("r", fetch_size=2)
            self._session1.read_transaction(work)
            self._session1.close()
            self._session1 = None

        for consume in (True, False):
            with self.subTest("consume" if consume else "iterate"):
                _test()

    def test_iteration_nested(self):
        # Verifies that it is possible to nest results with small fetch sizes
        # within a transaction function.
        # >= 4.0 supports multiple result streams on the connection. From this
        # view it is not possible to see that those streams are actually used.
        # (see stub tests for this verification).
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Fails for some reason")

        def run(tx, i, n):
            return tx.run("UNWIND RANGE ($i, $n) AS x RETURN x",
                          {"i": types.CypherInt(i), "n": types.CypherInt(n)})

        self._session1 = self._driver.session("r", fetch_size=2)

        # Todo: stash away the results for each level and test the behaviour
        #       of the driver when using them outside of the transaction.
        #       separate test?
        lasts = {}

        def nested(tx):
            i0 = 0
            n0 = 6
            res0 = run(tx, i0, n0)
            for r0 in range(i0, n0 + 1):
                rec = res0.next()
                self.assertEqual(
                    rec, types.Record(values=[types.CypherInt(r0)]))
                lasts[0] = rec.values[0].value
                i1 = 7
                n1 = 11
                res1 = run(tx, i1, n1)
                for r1 in range(i1, n1 + 1):
                    rec = res1.next()
                    self.assertEqual(
                        rec, types.Record(values=[types.CypherInt(r1)]))
                    lasts[1] = rec.values[0].value
                    i2 = 999
                    n2 = 1001
                    res2 = run(tx, i2, n2)
                    for r2 in range(i2, n2 + 1):
                        rec = res2.next()
                        self.assertEqual(
                            rec, types.Record(values=[types.CypherInt(r2)]))
                        lasts[2] = rec.values[0].value
                    self.assertEqual(res2.next(), types.NullRecord())
                self.assertEqual(res1.next(), types.NullRecord())
            self.assertEqual(res0.next(), types.NullRecord())
            return "done"

        x = self._session1.read_transaction(nested)
        self.assertEqual(lasts, {0: 6, 1: 11, 2: 1001})
        self.assertEqual(x, "done")

    def test_updates_last_bookmark_on_commit(self):
        # Verifies that last bookmark is set on the session upon succesful
        # commit using transactional function.

        def run(tx):
            tx.run("CREATE (n:SessionNode) RETURN n")

        self._session1 = self._driver.session("w")
        self._session1.write_transaction(run)
        bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(bookmarks), 1)
        self.assertGreater(len(bookmarks[0]), 3)

    def test_does_not_update_last_bookmark_on_rollback(self):
        if get_driver_name() in ["java"]:
            self.skipTest("Client exceptions not properly handled in backend")

        # Verifies that last bookmarks still is empty when transactional
        # function rolls back transaction.
        def run(tx):
            tx.run("CREATE (n:SessionNode) RETURN n")
            raise ApplicationCodeError("No thanks")

        self._session1 = self._driver.session("w")
        expected_exc = types.FrontendError
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            expected_exc = types.DriverError
        if get_driver_name() in ["dotnet"]:
            expected_exc = types.BackendError
        with self.assertRaises(expected_exc):
            self._session1.write_transaction(run)
        bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(bookmarks), 0)

    def test_client_exception_rolls_back_change(self):
        if get_driver_name() in ["java"]:
            self.skipTest("Client exceptions not properly handled in backend")
        node_id = -1

        def run(tx):
            nonlocal node_id
            result_ = tx.run("CREATE (n:VoidNode) RETURN ID(n)")
            node_id = result_.next().values[0].value
            raise ApplicationCodeError("No thanks")

        def assertion_query(tx):
            # Try to retrieve the node, it shouldn't be there
            result = tx.run(
                "MATCH (n:VoidNode) WHERE id(n) = $nodeid RETURN n",
                params={"nodeid": types.CypherInt(node_id)}
            )
            record = result.next()
            self.assertIsInstance(record, types.NullRecord)

        self._session1 = self._driver.session("w")
        expected_exc = types.FrontendError
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            expected_exc = types.DriverError
        if get_driver_name() in ["dotnet"]:
            expected_exc = types.BackendError
        with self.assertRaises(expected_exc):
            self._session1.write_transaction(run)

        self._session1.read_transaction(assertion_query)

    def test_tx_func_configuration(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["java"]:
            self.skipTest("Does not send metadata")

        def run(tx):
            values = []
            result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
            if get_driver_name() not in ["dotnet"]:
                # missing former types.Feature.TMP_RESULT_KEYS
                self.assertEqual(result.keys(), ["x"])
            for record in result:
                values.append(record.values[0])
            if get_server_info().version >= "4":
                result = tx.run("CALL tx.getMetaData")
                record = result.next()
                self.assertIsInstance(record, types.Record)
                self.assertEqual(record.values, [types.CypherMap(metadata)])

            return values

        metadata = {"foo": types.CypherFloat(1.5),
                    "bar": types.CypherString("baz")}
        self._session1 = self._driver.session("w")
        res = self._session1.read_transaction(
            run, timeout=3000,
            tx_meta={k: v.value for k, v in metadata.items()}
        )
        self.assertEqual(res, list(map(types.CypherInt, range(1, 5))))

    def test_tx_timeout(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript", "java"]:
            self.skipTest("Query update2 does not time out.")
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Backend crashes.")

        def create(tx):
            summary = tx.run("MERGE (:Node)").consume()
            return summary.database

        def update1(tx):
            tx.run("MATCH (a:Node) SET a.property = 1").consume()

            with self.assertRaises(types.FrontendError):
                self._session2.write_transaction(update2, timeout=250)

        def update2(tx):
            nonlocal exc
            with self.assertRaises(types.DriverError) as e:
                tx.run("MATCH (a:Node) SET a.property = 2").consume()
            exc = e.exception
            if (exc.code
                    != "Neo.TransientError.Transaction.LockClientStopped"):
                # This is not the error we are looking for. Maybe there was  a
                # leader election or so. Give the driver the chance to retry.
                raise exc
            else:
                # The error we are looking for. Raise ApplicationError instead
                # to make the driver stop retrying.
                raise ApplicationCodeError("Stop, hammer time!")

        exc = None

        self._session1 = self._driver.session("w")
        db = self._session1.write_transaction(create)
        self._session2 = self._driver.session(
            "w", bookmarks=self._session1.last_bookmarks(), database=db
        )
        self._session1.write_transaction(update1)
        self.assertIsInstance(exc, types.DriverError)
        self.assertEqual(exc.code,
                         "Neo.TransientError.Transaction.LockClientStopped")
        if get_driver_name() in ["python"]:
            self.assertEqual(exc.errorType,
                             "<class 'neo4j.exceptions.TransientError'>")
