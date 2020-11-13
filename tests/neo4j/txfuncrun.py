import unittest

import nutkit.protocol as types
from tests.neo4j.shared import get_driver
from tests.shared import new_backend, get_driver_name


class TestTxFuncRun(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._driver = get_driver(self._backend)
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        self._driver.close()
        self._backend.close()

    def test_iteration_nested(self):
        # Verifies that it is possible to nest results with small fetch sizes
        # within a transaction function.
        # >= 4.0 supports multiple result streams on the connection. From this
        # view it is not possible to see that those streams are actually used.
        # (see stub tests for this verification).

        if get_driver_name() not in ['go', 'dotnet', 'javascript', 'java']:
            self.skipTest("Fetchsize not implemented in backend")
        if get_driver_name() in ['dotnet']:
            self.skipTest("Fails for some reason")

        def run(tx, i, n):
            return tx.run("UNWIND RANGE ($i, $n) AS x RETURN x",
                          {"i": types.CypherInt(i), "n": types.CypherInt(n)})

        self._session = self._driver.session("r", fetchSize=2)

        # Todo: stash away the results for each level and test the behaviour
        #       of the driver when using them outside of the transaction.
        #       separate test?
        lasts = {}

        def nested(tx):
            i0 = 0
            n0 = 6
            res0 = run(tx, i0, n0)
            for r0 in range(i0, n0+1):
                rec = res0.next()
                self.assertEqual(
                    rec, types.Record(values=[types.CypherInt(r0)]))
                lasts[0] = rec.values[0].value
                i1 = 7
                n1 = 11
                res1 = run(tx, i1, n1)
                for r1 in range(i1, n1+1):
                    rec = res1.next()
                    self.assertEqual(
                        rec, types.Record(values=[types.CypherInt(r1)]))
                    lasts[1] = rec.values[0].value
                    i2 = 999
                    n2 = 1001
                    res2 = run(tx, i2, n2)
                    for r2 in range(i2, n2+1):
                        rec = res2.next()
                        self.assertEqual(
                            rec, types.Record(values=[types.CypherInt(r2)]))
                        lasts[2] = rec.values[0].value
                    self.assertEqual(res2.next(), types.NullRecord())
                self.assertEqual(res1.next(), types.NullRecord())
            self.assertEqual(res0.next(), types.NullRecord())
            return "done"

        x = self._session.readTransaction(nested)
        self.assertEqual(lasts, {0: 6, 1: 11, 2: 1001})
        self.assertEqual(x, "done")

    def test_updates_last_bookmark_on_commit(self):
        # Verifies that last bookmark is set on the session upon succesful
        # commit using transactional function.

        def run(tx):
            tx.run("CREATE (n:SessionNode) RETURN n")

        self._session = self._driver.session("w")
        self._session.writeTransaction(run)
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 1)
        self.assertGreater(len(bookmarks[0]), 3)

    def test_does_not_update_last_bookmark_on_rollback(self):
        if get_driver_name() in ["dotnet", "javascript", "java"]:
            self.skipTest("Rollback not implemented in backend")

        # Verifies that last bookmarks still is empty when transactional
        # function rolls back transaction.
        def run(tx):
            tx.run("CREATE (n:SessionNode) RETURN n")
            raise Exception("No thanks")

        self._session = self._driver.session("w")
        try:
            self._session.writeTransaction(run)
        except Exception:
            throwed = True
        self.assertTrue(throwed)
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 0)
