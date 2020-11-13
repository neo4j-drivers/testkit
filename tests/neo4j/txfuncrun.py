import unittest

from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken, NullRecord
import nutkit.protocol as types
from tests.neo4j.shared import *
from tests.shared import *


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

        def run(tx, i, n):
            return tx.run("UNWIND RANGE ($i, $n) AS x RETURN x", {"i": types.CypherInt(i), "n": types.CypherInt(n)})

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
                self.assertEqual(rec, types.Record(values=[types.CypherInt(r0)]))
                lasts[0] = rec.values[0].value
                i1 = 7
                n1 = 11
                res1 = run(tx, i1, n1)
                for r1 in range(i1, n1+1):
                    rec = res1.next()
                    self.assertEqual(rec, types.Record(values=[types.CypherInt(r1)]))
                    lasts[1] = rec.values[0].value
                    i2 = 999
                    n2 = 1001
                    res2 = run(tx, i2, n2)
                    for r2 in range(i2, n2+1):
                        rec = res2.next()
                        self.assertEqual(rec, types.Record(values=[types.CypherInt(r2)]))
                        lasts[2] = rec.values[0].value
                    self.assertEqual(res2.next(), types.NullRecord())
                self.assertEqual(res1.next(), types.NullRecord())
            self.assertEqual(res0.next(), types.NullRecord())
            return "done"

        x = self._session.readTransaction(nested)
        self.assertEqual(lasts, {0: 6, 1: 11, 2: 1001})
        self.assertEqual(x, "done")
