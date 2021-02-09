import unittest

import nutkit.protocol as types
from tests.neo4j.shared import get_driver
from tests.shared import get_driver_name, new_backend


class TestSessionRun(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._driver = get_driver(self._backend)
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        self._driver.close()
        self._backend.close()

    def test_iteration_smaller_than_fetch_size(self):
        if get_driver_name() not in ['go', 'dotnet', 'javascript', 'java']:
            self.skipTest("Fetchsize not implemented in backend")
        # Verifies that correct number of records are retrieved
        # Retrieve one extra record after last one the make sure driver can
        # handle that.
        self._session = self._driver.session("r", fetchSize=1000)
        result = self._session.run("UNWIND [1, 2, 3, 4, 5] AS x RETURN x")
        expects = [
            types.Record(values=[types.CypherInt(1)]),
            types.Record(values=[types.CypherInt(2)]),
            types.Record(values=[types.CypherInt(3)]),
            types.Record(values=[types.CypherInt(4)]),
            types.Record(values=[types.CypherInt(5)]),
            types.NullRecord(),
            types.NullRecord()
        ]
        for exp in expects:
            rec = result.next()
            self.assertEqual(rec, exp)

    def test_iteration_larger_than_fetch_size(self):
        if get_driver_name() not in ['go', 'dotnet', 'javascript', 'java']:
            self.skipTest("Fetchsize not implemented in backend")
        # Verifies that correct number of records are retrieved and that the
        # parameter is respected. Uses parameter to generate a long list of
        # records.  Typical fetch size is 1000, selected value should be a bit
        # larger than fetch size, if driver allows this as a parameter we
        # should set it to a known value.
        n = 1000
        self._session = self._driver.session("r", fetchSize=n)
        n = n + 7
        result = self._session.run(
            "UNWIND RANGE(0, $n) AS x RETURN x",
            params={"n": types.CypherInt(n)})
        for x in range(0, n):
            exp = types.Record(values=[types.CypherInt(x)])
            rec = result.next()
            self.assertEqual(rec, exp)

    def test_iteration_nested(self):
        if get_driver_name() in ['dotnet']:
            self.skipTest("Nested results not working in 4.2 and earlier. "
                          "FIX AND ENABLE in 4.3")
        if get_driver_name() not in ['go', 'dotnet', 'javascript']:
            self.skipTest("Fetchsize not implemented in backend")
        # Verifies that it is possible to nest results with small fetch sizes.
        # Auto-commit results does not (as of 4.x) support multiple results on
        # the same connection but that isn't visible when testing at
        # this level.
        self._session = self._driver.session("r", fetchSize=2)

        def run(i, n):
            return self._session.run(
                "UNWIND RANGE ($i, $n) AS x RETURN x",
                {"i": types.CypherInt(i), "n": types.CypherInt(n)})
        i0 = 0
        n0 = 6
        res0 = run(i0, n0)
        for r0 in range(i0, n0+1):
            rec = res0.next()
            self.assertEqual(rec, types.Record(values=[types.CypherInt(r0)]))
            i1 = 7
            n1 = 11
            res1 = run(i1, n1)
            for r1 in range(i1, n1+1):
                rec = res1.next()
                self.assertEqual(
                    rec, types.Record(values=[types.CypherInt(r1)]))
                i2 = 999
                n2 = 1001
                res2 = run(i2, n2)
                for r2 in range(i2, n2+1):
                    rec = res2.next()
                    self.assertEqual(
                        rec, types.Record(values=[types.CypherInt(r2)]))
                self.assertEqual(res2.next(), types.NullRecord())
            self.assertEqual(res1.next(), types.NullRecord())
        self.assertEqual(res0.next(), types.NullRecord())

    def test_recover_from_invalid_query(self):
        # Verifies that an error is returned on an invalid query and that
        # the session can function with a valid query afterwards.
        self._session = self._driver.session("r")
        with self.assertRaises(types.DriverError):
            # DEVIATION
            # Go   - error trigger upon run
            # Java - error trigger upon iteration
            result = self._session.run("INVALID QUERY")
            result.next()
            # TODO: Further inspection of the type of error?
            # Should be a client error

        # This one should function properly
        result = self._session.run("RETURN 1 as n")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(1)]))

    def test_recover_from_fail_on_streaming(self):
        self._session = self._driver.session("r")
        result = self._session.run("UNWIND [1, 0, 2] AS x RETURN 10 / x")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(10)]))
        with self.assertRaises(types.DriverError):
            result.next()
        # TODO: Further inspection of the type of error?
        # Should be a database error

        # This one should function properly
        result = self._session.run("RETURN 1 as n")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(1)]))

    def test_updates_last_bookmark(self):
        if not get_driver_name() in ['go', 'javascript', 'dotnet']:
            self.skipTest("result.consume not implemented in backend")
        self._session = self._driver.session("w")
        result = self._session.run("CREATE (n:SessionNode) RETURN n")
        result.consume()
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 1)
        self.assertGreater(len(bookmarks[0]), 3)
