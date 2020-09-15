import unittest

from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken, NullRecord
import nutkit.protocol as types
from tests.neo4j.shared import *
from tests.shared import *


class TestSessionRun(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._driver = get_driver(self._backend)
        self._session = self._driver.session("r")

    def tearDown(self):
        self._session.close()
        self._driver.close()
        self._backend.close()

    def test_iteration_smaller_than_fetch_size(self):
        # Verifies that correct number of records are retrieved
        # Retrieve one extra record after last one the make sure driver can handle that.
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
        # Verifies that correct number of records are retrieved and that the parameter
        # is respected. Uses parameter to generate a long list of records.
        # Typical fetch size is 1000, selected value should be a bit larger than fetch size,
        # if driver allows this as a parameter we should set it to a known value.
        n = 1007
        result = self._session.run("UNWIND RANGE(0, $n) AS x RETURN x", params={"n": types.CypherInt(n)})
        for x in range(0, n):
            exp = types.Record(values=[types.CypherInt(x)])
            rec = result.next()
            self.assertEqual(rec, exp)

    def test_recover_from_invalid_query(self):
        # Verifies that an error is returned on an invalid query and that the session
        # can function with a valid query afterwards.
        with self.assertRaises(types.DriverError) as e:
            # DEVIATION
            # Go   - error trigger upon run
            # Java - error trigger upon iteration
            result = self._session.run("INVALID QUERY")
            result.next()
            # TODO: Further inspection of the type of error? Should be a client error

        # This one should function properly
        result = self._session.run("RETURN 1 as n")
        self.assertEqual(result.next(), types.Record(values=[types.CypherInt(1)]))

    def test_recover_from_fail_on_streaming(self):
        result = self._session.run("UNWIND [1, 0, 2] AS x RETURN 10 / x")
        self.assertEqual(result.next(), types.Record(values=[types.CypherInt(10)]))
        with self.assertRaises(types.DriverError) as e:
            result.next()
        # TODO: Further inspection of the type of error? Should be a database error

        # This one should function properly
        result = self._session.run("RETURN 1 as n")
        self.assertEqual(result.next(), types.Record(values=[types.CypherInt(1)]))

