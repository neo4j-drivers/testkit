import unittest

from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken, NullRecord
import nutkit.protocol as types
from tests.neo4j.shared import *



class TestDatatypes(unittest.TestCase):
    def setUp(self):
        self._backend = newBackend()
        self._driver = getDriver(self._backend)
        self._session = self._driver.session("r")

    def tearDown(self):
        self._session.close()
        self._driver.close()
        self._backend.close()

    def test_primitives(self):
        result = self._session.run("RETURN NULL, 1, 'string', [1, 'a']")
        record = result.next()

        self.assertNotIsInstance(record, types.NullRecord)
        values = record.values
        self.assertIsInstance(values[0], types.CypherNull)
        self.assertIsInstance(values[1], types.CypherInt)
        self.assertIsInstance(values[2], types.CypherString)
        self.assertIsInstance(values[3], types.CypherList)
