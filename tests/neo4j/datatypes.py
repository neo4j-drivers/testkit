import unittest

from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken, NullRecord
import nutkit.protocol as types
from tests.neo4j.shared import *
from tests.shared import *


class TestDatatypes(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._driver = get_driver(self._backend)
        self._session = self._driver.session("w")

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
        self.assertEqual(values[1], 1)
        self.assertEqual(values[2], 'string')
        self.assertEqual(values[3].value, [types.CypherInt(1), types.CypherString('a')])

    def test_graph_node(self):
        result = self._session.run("CREATE (n:TestLabel {num: 1, txt: 'abc'}) RETURN n")
        record = result.next()
        self.assertNotIsInstance(record, types.NullRecord)

        node = record.values[0]
        self.assertIsInstance(node, types.CypherNode)
        self.assertEqual(node.labels, ['TestLabel'])
        self.assertEqual(node.props, types.CypherMap({"num": types.CypherInt(1), "txt": types.CypherString('abc')}))

