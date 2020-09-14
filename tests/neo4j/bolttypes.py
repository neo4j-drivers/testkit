import unittest

from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken, NullRecord
import nutkit.protocol as types
from tests.neo4j.shared import *
from tests.shared import *


class TestBoltTypes(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._host, self._port = get_neo4j_host_and_port()
        self._scheme = "bolt://%s:%d" % (self._host, self._port)

    def tearDown(self):
        self._session.close()
        self._driver.close()
        self._backend.close()

    def createDriverAndSession(self):
        auth_token = AuthorizationToken(scheme="basic",
                                        principal=os.environ.get(env_neo4j_user, "neo4j"),
                                        credentials=os.environ.get(env_neo4j_pass, "pass"))
        self._driver = Driver(self._backend, self._scheme, auth_token)
        self._session = self._driver.session("r")

    def verifyCanEcho(self, key, value):
        result = self._session.run("RETURN $x as y", params={"x": value(key)})
        records = result.next()
        self.assertEqual(records, types.Record(values=[key]))

    def testShouldEchoBack(self):
        test_map = {True:                   types.CypherBool,
                    False:                  types.CypherBool,
                    None:                   types.CypherNull,
                    1:                      types.CypherInt,
                    -7:                     types.CypherInt,
                    -129:                   types.CypherInt,
                    129:                    types.CypherInt,
                    2147483647:             types.CypherInt,
                    -2147483647:            types.CypherInt,
                    9223372036854775807:    types.CypherFloat,
                    -9223372036854775807:   types.CypherFloat,
                    1.7976931348623157E+308: types.CypherFloat,
                    2.2250738585072014e-308: types.CypherFloat,
                    4.9E-324:               types.CypherFloat,
                    0.0:                    types.CypherFloat,
                    1.1:                    types.CypherFloat,
                    "1":                    types.CypherString,
                    "-17∂ßå®":              types.CypherString,
                    "String":               types.CypherString,
                    "":                     types.CypherString}

        self.createDriverAndSession()

        for key, value in test_map.items():
            self.verifyCanEcho(key, value)

    def testShouldEchoVeryLongList(self):
        test_map = {None:                  types.CypherNull,
                    1:                     types.CypherInt,
                    1.1:                   types.CypherFloat,
                    "hello":               types.CypherString,
                    True:                  types.CypherBool}

        self.createDriverAndSession()

        long_list = []
        for key, value in test_map.items():
            long_list.clear()
            for i in range(1000):
                long_list.append(value(key))
            self.verifyCanEcho(long_list, types.CypherList)

    def testShouldEchoVeryLongString(self):
        self.createDriverAndSession()
        long_string = "*" * 10000
        self.verifyCanEcho(long_string, types.CypherString)

    def testShouldEchoNestedLst(self):
        test_lists = [
                        types.CypherList([types.CypherInt(1), types.CypherInt(2), types.CypherInt(3), types.CypherInt(4)]),
                        types.CypherList([types.CypherString("a"), types.CypherString("b"), types.CypherString("c"), types.CypherString("˚C")]),
                        types.CypherList([types.CypherBool(True), types.CypherBool(False)]),
                        types.CypherList([types.CypherFloat(1.1), types.CypherFloat(2.2), types.CypherFloat(3.3), types.CypherFloat(4.4)]),
                        types.CypherList([types.CypherNull(None), types.CypherNull(None)]),
                        types.CypherList([types.CypherNull(None), types.CypherBool(True), types.CypherString("Hello world"), types.CypherInt(-1234567890), types.CypherFloat(1.7976931348623157E+308)])
                     ]

        self.createDriverAndSession()
        self.verifyCanEcho(test_lists, types.CypherList)

    def test_combined_primitives(self):
        self.createDriverAndSession()

        result = self._session.run("RETURN NULL, 1, true, 'string', [1, 'a'], 1.23456")
        record = result.next()
        self.assertNotIsInstance(record, types.NullRecord)

        values = record.values
        self.assertIsInstance(values[0], types.CypherNull)
        self.assertIsInstance(values[1], types.CypherInt)
        self.assertIsInstance(values[2], types.CypherBool)
        self.assertIsInstance(values[3], types.CypherString)
        self.assertIsInstance(values[4], types.CypherList)
        self.assertIsInstance(values[5], types.CypherFloat)
        self.assertEqual(values[2], 1)
        self.assertEqual(values[3], 'string')
        self.assertEqual(values[4].value, [types.CypherInt(1), types.CypherString('a')])

    def test_graph_node(self):
        self.createDriverAndSession()

        result = self._session.run("CREATE (n:TestLabel {num: 1, txt: 'abc'}) RETURN n")
        record = result.next()
        self.assertNotIsInstance(record, types.NullRecord)

        node = record.values[0]
        self.assertIsInstance(node, types.CypherNode)
        self.assertEqual(node.labels, ['TestLabel'])
        self.assertEqual(node.props, types.CypherMap({"num": types.CypherInt(1), "txt": types.CypherString('abc')}))

    # Work in progress
    # def testShouldEchoVeryLongMap(self):
    #    test_map = {
    #                # None: types.CypherNull,
    #                # 1: types.CypherInt,
    #                # 1.1: types.CypherFloat,
    #                "hello": types.CypherString
    #                # True: types.CypherBool
    #                }

    #    self.createDriverAndSession()

    #    long_map = {}
    #    for key, value in test_map.items():
    #        long_map.clear()
    #        for i in range(1000):
    #            long_map[i+1] = value(key)
    #        self.verifyCanEcho(long_map, types.CypherMap)

    # def testShouldEchoNestedMap(self):
    # todo: need to implement the cypher map type to do this test.

    # def test_path(self):
        # todo: need to implement the cypher path type to do this test.
