import unittest

from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken, NullRecord
import nutkit.protocol as types
from tests.neo4j.shared import *
from tests.shared import *


class TestDataTypes(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._host, self._port = get_neo4j_host_and_port()
        self._scheme = "bolt://%s:%d" % (self._host, self._port)
        self._session = None
        self._driver = None

    def tearDown(self):
        if self._session:
            self._session.close()
        if self._driver:
            self._driver.close()
        self._backend.close()

    def createDriverAndSession(self):
        auth_token = AuthorizationToken(scheme="basic",
                                        principal=os.environ.get(env_neo4j_user, "neo4j"),
                                        credentials=os.environ.get(env_neo4j_pass, "pass"))
        self._driver = Driver(self._backend, self._scheme, auth_token)
        self._session = self._driver.session("w")

    def verifyCanEcho(self, val):
        result = self._session.run("RETURN $x as y", params={"x": val})
        record = result.next()
        self.assertEqual(record, types.Record(values=[val]))

    def testShouldEchoBack(self):
        if get_driver_name() in ['javascript', 'java']:
            self.skipTest("Not implemented in backend")

        vals = [
            types.CypherBool(True),
            types.CypherBool(False),
            types.CypherNull(),
            types.CypherInt(1),
            types.CypherInt(-7),
            types.CypherInt(-129),
            types.CypherInt(129),
            types.CypherInt(2147483647),
            types.CypherInt(-2147483647),
            #types.CypherFloat(9223372036854775807),        TODO: Investigate
            #types.CypherFloat(-9223372036854775807),
            #types.CypherFloat(1.7976931348623157E+308),
            #types.CypherFloat(2.2250738585072014e-308),
            #types.CypherFloat(4.9E-324),
            types.CypherFloat(0.0),
            types.CypherFloat(1.1),
            types.CypherString("1"),
            types.CypherString("-17∂ßå®"),
            types.CypherString("String"),
            types.CypherString(""),
        ]

        self.createDriverAndSession()
        for val in vals:
            self.verifyCanEcho(val)


    def testShouldEchoVeryLongList(self):
        if get_driver_name() in ['java']:
            self.skipTest("Not implemented in backend")

        vals = [
            types.CypherNull(),
            types.CypherInt(1),
            types.CypherFloat(1.1),
            types.CypherString("hello"),
            types.CypherBool(True),
        ]

        self.createDriverAndSession()

        for val in vals:
            long_list = []
            for i in range(1000):
                long_list.append(val)
            self.verifyCanEcho(types.CypherList(long_list))

    def testShouldEchoVeryLongString(self):
        if get_driver_name() in ['java']:
            self.skipTest("Not implemented in backend")

        self.createDriverAndSession()
        long_string = "*" * 10000
        self.verifyCanEcho(types.CypherString(long_string))

    def testShouldEchoNestedLists(self):
        if get_driver_name() in ['java', 'javascript']:
            self.skipTest("Not implemented in backend")

        test_lists = [
            types.CypherList([types.CypherInt(1), types.CypherInt(2), types.CypherInt(3), types.CypherInt(4)]),
            types.CypherList([types.CypherString("a"), types.CypherString("b"), types.CypherString("c"), types.CypherString("˚C")]),
            types.CypherList([types.CypherBool(True), types.CypherBool(False)]),
            types.CypherList([types.CypherFloat(1.1), types.CypherFloat(2.2), types.CypherFloat(3.3), types.CypherFloat(4.4)]),
            types.CypherList([types.CypherNull(None), types.CypherNull(None)]),
            types.CypherList([types.CypherNull(None), types.CypherBool(True), types.CypherString("Hello world"), types.CypherInt(-1234567890), types.CypherFloat(1.7976931348623157E+308)])
                     ]

        self.createDriverAndSession()
        self.verifyCanEcho(types.CypherList(test_lists))

    def testShouldEchoNode(self):
        self.createDriverAndSession()

        result = self._session.run("CREATE (n:TestLabel {num: 1, txt: 'abc'}) RETURN n")
        record = result.next()
        self.assertNotIsInstance(record, types.NullRecord)

        node = record.values[0]
        self.assertIsInstance(node, types.CypherNode)
        self.assertEqual(node.labels, types.CypherList([types.CypherString('TestLabel')]))
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
