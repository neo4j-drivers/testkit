import os

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.neo4j.shared import (
    env_neo4j_pass,
    env_neo4j_user,
    get_neo4j_host_and_port,
)
from tests.shared import TestkitTestCase


class TestDataTypes(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._host, self._port = get_neo4j_host_and_port()
        self._scheme = "bolt://%s:%d" % (self._host, self._port)
        self._session = None
        self._driver = None

    def tearDown(self):
        if self._session:
            self._session.close()
        if self._driver:
            self._driver.close()
        super().tearDown()

    def createDriverAndSession(self):
        auth_token = types.AuthorizationToken(
            scheme="basic",
            principal=os.environ.get(env_neo4j_user, "neo4j"),
            credentials=os.environ.get(env_neo4j_pass, "pass")
        )
        self._driver = Driver(self._backend, self._scheme, auth_token)
        self._session = self._driver.session("w")

    def verifyCanEcho(self, val):
        result = self._session.run("RETURN $x as y", params={"x": val})
        record = result.next()
        self.assertEqual(record, types.Record(values=[val]))

    def testShouldEchoBack(self):
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
            #types.CypherFloat(9223372036854775807),       # TODO: Investigate
            #types.CypherFloat(-9223372036854775807),
            #types.CypherFloat(1.7976931348623157E+308),
            #types.CypherFloat(2.2250738585072014e-308),
            #types.CypherFloat(4.9E-324),
            #types.CypherFloat(0.0),  # Js can not determine if it should be 0 or 0.0
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

        self.createDriverAndSession()
        long_string = "*" * 10000
        self.verifyCanEcho(types.CypherString(long_string))

    def testShouldEchoNestedLists(self):

        test_lists = [
            types.CypherList([types.CypherInt(1), types.CypherInt(2), types.CypherInt(3), types.CypherInt(4)]),
            types.CypherList([types.CypherString("a"), types.CypherString("b"), types.CypherString("c"), types.CypherString("˚C")]),
            types.CypherList([types.CypherBool(True), types.CypherBool(False)]),
            types.CypherList([types.CypherFloat(1.1), types.CypherFloat(2.2), types.CypherFloat(3.3), types.CypherFloat(4.4)]),
            types.CypherList([types.CypherNull(None), types.CypherNull(None)]),
            types.CypherList([types.CypherNull(None), types.CypherBool(True), types.CypherString("Hello world"), types.CypherInt(-1234567890), types.CypherFloat(123.456)])
                     ]

        self.createDriverAndSession()
        self.verifyCanEcho(types.CypherList(test_lists))

    def testShouldEchoListOfMaps(self):
        test_list = [
            types.CypherMap({
                "a": types.CypherInt(1),
                "b": types.CypherInt(2)
            }),
            types.CypherMap({
                "c": types.CypherInt(3),
                "d": types.CypherInt(4)
            })
        ]

        self.createDriverAndSession()
        self.verifyCanEcho(types.CypherList(test_list))

    def testShouldEchoMapOfLists(self):
        test_map = {
            'a': types.CypherList([types.CypherInt(1)]),
            'b': types.CypherList([types.CypherInt(2)])
        }

        self.createDriverAndSession()
        self.verifyCanEcho(types.CypherMap(test_map))

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
    def testShouldEchoVeryLongMap(self):

        test_list = [
                       types.CypherNull(None),
                       types.CypherInt(1),
                       types.CypherFloat(1.1),
                       types.CypherString("Hello World"),
                       types.CypherBool(True)
                    ]

        self.createDriverAndSession()

        long_map = {}
        for cypherType in test_list:
            long_map.clear()
            for i in range(1000):
                long_map[str(i)] = cypherType
            self.verifyCanEcho(types.CypherMap(long_map))

    def testShouldEchoNestedMap(self):

        test_maps = {
            "a": types.CypherMap({"a": types.CypherInt(1),
                                  "b": types.CypherInt(2),
                                  "c": types.CypherInt(3),
                                  "d": types.CypherInt(4)}),
            "b": types.CypherMap({"a": types.CypherBool(True),
                                  "b": types.CypherBool(False)}),
            "c": types.CypherMap({"a": types.CypherFloat(1.1),
                                  "b": types.CypherFloat(2.2),
                                  "c": types.CypherFloat(3.3)}),
            "d": types.CypherMap({"a": types.CypherString("a"),
                                  "b": types.CypherString("b"),
                                  "c": types.CypherString("c"),
                                  "temp": types.CypherString("˚C")}),
            "e": types.CypherMap({"a": types.CypherNull(None)}),
            "f": types.CypherMap({"a": types.CypherInt(1),
                                  "b": types.CypherBool(True),
                                  "c": types.CypherFloat(3.3),
                                  "d": types.CypherString("Hello World"),
                                  "e": types.CypherNull(None)}),

        }

        self.createDriverAndSession()
        self.verifyCanEcho(types.CypherMap(test_maps))

    # def test_path(self):
        # todo: need to implement the cypher path type to do this test.
