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

    def create_driver_and_session(self):
        auth_token = types.AuthorizationToken(
            scheme="basic",
            principal=os.environ.get(env_neo4j_user, "neo4j"),
            credentials=os.environ.get(env_neo4j_pass, "pass")
        )
        self._driver = Driver(self._backend, self._scheme, auth_token)
        self._session = self._driver.session("w")

    def verify_can_echo(self, val):
        result = self._session.run("RETURN $x as y", params={"x": val})
        record = result.next()
        self.assertEqual(record, types.Record(values=[val]))
        assert isinstance(result.next(), types.NullRecord)

    def test_should_echo_back(self):
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

        self.create_driver_and_session()
        for val in vals:
            self.verify_can_echo(val)

    def test_should_echo_very_long_list(self):
        vals = [
            types.CypherNull(),
            types.CypherInt(1),
            types.CypherFloat(1.1),
            types.CypherString("hello"),
            types.CypherBool(True),
        ]

        self.create_driver_and_session()

        for val in vals:
            long_list = []
            for i in range(1000):
                long_list.append(val)
            self.verify_can_echo(types.CypherList(long_list))

    def test_should_echo_very_long_string(self):

        self.create_driver_and_session()
        long_string = "*" * 10000
        self.verify_can_echo(types.CypherString(long_string))

    def test_should_echo_nested_lists(self):

        test_lists = [
            types.CypherList([types.CypherInt(1), types.CypherInt(2), types.CypherInt(3), types.CypherInt(4)]),
            types.CypherList([types.CypherString("a"), types.CypherString("b"), types.CypherString("c"), types.CypherString("˚C")]),
            types.CypherList([types.CypherBool(True), types.CypherBool(False)]),
            types.CypherList([types.CypherFloat(1.1), types.CypherFloat(2.2), types.CypherFloat(3.3), types.CypherFloat(4.4)]),
            types.CypherList([types.CypherNull(None), types.CypherNull(None)]),
            types.CypherList([types.CypherNull(None), types.CypherBool(True), types.CypherString("Hello world"), types.CypherInt(-1234567890), types.CypherFloat(123.456)])
                     ]

        self.create_driver_and_session()
        self.verify_can_echo(types.CypherList(test_lists))

    def test_should_echo_node(self):
        self.create_driver_and_session()

        result = self._session.run("CREATE (n:TestLabel {num: 1, txt: 'abc'}) RETURN n")
        record = result.next()
        self.assertNotIsInstance(record, types.NullRecord)

        node = record.values[0]
        self.assertIsInstance(node, types.CypherNode)
        self.assertEqual(node.labels, types.CypherList([types.CypherString('TestLabel')]))
        self.assertEqual(node.props, types.CypherMap({"num": types.CypherInt(1), "txt": types.CypherString('abc')}))

    # Work in progress
    def test_should_echo_very_long_map(self):

        test_list = [
                       types.CypherNull(None),
                       types.CypherInt(1),
                       types.CypherFloat(1.1),
                       types.CypherString("Hello World"),
                       types.CypherBool(True)
                    ]

        self.create_driver_and_session()

        long_map = {}
        for cypherType in test_list:
            long_map.clear()
            for i in range(1000):
                long_map[str(i)] = cypherType
            self.verify_can_echo(types.CypherMap(long_map))

    def test_should_echo_nested_map(self):

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

        self.create_driver_and_session()
        self.verify_can_echo(types.CypherMap(test_maps))

    def test_should_echo_list_of_maps(self):
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
        self.create_driver_and_session()
        self.verify_can_echo(types.CypherList(test_list))

    def test_should_echo_map_of_lists(self):
        test_map = {
            'a': types.CypherList([types.CypherInt(1)]),
            'b': types.CypherList([types.CypherInt(2)])
        }
        self.create_driver_and_session()
        self.verify_can_echo(types.CypherMap(test_map))

    # def test_path(self):
        # todo: need to implement the cypher path type to do this test.
