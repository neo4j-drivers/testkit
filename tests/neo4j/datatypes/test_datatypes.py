import nutkit.protocol as types
from tests.neo4j.datatypes._base import _TestTypesBase
from tests.shared import get_driver_name


class TestDataTypes(_TestTypesBase):
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
            types.CypherInt(-2147483648),

            types.CypherFloat(0),
            types.CypherFloat(0.0),
            types.CypherFloat(float("inf")),
            types.CypherFloat(float("-inf")),
            types.CypherFloat(float("nan")),
            types.CypherFloat(1),
            types.CypherFloat(-1),
            # max/min exponent
            types.CypherFloat(2**1023),
            types.CypherFloat(2**-1022),
            # max/min mantissa
            types.CypherFloat(9007199254740991),
            types.CypherFloat(-9007199254740991),
            types.CypherFloat(-(2 + 1 + 2e-51)),

            types.CypherString("1"),
            types.CypherString("-17∂ßå®"),
            types.CypherString("String"),
            types.CypherString(""),

            types.CypherList([
                types.CypherString("Hello"),
                types.CypherInt(1337),
                types.CypherString("Worlds!"),
            ]),

            types.CypherMap({
                "a": types.CypherString("Hello"),
                "b": types.CypherInt(1337),
                "c": types.CypherString("Worlds!"),
            }),

            types.CypherBytes(bytearray([0x00, 0x33, 0x66, 0x99, 0xCC, 0xFF])),
        ]

        self._create_driver_and_session()
        for val in vals:
            # TODO: remove this block once all languages work
            if get_driver_name() in ["javascript", "dotnet"]:
                # Driver treats float as int
                if (isinstance(val, types.CypherFloat)
                        and float(val.value) % 1 == 0):
                    continue
            # TODO: remove this block once all languages work
            if get_driver_name() in ["java", "javascript", "go", "dotnet"]:
                # driver backend does not implement special float values
                if (isinstance(val, types.CypherFloat)
                        and isinstance(val.value, str)):
                    continue
            # TODO: remove this block once all languages work
            if get_driver_name() in ["java", "javascript", "go", "dotnet"]:
                # driver backend  does not implement byte values
                if isinstance(val, types.CypherBytes):
                    continue

            self._verify_can_echo(val)

    def test_should_echo_very_long_list(self):
        vals = [
            types.CypherNull(),
            types.CypherInt(1),
            types.CypherFloat(1.1),
            types.CypherString("hello"),
            types.CypherBool(True),
        ]

        self._create_driver_and_session()

        for val in vals:
            long_list = []
            for _ in range(1000):
                long_list.append(val)
            self._verify_can_echo(types.CypherList(long_list))

    def test_should_echo_very_long_string(self):
        self._create_driver_and_session()
        long_string = "*" * 10000
        self._verify_can_echo(types.CypherString(long_string))

    def test_should_echo_nested_lists(self):
        test_lists = [
            types.CypherList([
                types.CypherInt(1),
                types.CypherInt(2),
                types.CypherInt(3),
                types.CypherInt(4),
            ]),
            types.CypherList([
                types.CypherString("a"),
                types.CypherString("b"),
                types.CypherString("c"),
                types.CypherString("˚C"),
            ]),
            types.CypherList([
                types.CypherBool(True),
                types.CypherBool(False),
            ]),
            types.CypherList([
                types.CypherFloat(1.1),
                types.CypherFloat(2.2),
                types.CypherFloat(3.3),
                types.CypherFloat(4.4),
            ]),
            types.CypherList([
                types.CypherNull(None),
                types.CypherNull(None)
            ]),
            types.CypherList([
                types.CypherNull(None),
                types.CypherBool(True),
                types.CypherString("Hello world"),
                types.CypherInt(-1234567890),
                types.CypherFloat(123.456)
            ])
        ]

        self._create_driver_and_session()
        self._verify_can_echo(types.CypherList(test_lists))

    def test_should_echo_node(self):
        def work(tx):
            result = tx.run(
                "CREATE (n:TestLabel {num: 1, txt: 'abc'}) RETURN n"
            )
            record_ = result.next()
            assert isinstance(result.next(), types.NullRecord)
            return record_

        self._create_driver_and_session()

        record = self._session.write_transaction(work)
        self.assertIsInstance(record, types.Record)

        node = record.values[0]
        self.assertIsInstance(node, types.CypherNode)
        self.assertEqual(
            node.labels,
            types.CypherList([types.CypherString("TestLabel")])
        )
        self.assertEqual(
            node.props,
            types.CypherMap({
                "num": types.CypherInt(1),
                "txt": types.CypherString("abc")
            })
        )

    def test_should_echo_relationship(self):
        def work(tx):
            result = tx.run("CREATE (a)-[r:KNOWS {since:1999}]->(b) "
                            "RETURN a, b, r")
            record_ = result.next()
            assert isinstance(result.next(), types.NullRecord)
            return record_

        self._create_driver_and_session()

        record = self._session.write_transaction(work)
        self.assertIsInstance(record, types.Record)
        values = record.values
        self.assertEqual(len(values), 3)
        a, b, r = values
        self.assertIsInstance(a, types.CypherNode)
        self.assertIsInstance(b, types.CypherNode)
        self.assertIsInstance(r, types.CypherRelationship)

        # TODO: will need to test elementId instead, once all drivers and
        #       backends support it.
        self.assertNotEqual(a.id, b.id)

        self.assertEqual(a.id, r.startNodeId)
        self.assertEqual(b.id, r.endNodeId)
        self.assertEqual(a.elementId, r.startNodeElementId)
        self.assertEqual(b.elementId, r.endNodeElementId)
        self.assertEqual(r.type, types.CypherString("KNOWS"))
        self.assertEqual(r.props, types.CypherMap(
            {"since": types.CypherInt(1999)}
        ))

    def test_should_echo_path(self):
        def work(tx):
            result = tx.run("CREATE p=(a)-[ab:X]->(b)-[bc:X]->(c) "
                            "RETURN a, b, c, ab, bc, p")
            record_ = result.next()
            assert isinstance(result.next(), types.NullRecord)
            return record_

        self._create_driver_and_session()

        record = self._session.write_transaction(work)
        self.assertIsInstance(record, types.Record)
        values = record.values
        self.assertEqual(len(values), 6)
        a, b, c, ab, bc, p = values

        self.assertIsInstance(a, types.CypherNode)
        self.assertIsInstance(b, types.CypherNode)
        self.assertIsInstance(c, types.CypherNode)
        self.assertIsInstance(ab, types.CypherRelationship)
        self.assertIsInstance(bc, types.CypherRelationship)
        self.assertIsInstance(p, types.CypherPath)

        # TODO: will need to test elementId instead, once all drivers and
        #       backends support it.
        self.assertNotEqual(a.id, b.id)
        self.assertNotEqual(a.id, c.id)
        self.assertNotEqual(b.id, c.id)

        self.assertEqual(ab, types.CypherRelationship(
            ab.id, a.id, b.id, types.CypherString("X"), types.CypherMap({}),
            ab.elementId, a.elementId, b.elementId
        ))
        self.assertEqual(bc, types.CypherRelationship(
            bc.id, b.id, c.id, types.CypherString("X"), types.CypherMap({}),
            bc.elementId, b.elementId, c.elementId
        ))

        self.assertEqual(p, types.CypherPath(types.CypherList([a, b, c]),
                                             types.CypherList([ab, bc])))

    # Work in progress
    def test_should_echo_very_long_map(self):
        test_list = [types.CypherNull(None),
                     types.CypherInt(1),
                     types.CypherFloat(1.1),
                     types.CypherString("Hello World"),
                     types.CypherBool(True)]

        self._create_driver_and_session()

        long_map = {}
        for cypher_type in test_list:
            long_map.clear()
            for i in range(1000):
                long_map[str(i)] = cypher_type
            self._verify_can_echo(types.CypherMap(long_map))

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

        self._create_driver_and_session()
        self._verify_can_echo(types.CypherMap(test_maps))

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
        self._create_driver_and_session()
        self._verify_can_echo(types.CypherList(test_list))

    def test_should_echo_map_of_lists(self):
        test_map = {
            "a": types.CypherList([types.CypherInt(1)]),
            "b": types.CypherList([types.CypherInt(2)])
        }
        self._create_driver_and_session()
        self._verify_can_echo(types.CypherMap(test_map))
