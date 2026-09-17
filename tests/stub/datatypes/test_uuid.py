import json
import uuid

from nutkit import protocol as types
from nutkit.protocol import as_cypher_type
from tests.shared import get_driver_name
from tests.stub.datatypes.echo_test_case import EchoTestCase
from tests.stub.shared import StubServer


class _UuidTestCase(EchoTestCase):
    required_features = (types.Feature.API_TYPE_UUID,)

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)

    def tearDown(self):
        self._server.reset()
        super().tearDown()


class TestUuid6x1(_UuidTestCase):
    def _bolt_version(self):
        return "6.1"

    required_features = (
        *_UuidTestCase.required_features,
        types.Feature.BOLT_6_1,
    )

    def test_uuid(self):
        for value in (
            uuid.UUID("00000000000000000000000000000000"),
            uuid.UUID("ffffffffffffffffffffffffffffffff"),
            uuid.UUID("0102030405060708090a0b0c0d0e0f12"),
            uuid.uuid4(),
        ):
            with self.subTest(value=str(value)):
                cypher_value = as_cypher_type(value)
                jolt_value = json.dumps({"UU": str(value)})
                self._test_echo(self._server, jolt_value, cypher_value)
            self._server.reset()

    def test_uuid_in_list(self):
        uuid_pairs = [
            [
                uuid.UUID("00000000000000000000000000000000"),
                uuid.UUID("ffffffffffffffffffffffffffffffff"),
            ],
            [uuid.UUID("0102030405060708090a0b0c0d0e0f12"), uuid.uuid4()],
        ]
        for pair in uuid_pairs:
            cypher_value = as_cypher_type(pair)
            with self.subTest(values=cypher_value):
                jolt_value = json.dumps([{"UU": str(x)} for x in pair])
                self._test_echo(self._server, jolt_value, cypher_value)
            self._server.reset()

    def test_uuid_in_map(self):
        for value in (
            uuid.UUID("00000000000000000000000000000000"),
            uuid.UUID("ffffffffffffffffffffffffffffffff"),
            uuid.UUID("0102030405060708090a0b0c0d0e0f12"),
            uuid.uuid4(),
        ):
            with self.subTest(value=str(value)):
                cypher_value = as_cypher_type({"key": value})
                jolt_value = json.dumps({"key": {"UU": str(value)}})
                self._test_echo(self._server, jolt_value, cypher_value)
            self._server.reset()

    def test_uuid_as_node_property(self):
        script = "uuid_node_property.script"
        for value in (
            uuid.UUID("00000000000000000000000000000000"),
            uuid.UUID("ffffffffffffffffffffffffffffffff"),
            uuid.UUID("0102030405060708090a0b0c0d0e0f12"),
            uuid.uuid4(),
        ):
            with self.subTest(value=str(value)):
                with self._started_server(
                    self._server,
                    script,
                    vars_={"#UUID#": json.dumps({"UU": str(value)})},
                ):
                    with self._driver(self._server) as driver:
                        with driver.session("r") as session:
                            result = session.run("MATCH (n:Thing) RETURN n")
                            records = list(result)
                            self._server.done()
                            self.assertEqual(len(records), 1)
                            node = records[0].values[0]
                            self.assertIsInstance(node, types.CypherNode)
                            self.assertEqual(
                                node.props.value["uid"],
                                types.CypherUUID(value),
                            )
                            self.assertEqual(len(node.props.value), 1)


class TestUuid6x0(_UuidTestCase):
    def _bolt_version(self):
        return "6.0"

    required_features = (
        *_UuidTestCase.required_features,
        types.Feature.BOLT_6_0,
    )

    def test_rejects_uuid_parameter(self):
        with self._started_server(self._server, "anything_goes.script"):
            with self._driver(self._server) as driver:
                with driver.session("r") as session:
                    with self.assertRaises(types.DriverError) as exc:
                        session.run(
                            "RETURN $value AS value",
                            params={"value": types.CypherUUID(uuid.uuid4())},
                        ).consume()
                    self._server.done()
                    self.assertIn("uuid", exc.exception.msg.lower())

    def test_rejects_uuid_result(self):
        with self._started_server(self._server, "uuid_result.script"):
            with self._driver(self._server) as driver:
                with driver.session("r") as session:
                    with self.assertRaises(types.DriverError) as exc:
                        list(session.run("RETURN uuid()"))
                    # drivers don't need to handle the connection gracefully
                    self._server.reset()

        driver_name = get_driver_name()
        msg = exc.exception.msg.lower()
        if driver_name in ["python"]:
            self.assertIn("packstream", msg)
            self.assertIn("e0", msg)  # UUID PackStream type marker byte
        elif driver_name in ["dotnet"]:
            self.assertIn("uuid", msg)
            self.assertIn("6.1", msg)  # demands bolt 6.1
        elif driver_name in ["javascript"]:
            self.assertIn("unknown packed", msg)
            self.assertIn("e0", msg)  # UUID PackStream type marker byte
        elif driver_name in ["java", "ruby"]:
            self.assertIn("unknown packstream", msg)
            self.assertIn("e0", msg)  # UUID PackStream type marker byte
        elif driver_name in ["go"]:
            self.assertIn("packstream", msg)
            self.assertIn("e0", msg)  # UUID PackStream type marker byte
        else:
            raise NotImplementedError(f"Add error assertion for {driver_name}")
