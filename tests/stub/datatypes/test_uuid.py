import json
import uuid
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestUuid(TestkitTestCase):
    required_features = (
        types.Feature.API_TYPE_UUID,
        types.Feature.BOLT_6_1,
    )
    bolt_version = "6.1"

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    @contextmanager
    def _started_server(self, server, script, vars_=None):
        version_folder = "v{}".format(self.bolt_version.replace(".", "x"))
        server.start(
            path=self.script_path(version_folder, script),
            vars_=vars_,
        )
        try:
            yield
        finally:
            server.reset()

    def _driver(self, server):
        uri = "bolt://%s" % server.address
        auth = types.AuthorizationToken("basic", principal="", credentials="")
        return Driver(self._backend, uri, auth)

    def test_uuid(self):
        script = "echo_uuid.script"
        for value in (
            uuid.UUID("00000000000000000000000000000000"),
            uuid.UUID("ffffffffffffffffffffffffffffffff"),
            uuid.UUID("0102030405060708090a0b0c0d0e0f10"),
            uuid.uuid4(),
        ):
            with self.subTest(value=str(value)):
                with self._started_server(
                    self._server,
                    script,
                    vars_={
                        "#UUID#": json.dumps({"UU": str(value)}),
                    },
                ):
                    with self._driver(self._server) as driver:
                        with driver.session("r") as session:
                            cypher_uuid = types.CypherUUID(value)
                            result = session.run(
                                "RETURN $uuid AS uuid",
                                params={"uuid": cypher_uuid},
                            )
                            records = list(result)
                            self._server.done()
                            self.assertEqual(len(records), 1)
                            fields = records[0].values
                            self.assertEqual(len(fields), 1)
                            self.assertEqual(cypher_uuid, fields[0])


    def test_uuid_in_list(self):
        script = "echo_uuid_list.script"
        uuid_pairs = [
            (
                uuid.UUID("00000000000000000000000000000000"), 
                uuid.UUID("ffffffffffffffffffffffffffffffff")
            ),
            (uuid.UUID("0102030405060708090a0b0c0d0e0f10"), uuid.uuid4()),
        ]
        for a, b in uuid_pairs:
            with self.subTest(a=str(a), b=str(b)):
                jolt_list = json.dumps(
                    [{"UU": str(a)}, {"UU": str(b)}]
                )
                with self._started_server(
                    self._server,
                    script,
                    vars_={"#UUIDS#": jolt_list},
                ):
                    with self._driver(self._server) as driver:
                        with driver.session("r") as session:
                            cypher_uuids = types.CypherList(
                                [types.CypherUUID(a), types.CypherUUID(b)]
                            )
                            result = session.run(
                                "RETURN $uuids AS uuids",
                                params={"uuids": cypher_uuids},
                            )
                            records = list(result)
                            self._server.done()
                            self.assertEqual(len(records), 1)
                            fields = records[0].values
                            self.assertEqual(len(fields), 1)
                            self.assertEqual(cypher_uuids, fields[0])

    def test_uuid_as_node_property(self):
        script = "uuid_node_property.script"
        for value in (
            uuid.UUID("00000000000000000000000000000000"),
            uuid.UUID("ffffffffffffffffffffffffffffffff"),
            uuid.UUID("0102030405060708090a0b0c0d0e0f10"),
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
                                node.props["uid"], types.CypherUUID(value)
                            )
