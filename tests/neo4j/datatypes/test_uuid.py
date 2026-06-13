import uuid

import nutkit.protocol as types
from tests.neo4j.datatypes._base import _TestTypesBase


class TestUuidTypes(_TestTypesBase):

    required_features = (
        types.Feature.API_TYPE_UUID,
        types.Feature.BOLT_6_1,
    )

    def test_should_echo_uuid(self):
        values = [
            uuid.UUID("00000000-0000-0000-0000-000000000000"),  # nil UUID
            uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),  # all-ones
            uuid.UUID("01020304-0506-0708-090a-0b0c0d0e0f12"),  # sequential
            uuid.uuid4(),
        ]
        self._create_driver_and_session()
        for value in values:
            with self.subTest(value=str(value)):
                self._verify_can_echo(types.CypherUUID(value))

    def test_uuid_in_list(self):
        self._create_driver_and_session()
        nil = uuid.UUID("00000000-0000-0000-0000-000000000000")
        ones = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
        data = types.CypherList([
            types.CypherUUID(nil),
            types.CypherUUID(ones),
            types.CypherUUID(uuid.uuid4()),
        ])
        self._verify_can_echo(data)

    def test_uuid_in_map(self):
        value = uuid.uuid4()
        self._create_driver_and_session()
        self._verify_can_echo(
            types.CypherMap({"id": types.CypherUUID(value)})
        )

    def test_cypher_created_uuid(self):
        self._create_driver_and_session()
        values = self._read_query_values("RETURN uuid()")
        self.assertEqual(len(values), 1)
        self.assertIsInstance(values[0], types.CypherUUID)

    def test_uuid_stored_on_node(self):
        uid = uuid.uuid4()
        self._create_driver_and_session()
        values = self._write_query_values(
            "CREATE (n:Thing {uid: $uid}) RETURN n.uid",
            params={"uid": types.CypherUUID(uid)},
        )
        self.assertEqual(values, [types.CypherUUID(uid)])
