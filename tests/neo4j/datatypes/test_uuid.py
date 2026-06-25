import uuid

import nutkit.protocol as types
from tests.neo4j.datatypes._base import _TestTypesBase
from tests.neo4j.shared import requires_min_bolt_version


class TestUuidTypes(_TestTypesBase):

    required_features = (
        types.Feature.API_TYPE_UUID,
        types.Feature.BOLT_6_1,
    )

    @requires_min_bolt_version("6.1")
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

    @requires_min_bolt_version("6.1")
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

    @requires_min_bolt_version("6.1")
    def test_uuid_in_map(self):
        value = uuid.uuid4()
        self._create_driver_and_session()
        self._verify_can_echo(
            types.CypherMap({"id": types.CypherUUID(value)})
        )

    @requires_min_bolt_version("6.1")
    def test_cypher_created_uuid(self):
        raw_uuids = [
            "00000000-0000-0000-0000-000000000000",  # nil UUID
            "ffffffff-ffff-ffff-ffff-ffffffffffff",  # all-ones
            "01020304-0506-0708-090a-0b0c0d0e0f12",  # sequential
        ]
        self._create_driver_and_session()
        values = self._read_query_values(
            "UNWIND $raw_uuids AS raw_uuid RETURN collect(uuid(raw_uuid))",
            {"raw_uuids": types.as_cypher_type(raw_uuids)},
        )
        self.assertEqual(len(values), 1)
        self.assertIsInstance(values, list)
        values = values[0].value
        for value, expected in zip(values, raw_uuids):
            self.assertEqual(value, types.CypherUUID(uuid.UUID(expected)))

    @requires_min_bolt_version("6.1")
    def test_uuid_stored_on_node(self):
        uid = uuid.uuid4()
        self._create_driver_and_session()
        values = self._write_query_values(
            "CREATE (n:Thing {uid: $uid}) RETURN n.uid",
            params={"uid": types.CypherUUID(uid)},
        )
        self.assertEqual(values, [types.CypherUUID(uid)])
