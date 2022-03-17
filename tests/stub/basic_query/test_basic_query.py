from nutkit import protocol as types
from nutkit.frontend import Driver
from nutkit.protocol import (
    CypherInt,
    CypherList,
    CypherNull,
    CypherPath,
    CypherString,
)
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestBasicQuery(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic", principal="",
                                                       credentials=""))
        self._session = None

    def tearDown(self):
        if self._session is not None:
            self._session.close()
        self._server.reset()
        super().tearDown()

    def _assert_is_unset_id(self, id_):
        if self.driver_supports_features(
            types.Feature.DETAIL_THROW_ON_MISSING_ID
        ):
            return
        if self.driver_supports_features(
            types.Feature.DETAIL_NULL_ON_MISSING_ID
        ):
            self.assertEqual(CypherNull(), id_)
        else:
            self.assertEqual(CypherInt(-1), id_)

    def _assert_element_id(self, expected, actual):
        # TODO: can be removed once all official drivers support new
        #       TestKit protocol
        if self.driver_supports_features(
            types.Feature.BOLT_5_0
        ):
            self.assertEqual(expected, actual)

    @driver_feature(types.Feature.BOLT_4_4)
    def test_4x4_populates_node_element_id_with_id(self):
        script_params = {
            "#BOLT_PROTOCOL#": "4.4",
            "#RESULT#": '{"()": [123, ["l1", "l2"], {"a": {"Z": "42"}}]}'
        }
        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )

        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH (n) RETURN n LIMIT 1")

        node = result_handle.next()

        self.assertEqual(CypherInt(123), node.values[0].id)
        self._assert_element_id(CypherString("123"), node.values[0].elementId)

        self._session.close()
        self._session = None
        self._server.done()

    @driver_feature(types.Feature.BOLT_5_0)
    def test_5x0_populates_node_element_id_with_string(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"()": [123, ["l1", "l2"], {"a": {"Z": "42"}}, "123"]}'
        }
        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )

        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH (n) RETURN n LIMIT 1")

        node = result_handle.next()

        self.assertEqual(CypherInt(123), node.values[0].id)
        self.assertEqual(CypherString("123"), node.values[0].elementId)

        self._session.close()
        self._session = None
        self._server.done()

    @driver_feature(types.Feature.BOLT_5_0)
    def test_5x0_populates_node_only_element_id(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"()": [null, ["l1", "l2"], {"a": {"Z": "42"}}, "n1-123"]}'
        }

        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )

        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH (n) RETURN n LIMIT 1")

        node = result_handle.next()

        self._assert_is_unset_id(node.values[0].id)
        self.assertEqual(CypherString("n1-123"), node.values[0].elementId)

        self._session.close()
        self._session = None
        self._server.done()

    @driver_feature(types.Feature.BOLT_5_0,
                    types.Feature.DETAIL_THROW_ON_MISSING_ID)
    def test_5x0_throws_on_node_access_id(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"()": [null, ["l1", "l2"], {"a": {"Z": "42"}}, "n1-123"]}'
        }

        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )

        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH (n) RETURN n LIMIT 1")

        with self.assertRaises(types.DriverError) as exc:
            result_handle.read_cypher_type_field("n", "node", "id")

        if get_driver_name() in ["dotnet"]:
            self.assertEqual("InvalidOperationException",
                             exc.exception.errorType)

        self._session.close()
        self._session = None
        self._server.done()

    @driver_feature(types.Feature.BOLT_4_4)
    def test_4x4_populates_rel_element_id_with_id(self):
        script_params = {
            "#BOLT_PROTOCOL#": "4.4",
            "#RESULT#": '{"->": [123, 1, "f", 2, {"a": {"Z": "42"}}]}'
        }

        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )
        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH ()-[r:]-() RETURN r LIMIT 1")

        relationship = result_handle.next()

        self.assertEqual(CypherInt(123), relationship.values[0].id)
        self._assert_element_id(CypherString("123"),
                                relationship.values[0].elementId)
        self.assertEqual(CypherInt(1), relationship.values[0].startNodeId)
        self.assertEqual(CypherInt(2), relationship.values[0].endNodeId)
        self._assert_element_id(CypherString("1"),
                                relationship.values[0].startNodeElementId)
        self._assert_element_id(CypherString("2"),
                                relationship.values[0].endNodeElementId)

        self._session.close()
        self._session = None
        self._server.done()

    @driver_feature(types.Feature.BOLT_5_0)
    def test_5x0_populates_rel_element_id_with_string(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"->": [123, 1, "f", 2, {"a": {"Z": "42"}}, "123", '
                '"1", "2"]}'
        }
        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )
        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH ()-[r:]-() RETURN r LIMIT 1")

        relationship = result_handle.next()

        self.assertEqual(CypherInt(123), relationship.values[0].id)
        self.assertEqual(CypherString("123"),
                         relationship.values[0].elementId)
        self.assertEqual(CypherInt(1), relationship.values[0].startNodeId)
        self.assertEqual(CypherInt(2), relationship.values[0].endNodeId)
        self.assertEqual(CypherString("1"),
                         relationship.values[0].startNodeElementId)
        self.assertEqual(CypherString("2"),
                         relationship.values[0].endNodeElementId)

        self._session.close()
        self._session = None
        self._server.done()

    @driver_feature(types.Feature.BOLT_5_0)
    def test_5x0_populates_rel_only_element_id(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"->": [null, null, "f", null, {"a": {"Z": "42"}}, "r1-123", '
                '"n1-1", "n1-2"]}'
        }

        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )
        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH (n) RETURN n LIMIT 1")

        relationship = result_handle.next()

        self._assert_is_unset_id(relationship.values[0].id)
        self.assertEqual(CypherString("r1-123"),
                         relationship.values[0].elementId)

        self._assert_is_unset_id(relationship.values[0].startNodeId)
        self._assert_is_unset_id(relationship.values[0].endNodeId)
        self.assertEqual(CypherString("n1-1"),
                         relationship.values[0].startNodeElementId)
        self.assertEqual(CypherString("n1-2"),
                         relationship.values[0].endNodeElementId)

        self._session.close()
        self._session = None
        self._server.done()

    @driver_feature(types.Feature.BOLT_4_4)
    def test_4x4_populates_path_element_ids_with_long(self):
        script_params = {
            "#BOLT_PROTOCOL#": "4.4",
            "#RESULT#":
                '{"..": ['
                '{"()": [1, ["l"], {}]}, '
                '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
                '{"()": [3, ["l"], {}]}, '
                '{"->": [4, 3, "RELATES_TO", 1, {}]}, '
                '{"()": [1, ["l"], {}]}'
                ']}'  # noqa: Q000
        }

        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )
        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH p = ()--()--() "
                                          "RETURN p LIMIT 1")
        result = result_handle.next()
        self.assertIsInstance(result.values[0], CypherPath)
        path = result.values[0]
        self.assertIsInstance(path.nodes, CypherList)
        self.assertIsInstance(path.relationships, CypherList)
        nodes = path.nodes.value
        rels = path.relationships.value
        # node ids
        self.assertEqual(CypherInt(1), nodes[0].id)
        self._assert_element_id(CypherString("1"), nodes[0].elementId)
        # rel ids
        self.assertEqual(CypherInt(2), rels[0].id)
        self._assert_element_id(CypherString("2"), rels[0].elementId)
        # rel start/end ids
        self.assertEqual(CypherInt(1), rels[0].startNodeId)
        self.assertEqual(CypherInt(3), rels[0].endNodeId)
        self._assert_element_id(CypherString("1"), rels[0].startNodeElementId)
        self._assert_element_id(CypherString("3"), rels[0].endNodeElementId)
        self._session.close()
        self._session = None
        self._server.done()

    @driver_feature(types.Feature.BOLT_5_0)
    def test_5x0_populates_path_element_ids_with_string(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"..": ['
                '{"()": [1, ["l"], {}, "1"]}, '
                '{"->": [2, 1, "RELATES_TO", 3, {}, "2", "1", "3"]}, '
                '{"()": [3, ["l"], {}, "3"]}, '
                '{"->": [4, 3, "RELATES_TO", 1, {}, "4", "3", "1"]}, '
                '{"()": [1, ["l"], {}, "1"]}'
                ']}'  # noqa: Q000
        }

        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )
        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH p = ()--()--() "
                                          "RETURN p LIMIT 1")

        result = result_handle.next()

        self.assertIsInstance(result.values[0], CypherPath)
        path = result.values[0]
        self.assertIsInstance(path.nodes, CypherList)
        self.assertIsInstance(path.relationships, CypherList)
        nodes = path.nodes.value
        rels = path.relationships.value
        # node ids
        self.assertEqual(CypherInt(1), nodes[0].id)
        self.assertEqual(CypherString("1"), nodes[0].elementId)
        # rel ids
        self.assertEqual(CypherInt(2), rels[0].id)
        self.assertEqual(CypherString("2"), rels[0].elementId)
        # rel start/end ids
        self.assertEqual(CypherInt(1), rels[0].startNodeId)
        self.assertEqual(CypherInt(3), rels[0].endNodeId)
        self.assertEqual(CypherString("1"), rels[0].startNodeElementId)
        self.assertEqual(CypherString("3"), rels[0].endNodeElementId)

        self._session.close()
        self._session = None
        self._server.done()

    @driver_feature(types.Feature.BOLT_5_0)
    def test_5x0_populates_path_element_ids_with_only_string(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"..": ['
                '{"()": [null, ["l"], {}, "n1-1"]}, '
                '{"->": [null, null, "RELATES_TO", null, {}, '
                '"r1-2", "n1-1", "n1-3"]}, '
                '{"()": [null, ["l"], {}, "n1-3"]}, '
                '{"->": [null, null, "RELATES_TO", null, '
                '{}, "r1-4", "n1-3", "n1-1"]}, '
                '{"()": [null, ["l"], {}, "n1-1"]}'
                ']}'  # noqa: Q000
        }

        self._server.start(
            path=self.script_path("single_result.script"),
            vars_=script_params
        )
        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH p = ()--()--() "
                                          "RETURN p LIMIT 1")

        result = result_handle.next()

        self.assertIsInstance(result.values[0], CypherPath)
        path = result.values[0]
        self.assertIsInstance(path.nodes, CypherList)
        self.assertIsInstance(path.relationships, CypherList)
        nodes = path.nodes.value
        rels = path.relationships.value
        # node ids
        self._assert_is_unset_id(nodes[0].id)
        self.assertEqual(CypherString("n1-1"), nodes[0].elementId)
        # rel ids
        self._assert_is_unset_id(rels[0].id)
        self.assertEqual(CypherString("r1-2"), rels[0].elementId)
        # rel start/end ids
        self._assert_is_unset_id(rels[0].startNodeId)
        self._assert_is_unset_id(rels[0].endNodeId)
        self.assertEqual(CypherString("n1-1"), rels[0].startNodeElementId)
        self.assertEqual(CypherString("n1-3"), rels[0].endNodeElementId)

        self._session.close()
        self._session = None
        self._server.done()
