from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from nutkit.protocol import (
    CypherInt,
    CypherList,
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
        self._session = None

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def _assert_element_id(self, expected, actual):
        # TODO: can be removed once all official drivers support new
        #       TestKit protocol
        if self.driver_supports_features(
            types.Feature.BOLT_5_0
        ):
            self.assertEqual(expected, actual)

    def _validate_invalid_operation(self, exc):
        if get_driver_name() in ["dotnet"]:
            self.assertEqual("InvalidOperationException",
                             exc.exception.errorType)
        if get_driver_name() in ["java"]:
            self.assertEqual("java.lang.IllegalStateException",
                             exc.exception.errorType)

    @contextmanager
    def _get_session(self, script_path, vars_=None):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path(script_path), vars_=vars_)
        session = driver.session("r", fetch_size=1)
        try:
            yield session
        finally:
            session.close()
            driver.close()
            self._server.reset()

    @driver_feature(types.Feature.BOLT_4_4)
    def test_4x4_populates_node_element_id_with_id(self):
        script_params = {
            "#BOLT_PROTOCOL#": "4.4",
            "#RESULT#": '{"()": [123, ["l1", "l2"], {"a": {"Z": "42"}}]}'
        }

        with self._get_session("single_result.script",
                               script_params) as session:
            result_handle = session.run("MATCH (n) RETURN n LIMIT 1")

            node = result_handle.next()

            self.assertEqual(CypherInt(123), node.values[0].id)
            self._assert_element_id(CypherString("123"), node.values[0]
                                    .elementId)

            self._server.done()

    @driver_feature(types.Feature.BOLT_5_0)
    def test_5x0_populates_node_element_id_with_string(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"()": [123, ["l1", "l2"], {"a": {"Z": "42"}}, "n1-123"]}'
        }
        with self._get_session("single_result.script",
                               script_params) as session:
            result_handle = session.run("MATCH (n) RETURN n LIMIT 1")

            node = result_handle.next()

            self.assertEqual(CypherInt(123), node.values[0].id)
            self.assertEqual(CypherString("n1-123"), node.values[0].elementId)

            self._server.done()

    @driver_feature(types.Feature.BOLT_4_4)
    def test_4x4_populates_rel_element_id_with_id(self):
        script_params = {
            "#BOLT_PROTOCOL#": "4.4",
            "#RESULT#": '{"->": [123, 1, "f", 2, {"a": {"Z": "42"}}]}'
        }
        with self._get_session("single_result.script",
                               script_params) as session:
            result_handle = session.run("MATCH ()-[r:]-() RETURN r LIMIT 1")

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

            self._server.done()

    @driver_feature(types.Feature.BOLT_5_0)
    def test_5x0_populates_rel_element_id_with_string(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"->": [123, 1, "f", 2, {"a": {"Z": "42"}}, "r1-123", '
                '"n1-1", "n1-2"]}'
        }
        with self._get_session("single_result.script",
                               script_params) as session:
            result_handle = session.run("MATCH ()-[r:]-() RETURN r LIMIT 1")

            relationship = result_handle.next()

            self.assertEqual(CypherInt(123), relationship.values[0].id)
            self.assertEqual(CypherString("r1-123"),
                             relationship.values[0].elementId)
            self.assertEqual(CypherInt(1), relationship.values[0].startNodeId)
            self.assertEqual(CypherInt(2), relationship.values[0].endNodeId)
            self.assertEqual(CypherString("n1-1"),
                             relationship.values[0].startNodeElementId)
            self.assertEqual(CypherString("n1-2"),
                             relationship.values[0].endNodeElementId)

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
        with self._get_session("single_result.script",
                               script_params) as session:

            result_handle = session.run("MATCH p = ()--()--() "
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
            self._assert_element_id(CypherString("1"),
                                    rels[0].startNodeElementId)
            self._assert_element_id(CypherString("3"),
                                    rels[0].endNodeElementId)

            self._server.done()

    @driver_feature(types.Feature.BOLT_5_0)
    def test_5x0_populates_path_element_ids_with_string(self):
        script_params = {
            "#BOLT_PROTOCOL#": "5.0",
            "#RESULT#":
                '{"..": ['
                '{"()": [1, ["l"], {}, "n1-1"]}, '
                '{"->": [2, 1, "RELATES_TO", 3, {}, "r1-2", "n1-1", "n1-3"]}, '
                '{"()": [3, ["l"], {}, "n1-3"]}, '
                '{"->": [4, 3, "RELATES_TO", 1, {}, "r1-4", "n1-3", "n1-1"]}, '
                '{"()": [1, ["l"], {}, "n1-1"]}'
                ']}'  # noqa: Q000
        }
        with self._get_session("single_result.script",
                               script_params) as session:
            result_handle = session.run("MATCH p = ()--()--() "
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
            self.assertEqual(CypherString("n1-1"), nodes[0].elementId)
            # rel ids
            self.assertEqual(CypherInt(2), rels[0].id)
            self.assertEqual(CypherString("r1-2"), rels[0].elementId)
            # rel start/end ids
            self.assertEqual(CypherInt(1), rels[0].startNodeId)
            self.assertEqual(CypherInt(3), rels[0].endNodeId)
            self.assertEqual(CypherString("n1-1"), rels[0].startNodeElementId)
            self.assertEqual(CypherString("n1-3"), rels[0].endNodeElementId)

            self._server.done()
