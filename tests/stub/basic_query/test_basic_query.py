from nutkit import protocol as types
from nutkit.frontend import Driver
from nutkit.protocol import CypherInt, CypherString
from tests.shared import (
    get_driver_name,
    TestkitTestCase
)
from tests.stub.shared import StubServer


class TestBasicQuery(TestkitTestCase):
    # required_features = types.Feature.BOLT_5_0

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

    def test_4x4_populates_element_id_with_id(self):
        self._server.start(
            path=self.script_path("4_4_protocol_one_node_query.script")
        )

        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH (n) RETURN n LIMIT 1")

        node = result_handle.next()

        self.assertEqual(CypherInt(123), node.values[0].id)
        self.assertEqual(CypherString("123"), node.values[0].elementId)

        self._session.close()
        self._session = None
        self._server.done()

    def test_5x0_populates_element_id_with_string(self):
        self._server.start(
            path=self.script_path("5_0_protocol_one_node_query.script")
        )

        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH (n) RETURN n LIMIT 1")

        node = result_handle.next()

        self.assertEqual(CypherInt(123), node.values[0].id)
        self.assertEqual(CypherString("n1-123"), node.values[0].elementId)

        self._session.close()
        self._session = None
        self._server.done()

    def test_5x0_populates_only_element_id(self):
        self._server.start(
            path=self.script_path("5_0_protocol_freki_one_node_query.script")
        )

        self._session = self._driver.session("r", fetch_size=1)
        result_handle = self._session.run("MATCH (n) RETURN n LIMIT 1")

        node = result_handle.next()

        self.assertEqual(CypherInt(-1), node.values[0].id)
        self.assertEqual(CypherString("n1-123"), node.values[0].elementId)

        self._session.close()
        self._session = None
        self._server.done()
