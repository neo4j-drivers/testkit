from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestDriverExecuteQuery(TestkitTestCase):
    required_features = (
        types.Feature.BOLT_5_0,
        types.Feature.API_BOOKMARK_MANAGER,
    )

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader = StubServer(9010)
        self._writer = StubServer(9020)
        self._driver = None

    def tearDown(self):
        if self._driver:
            self._driver.close()

        self._router.reset()
        self._reader.reset()
        self._writer.reset()
        return super().tearDown()

    def test_execute_query_without_params_and_config(self):
        self._start_server(self._router, "router.script")
        self._start_server(self._writer, "tx_return_1.script")
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query("RETURN 1 as n")

        self.assertEqual(eager_result.keys, ["n"])
        self.assertEqual(eager_result.records, [
                         types.Record(values=[types.CypherInt(1)])])
        self.assertIsNotNone(eager_result.summary)

    def test_execute_query_without_config(self):
        self._start_server(self._router, "router.script")
        self._start_server(self._writer, "tx_return_1_with_params.script")
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query("RETURN 1 as n", {
            "a": types.CypherInt(1)
        })

        self.assertEqual(eager_result.keys, ["n"])
        self.assertEqual(eager_result.records, [
                         types.Record(values=[types.CypherInt(1)])])
        self.assertIsNotNone(eager_result.summary)

    def test_configure_routing_to_writers(self):
        self._start_server(self._router, "router.script")
        self._start_server(self._writer, "tx_return_1_with_params.script")
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query("RETURN 1 as n", {
            "a": types.CypherInt(1)
        }, {"routing": "W"})

        self.assertEqual(eager_result.keys, ["n"])
        self.assertEqual(eager_result.records, [
                         types.Record(values=[types.CypherInt(1)])])
        self.assertIsNotNone(eager_result.summary)

    def test_configure_routing_to_readers(self):
        self._start_server(self._router, "router.script")
        self._start_server(self._reader, "tx_return_1_with_params.script")
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query("RETURN 1 as n", {
            "a": types.CypherInt(1)
        }, {"routing": "R"})

        self.assertEqual(eager_result.keys, ["n"])
        self.assertEqual(eager_result.records, [
                         types.Record(values=[types.CypherInt(1)])])
        self.assertIsNotNone(eager_result.summary)

    def test_configure_database(self):
        pass

    def test_configure_impersonated_user(self):
        pass

    def test_causal_consistency_between_query_executions(self):
        pass

    def test_disable_bookmark_manager(self):
        pass

    def test_configure_custom_bookmark_manager(self):
        pass

    def _start_server(self, server, script):
        server.start(self.script_path(script),
                     vars_={"#HOST#": self._router.host})

    def _new_driver(self):
        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        driver = Driver(
            self._backend,
            uri, auth
        )
        return driver
