import nutkit.protocol as types
from nutkit.frontend import (
    BookmarkManager,
    Driver,
    Neo4jBookmarkManagerConfig,
)
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestDriverExecuteQuery(TestkitTestCase):
    required_features = (
        types.Feature.BOLT_5_0,
        types.Feature.API_BOOKMARK_MANAGER,
        types.Feature.API_DRIVER_EXECUTE_QUERY
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

        eager_result = self._driver.execute_query("RETURN 1 AS n")

        self._assert_eager_result(
            eager_result,
            keys=["n"],
            records=[types.Record(values=[types.CypherInt(1)])],
            query_type="r"
        )

    def test_execute_query_without_config(self):
        self._start_server(self._router, "router.script")
        self._start_server(self._writer, "tx_return_1_with_params.script")
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query(
            "RETURN $a AS n",
            params={"a": types.CypherInt(1)}
        )

        self._assert_eager_result(
            eager_result,
            keys=["n"],
            records=[types.Record(values=[types.CypherInt(1)])],
            query_type="r"
        )

    def test_configure_routing_to_writers(self):
        self._start_server(self._router, "router.script")
        self._start_server(self._writer, "tx_return_1_with_params.script")
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query(
            "RETURN $a AS n",
            params={"a": types.CypherInt(1)},
            routing="w"
        )

        self._assert_eager_result(
            eager_result,
            keys=["n"],
            records=[types.Record(values=[types.CypherInt(1)])],
            query_type="r"
        )

    def test_configure_routing_to_readers(self):
        self._start_server(self._router, "router.script")
        self._start_server(self._reader, "tx_return_1_with_params.script")
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query(
            "RETURN $a AS n",
            params={"a": types.CypherInt(1)},
            routing="r"
        )

        self._assert_eager_result(
            eager_result,
            keys=["n"],
            records=[types.Record(values=[types.CypherInt(1)])],
            query_type="r"
        )

    def test_configure_database(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(
            self._writer, "tx_return_1_with_params.script", database="neo4j"
        )
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query(
            "RETURN $a AS n",
            params={"a": types.CypherInt(1)},
            database="neo4j"
        )

        self._assert_eager_result(
            eager_result,
            keys=["n"],
            records=[types.Record(values=[types.CypherInt(1)])],
            query_type="r"
        )

    def test_configure_impersonated_user(self):
        self._start_server(self._router, "router_with_impersonation.script")
        self._start_server(
            self._writer, "tx_return_1_with_impersonation.script"
        )
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query(
            "RETURN 1 AS n",
            impersonated_user="that-other-dude"
        )

        self._assert_eager_result(
            eager_result,
            keys=["n"],
            records=[types.Record(values=[types.CypherInt(1)])],
            query_type="r"
        )

    def test_configure_auth(self):
        self._start_server(
            self._writer, "tx_return_1_with_auth.script"
        )
        self._driver = self._new_driver(False)

        self._driver.execute_query(
            "RETURN 1 AS n",
            database="adb",
            auth=types.AuthorizationToken(
                "basic", principal="neo5j",
                credentials="pass++"
            )
        )
        self._writer.done()

    def test_configure_re_auth(self):
        self._start_server(
            self._writer, "tx_return_1_with_re_auth.script"
        )
        self._driver = self._new_driver(False)

        self._driver.execute_query(
            "RETURN 1 AS n",
            database="adb",
            auth=types.AuthorizationToken(
                "basic", principal="neo5j",
                credentials="pass++"
            )
        )
        self._writer.done()

    def test_configure_transaction_metadata(self):
        self._start_server(self._router, "router.script")
        self._start_server(
            self._writer, "tx_return_1_with_tx_meta.script"
        )
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query(
            "RETURN 1 AS n",
            tx_meta={"foo": types.CypherString("bar")}
        )

        self._assert_eager_result(
            eager_result,
            keys=["n"],
            records=[types.Record(values=[types.CypherInt(1)])],
            query_type="r"
        )

    def test_configure_transaction_timeout(self):
        self._start_server(self._router, "router.script")
        self._start_server(
            self._writer, "tx_return_1_with_tx_timeout.script"
        )
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query(
            "RETURN 1 AS n",
            timeout=1234,
        )

        self._assert_eager_result(
            eager_result,
            keys=["n"],
            records=[types.Record(values=[types.CypherInt(1)])],
            query_type="r"
        )

    def test_causal_consistency_between_query_executions(self):
        self._start_server(self._router, "router.script")
        self._start_server(
            self._writer, "transaction_chaining.script"
        )
        self._driver = self._new_driver()

        # CREATING NODE
        eager_result = self._driver.execute_query(
            "CREATE (p:Person {name:$name}) RETURN p.name AS name",
            params={"name": types.CypherString("the person")}
        )

        self._assert_eager_result(
            eager_result,
            keys=["name"],
            records=[types.Record(values=[types.CypherString("the person")])],
            query_type="w"
        )

        # READING SAME NODE
        eager_result2 = self._driver.execute_query(
            "MATCH (p:Person {name:$name}) RETURN p.name AS name",
            params={"name": types.CypherString("the person")}
        )

        self._assert_eager_result(
            eager_result2,
            keys=["name"],
            records=[types.Record(values=[types.CypherString("the person")])],
            query_type="r"
        )

    def test_disable_bookmark_manager(self):
        self._start_server(self._router, "router.script")
        self._start_server(
            self._writer, "transaction_chaining.script"
        )
        self._driver = self._new_driver()

        # CREATING NODE
        eager_result = self._driver.execute_query(
            "CREATE (p:Person {name:$name}) RETURN p.name AS name",
            params={"name": types.CypherString("the person")},
            bookmark_manager=None
        )

        self._assert_eager_result(
            eager_result,
            keys=["name"],
            records=[types.Record(values=[types.CypherString("the person")])],
            query_type="w"
        )

        # READING SAME NODE
        eager_result2 = self._driver.execute_query(
            "MATCH (p:Person {name:$name}) RETURN p.name AS name",
            params={"name": types.CypherString("the person")},
            bookmark_manager=None
        )

        self._assert_eager_result(
            eager_result2,
            keys=["name"],
            records=[],
            query_type="r"
        )

    def test_configure_custom_bookmark_manager(self):
        self._start_server(self._router, "router.script")
        self._start_server(
            self._writer, "transaction_chaining_custom_bmm.script"
        )
        bookmark_manager = BookmarkManager(
            self._backend,
            config=Neo4jBookmarkManagerConfig(
                initial_bookmarks=["bm1"]
            )
        )
        self._driver = self._new_driver()

        # CREATING NODE
        eager_result = self._driver.execute_query(
            "CREATE (p:Person {name:$name}) RETURN p.name AS name",
            params={"name": types.CypherString("a person")},
            bookmark_manager=bookmark_manager
        )

        self._assert_eager_result(
            eager_result,
            keys=["name"],
            records=[types.Record(values=[types.CypherString("a person")])],
            query_type="w"
        )

        # READING SAME NODE
        eager_result2 = self._driver.execute_query(
            "MATCH (p:Person {name:$name}) RETURN p.name AS name",
            params={"name": types.CypherString("a person")},
            bookmark_manager=bookmark_manager
        )

        self._assert_eager_result(
            eager_result2,
            keys=["name"],
            records=[types.Record(values=[types.CypherString("a person")])],
            query_type="r"
        )

        bookmark_manager.close()

    def test_retry_on_retryable_error(self):
        self._start_server(
            self._router, "router_invert_reader_and_writer_second_call.script"
        )
        self._start_server(
            self._writer, "tx_return_1_disconnect_on_pull.script"
        )
        self._start_server(self._reader, "tx_return_1.script")
        self._driver = self._new_driver()

        eager_result = self._driver.execute_query("RETURN 1 AS n")

        self.assertEqual(["n"], eager_result.keys)
        self.assertEqual([types.Record(values=[types.CypherInt(1)])],
                         eager_result.records)
        self.assertIsNotNone(eager_result.summary)

    def test_thrown_non_retryable_error(self):
        self._start_server(self._router, "router.script")
        self._start_server(
            self._writer, "tx_return_1_transaction_terminated_on_pull.script"
        )

        self._driver = self._new_driver()

        with self.assertRaises(types.DriverError) as exc:
            self._driver.execute_query("RETURN 1 AS n")

        self.assertEqual(exc.exception.code,
                         "Neo.ClientError.Transaction.Terminated")

    def _assert_eager_result(self, eager_result, *, keys, records, query_type):
        self.assertEqual(keys, eager_result.keys)
        self.assertEqual(records, eager_result.records)
        self.assertIsNotNone(eager_result.summary)
        summary = eager_result.summary
        self.assertEqual("Neo4j/5.0.0", summary.server_info.agent)
        self.assertEqual("5.0", summary.server_info.protocol_version)
        self.assertEqual(query_type, summary.query_type)

    def _start_server(self, server, script, database=""):
        server.start(self.script_path(script), vars_={
            "#HOST#": self._router.host,
            "#DB#": database
        })

    def _new_driver(self, routing=True):
        if routing:
            uri = "neo4j://%s" % self._router.address
        else:
            uri = "bolt://%s" % self._writer.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        driver = Driver(self._backend, uri, auth)
        return driver
