import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    get_dns_resolved_server_address,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class NoRoutingV4x1(TestkitTestCase):

    required_features = types.Feature.BOLT_4_1,
    bolt_version = "4.1"
    version_dir = "v4x1_no_routing"
    server_agent = "Neo4j/4.1.0"
    adb = "adb"

    def setUp(self):
        super().setUp()
        self._server = StubServer(9000)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def get_vars(self):
        # TODO: "#ROUTING#": "" is the correct way to go
        #       (minimal data transmission)
        routing = ""
        if get_driver_name() in ["dotnet"]:
            routing = ', "routing": null'
        return {
            "#VERSION#": self.bolt_version,
            "#SERVER_AGENT#": self.server_agent,
            "#USER_AGENT#": "007",
            "#ROUTING#": routing
        }

    # Checks that routing is disabled when URI is bolt, no routing in HELLO and
    # no call to retrieve routing table. From bolt >= 4.1 the routing context
    # is used to disable/enable server side routing.
    def test_should_read_successfully_using_read_session_run(self):
        # Driver is configured to talk to "routing" stub server
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(self.version_dir, "reader.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("r", database=self.adb)
        res = session.run("RETURN 1 as n")
        list(res)
        summary = res.consume()
        session.close()
        driver.close()

        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._server),
                         self._server.address])
        self._server.done()

    def test_should_read_successfully_using_write_session_run(self):
        # Driver is configured to talk to "routing" stub server
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(self.version_dir,
                                  "reader_write_mode.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb)
        res = session.run("RETURN 1 as n")
        list(res)
        summary = res.consume()
        session.close()
        driver.close()

        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._server),
                         self._server.address])
        self._server.done()

    def test_should_exclude_routing_context(self):
        # TODO remove this block once implemented
        if get_driver_name() in ["dotnet"]:
            self.skipTest("does not exclude routing context")
        uri = "bolt://%s" % self._server.address
        no_routing_context_vars = self.get_vars()
        no_routing_context_vars.update({
            "#ROUTING#": ""
        })
        self._server.start(
            path=self.script_path(self.version_dir,
                                  "reader_write_mode.script"),
            vars_=no_routing_context_vars
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb)
        res = session.run("RETURN 1 as n")
        list(res)
        summary = res.consume()
        session.close()
        driver.close()

        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._server),
                         self._server.address])
        self._server.done()

    def test_should_send_custom_user_agent_using_write_session_run(self):
        uri = "bolt://%s" % self._server.address
        custom_agent = "custom"
        custom_agent_context_vars = self.get_vars()
        custom_agent_context_vars.update({
            "#USER_AGENT#": custom_agent
        })
        self._server.start(
            path=self.script_path(self.version_dir,
                                  "reader_write_mode.script"),
            vars_=custom_agent_context_vars
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent=custom_agent)

        session = driver.session("w", database=self.adb)
        res = session.run("RETURN 1 as n")
        list(res)
        summary = res.consume()
        session.close()
        driver.close()

        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._server),
                         self._server.address])
        self._server.done()

    def test_should_error_on_rollback_failure_using_tx_rollback(self):
        # TODO There is a pending unification task to fix this.
        # Once fixed, this block should be removed.
        if get_driver_name() in ["javascript", "go"]:
            self.skipTest("There is a pending unification task to fix this.")
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "writer_yielding_db_unavailable_error_on_rollback.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        res = tx.run("RETURN 1 as n")
        list(res)
        summary = res.consume()

        with self.assertRaises(types.DriverError) as exc:
            tx.rollback()

        session.close()
        driver.close()

        self._assert_is_transient_rollback_exception(exc.exception)
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._server),
                         self._server.address])
        self._server.done()

    def test_should_error_on_rollback_failure_using_tx_close(self):
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "writer_yielding_db_unavailable_error_on_rollback.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        res = tx.run("RETURN 1 as n")
        summary = res.consume()

        with self.assertRaises(types.DriverError) as exc:
            tx.close()

        session.close()
        driver.close()

        self._assert_is_transient_rollback_exception(exc.exception)
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._server),
                         self._server.address])
        self._server.done()

    def test_should_error_on_rollback_failure_using_session_close(
            self):
        # TODO There is a pending unification task to fix this.
        # Once fixed, this block should be removed.
        if get_driver_name() in ["javascript", "go"]:
            self.skipTest("There is a pending unification task to fix this.")

        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "writer_yielding_db_unavailable_error_on_rollback.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        res = tx.run("RETURN 1 as n")
        list(res)
        summary = res.consume()

        with self.assertRaises(types.DriverError) as exc:
            session.close()

        driver.close()

        self._assert_is_transient_rollback_exception(exc.exception)
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._server),
                         self._server.address])
        self._server.done()

    def test_should_accept_custom_fetch_size_using_driver_configuration(
            self):
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(self.version_dir,
                                  "writer_with_custom_fetch_size.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007", fetch_size=2)

        session = driver.session("w", database=self.adb)
        res = session.run("RETURN 1 as n")
        records = list(res)

        session.close()
        driver.close()

        self.assertEqual([types.Record(values=[types.CypherInt(1)]),
                          types.Record(values=[types.CypherInt(3)]),
                          types.Record(values=[types.CypherInt(5)]),
                          types.Record(values=[types.CypherInt(7)]),
                          types.Record(values=[types.CypherInt(9)])], records)
        self._server.done()

    def test_should_accept_custom_fetch_size_using_session_configuration(
            self):
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(self.version_dir,
                                  "writer_with_custom_fetch_size.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb, fetch_size=2)
        res = session.run("RETURN 1 as n")
        records = list(res)

        session.close()
        driver.close()

        self.assertEqual([types.Record(values=[types.CypherInt(1)]),
                          types.Record(values=[types.CypherInt(3)]),
                          types.Record(values=[types.CypherInt(5)]),
                          types.Record(values=[types.CypherInt(7)]),
                          types.Record(values=[types.CypherInt(9)])], records)
        self._server.done()

    @driver_feature(types.Feature.API_RESULT_LIST)
    def test_should_pull_custom_size_and_then_all_using_session_configuration(
            self):
        uri = "bolt://%s" % self._server.address
        script = (
            "writer_with_custom_fetch_size_pull_all.script"
            if self.driver_supports_features(
                types.Feature.OPT_RESULT_LIST_FETCH_ALL
            )
            else "writer_with_custom_fetch_size.script"
        )
        self._server.start(
            path=self.script_path(self.version_dir, script),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb, fetch_size=2)
        res = session.run("RETURN 1 as n")
        records = res.list()

        session.close()
        driver.close()

        self.assertEqual(records, [types.Record(values=[types.CypherInt(i)])
                                   for i in (1, 3, 5, 7, 9)])
        self._server.done()

    def test_should_pull_all_when_fetch_is_minus_one_using_driver_configuration(  # noqa: E501
            self):
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(self.version_dir,
                                  "writer_yielding_multiple_records.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007", fetch_size=-1)

        session = driver.session("w", database=self.adb)
        res = session.run("RETURN 1 as n")
        records = list(res)

        session.close()
        driver.close()

        self.assertEqual([types.Record(values=[types.CypherInt(1)]),
                          types.Record(values=[types.CypherInt(5)]),
                          types.Record(values=[types.CypherInt(7)])], records)
        self._server.done()

    def test_should_error_on_commit_failure_using_tx_commit(self):
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "writer_yielding_db_unavailable_error_on_commit.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        res = tx.run("RETURN 1 as n")
        list(res)
        summary = res.consume()

        with self.assertRaises(types.DriverError) as exc:
            tx.commit()

        session.close()
        driver.close()

        self._assert_is_transient_commit_exception(exc.exception)
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._server),
                         self._server.address])
        self._server.done()

    def test_should_check_multi_db_support(self):
        # TODO remove this block once all drivers support this
        if get_driver_name() in ["go"]:
            self.skipTest("Does not support CheckMultiDBSupport request")
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(self.version_dir, "optional_hello.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        supports_multi_db = driver.supports_multi_db()

        driver.close()

        self._assert_supports_multi_db(supports_multi_db=supports_multi_db)
        self._server.done()

    def test_should_accept_noop_during_records_streaming(
            self):
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "writer_yielding_multiple_records_with_noops.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb)
        res = session.run("RETURN 1 as n")
        records = list(res)

        session.close()
        driver.close()

        self.assertEqual([types.Record(values=[types.CypherInt(1)]),
                          types.Record(values=[types.CypherInt(5)]),
                          types.Record(values=[types.CypherInt(7)])], records)
        self._server.done()

    def test_should_error_on_database_shutdown_using_tx_commit(self):
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "writer_yielding_db_unavailable_error_"
                "then_shut_down_on_commit.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        res = tx.run("RETURN 1 as n")
        list(res)
        summary = res.consume()

        with self.assertRaises(types.DriverError) as exc:
            tx.commit()

        session.close()
        driver.close()

        self._assert_is_transient_commit_exception(
            exc.exception, expected_msg="Database shut down.")
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._server),
                         self._server.address])
        self._server.done()

    def test_should_error_on_database_shutdown_using_tx_run(self):
        # TODO remove this block once all drivers support this
        if get_driver_name() in ["go"]:
            self.skipTest("Fails with pending transaction message")
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "writer_with_bookmark_yielding_error_on_run.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("w", database=self.adb,
                                 bookmarks=["neo4j:bookmark:v1:tx0"])
        tx = session.begin_transaction()

        with self.assertRaises(types.DriverError) as exc:
            res = tx.run("RETURN 1 as n")
            res.consume()

        session.close()
        driver.close()

        self._assert_is_transient_commit_exception(
            exc.exception, expected_msg="Database shut down.")
        self._server.done()

    def test_should_read_successfully_with_database_name_using_session_run(
            self):
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "reader_yielding_multiple_records.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("r", database=self.adb)
        res = session.run("RETURN 1 as n")
        records = list(res)

        session.close()
        driver.close()

        self.assertEqual([types.Record(values=[types.CypherInt(1)]),
                          types.Record(values=[types.CypherInt(5)]),
                          types.Record(values=[types.CypherInt(7)])], records)
        self._server.done()

    def test_should_read_successfully_with_database_name_using_tx_function(
            self):
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "reader_tx_yielding_multiple_records.script"),
            vars_=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="p",
                                                 credentials="c"),
                        user_agent="007")

        session = driver.session("r", database=self.adb)
        records = None

        def work(tx):
            nonlocal records
            result = tx.run("RETURN 1 as n")
            records = list(result)

        session.read_transaction(work)

        session.close()
        driver.close()

        self.assertEqual([types.Record(values=[types.CypherInt(1)]),
                          types.Record(values=[types.CypherInt(5)]),
                          types.Record(values=[types.CypherInt(7)])], records)
        self._server.done()

    def _assert_is_transient_rollback_exception(
            self, e, expected_msg="Unable to rollback"):
        if get_driver_name() in ["java"]:
            self.assertEqual("org.neo4j.driver.exceptions.TransientException",
                             e.errorType)
            self.assertEqual(expected_msg, e.msg)
        elif get_driver_name() in ["ruby"]:
            self.assertEqual("Neo4j::Driver::Exceptions::TransientException",
                             e.errorType)
            self.assertEqual(expected_msg, e.msg)
        self.assertTrue(expected_msg in e.msg)
        self.assertEqual("Neo.TransientError.General.DatabaseUnavailable",
                         e.code)

    def _assert_is_transient_commit_exception(
            self, e, expected_msg="Unable to commit"):
        if get_driver_name() in ["java"]:
            self.assertEqual("org.neo4j.driver.exceptions.TransientException",
                             e.errorType)
            self.assertEqual(expected_msg, e.msg)
        elif get_driver_name() in ["ruby"]:
            self.assertEqual("Neo4j::Driver::Exceptions::TransientException",
                             e.errorType)
            self.assertEqual(expected_msg, e.msg)
        self.assertTrue(expected_msg in e.msg)
        self.assertEqual("Neo.TransientError.General.DatabaseUnavailable",
                         e.code)

    def _assert_supports_multi_db(self, supports_multi_db):
        self.assertTrue(supports_multi_db)
