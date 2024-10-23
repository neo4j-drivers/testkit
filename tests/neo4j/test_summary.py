import re

from nutkit import protocol as types
from tests.neo4j.shared import (
    cluster_unsafe_test,
    get_driver,
    get_neo4j_host_and_port,
    get_neo4j_resolved_host_and_port,
    get_server_info,
    QueryBuilder,
    requires_multi_db_support,
)
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)


class TestSummary(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._driver = get_driver(self._backend)
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        self._driver.close()
        super().tearDown()

    def clear_db(self):
        def work(tx):
            result = tx.run("MATCH (n) DETACH DELETE n")
            result.consume()

        self._session = self._driver.session("w")
        try:
            return self._session.execute_write(work)
        finally:
            self._session.close()

    def get_summary(self, query, params=None, **kwargs):
        def work(tx):
            result = tx.run(query, params=params, **kwargs)
            for _ in result:
                pass
            summary = result.consume()
            return summary
        params = {} if params is None else params
        self._session = self._driver.session("w")
        return self._session.execute_write(work)

    def test_can_obtain_summary_after_consuming_result(self):
        summary = self.get_summary("CREATE (n) RETURN n")
        self.assertEqual(summary.query.text, "CREATE (n) RETURN n")
        self.assertEqual(summary.query.parameters, {})
        self.assertEqual(summary.query_type, "rw")
        self.assertEqual(summary.counters.nodes_created, 1)

    def test_no_plan_info(self):
        summary = self.get_summary("CREATE (n) RETURN n")
        self.assertIsNone(summary.plan)
        self.assertIsNone(summary.profile)

    def test_can_obtain_plan_info(self):
        summary = self.get_summary("EXPLAIN CREATE (n) RETURN n")
        self.assertIsInstance(summary.plan, dict)

    def test_can_obtain_profile_info(self):
        summary = self.get_summary("PROFILE CREATE (n) RETURN n")
        self.assertIsInstance(summary.profile, dict)

    def test_no_notification_info(self):
        summary = self.get_summary("CREATE (n) RETURN n")
        notifications = summary.notifications
        self.assertTrue(notifications is None or summary.notifications == [])

    def _test_status(self, query, expected_code):
        summary = self.get_summary(query)
        statuses = summary.gql_status_objects
        self.assertEqual(len(statuses), 1)
        status = statuses[0]
        self.assertFalse(status.is_notification)
        self.assertEqual(status.gql_status, expected_code)

    @driver_feature(types.Feature.API_SUMMARY_GQL_STATUS_OBJECTS)
    def test_success_status(self):
        self._test_status("CREATE (n) RETURN n", "00000")

    @driver_feature(types.Feature.API_SUMMARY_GQL_STATUS_OBJECTS)
    def test_omitted_status(self):
        self._test_status("CREATE (n)", "00001")

    @driver_feature(types.Feature.API_SUMMARY_GQL_STATUS_OBJECTS)
    def test_no_data_status(self):
        self.clear_db()
        self._test_status("MATCH (n) RETURN n", "02000")

    def test_can_obtain_notification_info(self):
        summary = self.get_summary("EXPLAIN MATCH (n), (m) RETURN n, m")
        notifications = summary.notifications
        self.assertIsInstance(notifications, list)
        self.assertEqual(len(notifications), 1)
        self.assertIsInstance(notifications[0], dict)

    def test_contains_time_information(self):
        summary = self.get_summary("UNWIND range(1, 100) AS n "
                                   "RETURN n AS number")

        self.assertIsInstance(summary.result_available_after, int)
        self.assertIsInstance(summary.result_consumed_after, int)

        self.assertGreaterEqual(summary.result_available_after, 0)
        self.assertGreaterEqual(summary.result_consumed_after, 0)

    def test_protocol_version_information(self):
        summary = self.get_summary("RETURN 1 AS number")

        max_server_protocol_version = get_server_info().max_protocol_version
        common_protocol_versions = [
            f.value.split(":")[-1] for f in self._driver_features
            if (re.match(r"BOLT_\d+_\d+", f.name)
                and f.value.split(":")[-1] <= max_server_protocol_version)
        ]
        if not common_protocol_versions:
            self.skipTest("Driver does not support server version.")
        common_max_version = max(common_protocol_versions)
        if common_max_version == "4.2":
            # Both versions are equivalent. Since 4.2 was introduced before
            # having version ranges in the handshake, we allow drivers to
            # negotiate bolt 4.1 with 4.2 to be able to fit support for more
            # server versions into the handshake
            self.assertIn(summary.server_info.protocol_version,
                          ("4.2", "4.1"))
        else:
            self.assertEqual(summary.server_info.protocol_version,
                             common_max_version)

    def test_agent_string(self):
        summary = self.get_summary("RETURN 1 AS number")

        agent = summary.server_info.agent
        self.assertIsInstance(agent, str)
        server_info = get_server_info()
        if server_info.edition == "aura":
            # for aura the agent string tends to be all over the place...
            self.assertTrue(agent.startswith("Neo4j/"))
        elif re.match(r"(\d+)\.dev", server_info.version):
            self.assertTrue(agent.startswith(
                "Neo4j/" + server_info.version.split(".")[0]
            ))
        else:
            agent = ".".join(summary.server_info.agent.split(".")[:2])
            self.assertEqual(agent, get_server_info().server_agent)

    @cluster_unsafe_test  # routing can lead us to another server (address)
    def test_address(self):
        summary = self.get_summary("RETURN 1 AS number")
        self.assertTrue(summary.server_info.address in
                        ["%s:%s" % get_neo4j_resolved_host_and_port(),
                         "%s:%s" % get_neo4j_host_and_port()])

    def _assert_counters(self, summary, nodes_created=0, nodes_deleted=0,
                         relationships_created=0, relationships_deleted=0,
                         properties_set=0, labels_added=0, labels_removed=0,
                         indexes_added=0, indexes_removed=0,
                         constraints_added=0, constraints_removed=0,
                         system_updates=0, contains_updates=False,
                         contains_system_updates=False):
        for attr, val in locals().items():
            if attr in ("self", "summary"):
                continue
            counter_val = getattr(summary.counters, attr)
            self.assertIsInstance(counter_val, type(val))
            self.assertEqual(counter_val, val)

    def test_summary_counters_case_1(self):
        params = {"number": types.CypherInt(3)}

        summary = self.get_summary("RETURN $number AS x", params=params)

        self.assertEqual(summary.query.text, "RETURN $number AS x")
        self.assertEqual(summary.query.parameters, params)

        self.assertIn(summary.query_type, ("r", "w", "rw", "s"))

        self._assert_counters(summary)

    @requires_multi_db_support
    @cluster_unsafe_test
    def test_summary_counters_case_2(self):
        version = get_server_info().version
        new_index_syntax = version >= "4"
        new_constraint_syntax = version >= "4.4"

        self._session = self._driver.session("w", database="system")

        drop_db_test_query = QueryBuilder.drop_db("test")
        create_db_test_query = QueryBuilder.create_db("test")
        self._session.run(drop_db_test_query).consume()

        # SHOW DATABASES

        result = self._session.run("SHOW DATABASES")
        databases = set()
        try:
            name_idx = result.keys().index("name")
        except ValueError:
            pass
        else:
            for record in result:
                databases.add(record.values[name_idx].value)
        self.assertIn("system", databases)
        self.assertIn("neo4j", databases)

        summary = result.consume()

        self.assertEqual(summary.query.text, "SHOW DATABASES")
        self.assertEqual(summary.query.parameters, {})

        self.assertIn(summary.query_type, ("r", "w", "rw", "s"))

        self._assert_counters(summary)

        # CREATE DATABASE test
        self._session.run(drop_db_test_query).consume()
        summary = self._session.run(create_db_test_query).consume()

        self.assertEqual(summary.query.text, create_db_test_query)
        self.assertEqual(summary.query.parameters, {})

        self.assertIn(summary.query_type, ("r", "w", "rw", "s"))

        self._assert_counters(summary,
                              system_updates=1, contains_system_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("CREATE (n)").consume()
        self._assert_counters(summary, nodes_created=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("MATCH (n) DELETE (n)").consume()
        self._assert_counters(summary, nodes_deleted=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("CREATE ()-[:KNOWS]->()").consume()
        self._assert_counters(summary, nodes_created=2,
                              relationships_created=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("MATCH ()-[r:KNOWS]->() "
                                    "DELETE r").consume()
        self._assert_counters(summary,
                              relationships_deleted=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("CREATE (n:ALabel)").consume()
        self._assert_counters(summary, nodes_created=1, labels_added=1,
                              contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run(
            "MATCH (n:ALabel) REMOVE n:ALabel"
        ).consume()
        self._assert_counters(summary, labels_removed=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("CREATE (n {magic: 42})").consume()
        self._assert_counters(summary, nodes_created=1, properties_set=1,
                              contains_updates=True)
        self._session.close()

        if new_index_syntax:  # 4.0+
            query = "CREATE INDEX test_label_prop FOR (n:ALabel) ON (n.prop)"
        else:  # 3.5-
            query = "CREATE INDEX ON :ALabel (prop)"
        self._session = self._driver.session("w", database="test")
        summary = self._session.run(query).consume()
        self._assert_counters(summary, indexes_added=1, contains_updates=True)
        self._session.close()

        if new_index_syntax:  # 4.0+
            query = "DROP INDEX test_label_prop"
        else:  # 3.5-
            query = "DROP INDEX ON :ALabel(prop)"
        self._session = self._driver.session("w", database="test")
        summary = self._session.run(query).consume()
        self._assert_counters(summary, indexes_removed=1,
                              contains_updates=True)
        self._session.close()

        if new_constraint_syntax:  # 4.4+
            query = ("CREATE CONSTRAINT test_book_isbn FOR (book:Book) "
                     "REQUIRE book.isbn IS UNIQUE")
        else:  # 4.3-
            query = ("CREATE CONSTRAINT ON (book:Book) "
                     "ASSERT book.isbn IS UNIQUE")
        self._session = self._driver.session("w", database="test")
        summary = self._session.run(query).consume()
        self._assert_counters(summary,
                              constraints_added=1, contains_updates=True)
        self._session.close()

        if new_constraint_syntax:  # 4.4+
            query = "DROP CONSTRAINT test_book_isbn"
        else:  # 4.3-
            query = ("DROP CONSTRAINT ON (book:Book) "
                     "ASSERT book.isbn IS UNIQUE")
        self._session = self._driver.session("w", database="test")
        summary = self._session.run(query).consume()
        self._assert_counters(summary,
                              constraints_removed=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="system")
        self._session.run(drop_db_test_query).consume()
