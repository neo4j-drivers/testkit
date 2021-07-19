from nutkit import protocol as types
from ..shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from .shared import (
    get_driver,
    get_server_info,
)

# TODO: migrate rest of test_bolt_driver.py


class TestDirectDriver(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._driver = get_driver(self._backend)
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        self._driver.close()
        super().tearDown()

    def get_summary(self, query, params=None, **kwargs):
        params = {} if params is None else params
        self._session = self._driver.session("w")
        result = self._session.run(query, params=params, **kwargs)
        for _ in result:
            pass
        summary = result.consume()
        return summary

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_can_obtain_summary_after_consuming_result(self):
        summary = self.get_summary("CREATE (n) RETURN n")
        self.assertEqual(summary.query.text, "CREATE (n) RETURN n")
        self.assertEqual(summary.query.parameters, {})
        self.assertEqual(summary.query_type, "rw")
        self.assertEqual(summary.counters.nodes_created, 1)

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_no_plan_info(self):
        summary = self.get_summary("CREATE (n) RETURN n")
        self.assertIsNone(summary.plan)
        self.assertIsNone(summary.profile)

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_can_obtain_plan_info(self):
        summary = self.get_summary("EXPLAIN CREATE (n) RETURN n")
        self.assertIsInstance(summary.plan, dict)

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_no_notification_info(self):
        summary = self.get_summary("CREATE (n) RETURN n")
        self.assertIsNone(summary.notifications)

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_can_obtain_notification_info(self):
        summary = self.get_summary("EXPLAIN MATCH (n), (m) RETURN n, m")
        notifications = summary.notifications
        self.assertIsInstance(notifications, list)
        self.assertEqual(len(notifications), 1)
        self.assertIsInstance(notifications[0], dict)

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_contains_time_information(self):
        summary = self.get_summary("UNWIND range(1, 100) AS n "
                                   "RETURN n AS number")

        self.assertIsInstance(summary.result_available_after, int)
        self.assertIsInstance(summary.result_consumed_after, int)

        self.assertGreaterEqual(summary.result_available_after, 0)
        self.assertGreaterEqual(summary.result_consumed_after, 0)

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_protocol_version_information(self):
        summary = self.get_summary("RETURN 1 AS number")

        max_protocol_version = get_server_info().max_protocol_version
        if max_protocol_version == "4.2":
            # Both versions are equivalent. Since 4.2 was introduced before
            # having version ranges in the handshake, we allow drivers to
            # negotiate bolt 4.1 with 4.2 to be able to fit support for more
            # server versions into the handshake
            self.assertIn(summary.server_info.protocol_version,
                          ("4.2", "4.1"))
        else:
            self.assertEqual(summary.server_info.protocol_version,
                             get_server_info().max_protocol_version)

    def test_agent_string(self):
        summary = self.get_summary("RETURN 1 AS number")
        if isinstance(summary, dict) and get_driver_name() in ["java"]:
            self.skipTest("Java 4.2 backend does not support summary")

        self.assertTrue(summary.server_info.agent.startswith("Neo4j/"))
        version = summary.server_info.agent[6:].split(".")
        self.assertEqual(version[:2], get_server_info().version.split("."))

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

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_summary_counters_case_1(self):
        params = {"number": types.CypherInt(3)}

        summary = self.get_summary("RETURN $number AS x", params=params)

        self.assertEqual(summary.query.text, "RETURN $number AS x")
        self.assertEqual(summary.query.parameters, params)

        self.assertIn(summary.query_type, ("r", "w", "rw", "s"))

        self._assert_counters(summary)

    @driver_feature(types.Feature.TMP_RESULT_KEYS,
                    types.Feature.TMP_FULL_SUMMARY)
    def test_summary_counters_case_2(self):
        if not get_server_info().supports_multi_db:
            self.skipTest("Needs multi DB support")

        self._session = self._driver.session("w", database="system")

        self._session.run("DROP DATABASE test IF EXISTS").consume()

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
        self._session.run("DROP DATABASE test IF EXISTS").consume()
        summary = self._session.run("CREATE DATABASE test").consume()

        self.assertEqual(summary.query.text, "CREATE DATABASE test")
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
        self._assert_counters(summary, nodes_created=2, relationships_created=1,
                              contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("MATCH ()-[r:KNOWS]->() DELETE r").consume()
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

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("CREATE INDEX ON :ALabel(prop)").consume()
        self._assert_counters(summary, indexes_added=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("DROP INDEX ON :ALabel(prop)").consume()
        self._assert_counters(summary, indexes_removed=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("CREATE CONSTRAINT ON (book:Book) "
                                    "ASSERT book.isbn IS UNIQUE").consume()
        self._assert_counters(summary,
                              constraints_added=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="test")
        summary = self._session.run("DROP CONSTRAINT ON (book:Book) "
                                    "ASSERT book.isbn IS UNIQUE").consume()
        self._assert_counters(summary,
                              constraints_removed=1, contains_updates=True)
        self._session.close()

        self._session = self._driver.session("w", database="system")
        self._session.run("DROP DATABASE test IF EXISTS").consume()
