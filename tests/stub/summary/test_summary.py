from contextlib import contextmanager
import json

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_dns_resolved_server_address,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestSummary(TestkitTestCase):
    """Test result summary contents."""

    required_features = types.Feature.BOLT_4_4,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9000)

    def tearDown(self):
        self._server.done()
        super().tearDown()

    @contextmanager
    def _get_session(self, script, vars_=None):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path(script), vars_=vars_)
        session = driver.session("w", fetch_size=1000)
        try:
            yield session
        finally:
            session.close()
            driver.close()
            self._server.reset()

    def _assert_counters(self, summary,
                         constraints_added=0, constraints_removed=0,
                         indexes_added=0, indexes_removed=0,
                         labels_added=0, labels_removed=0,
                         nodes_created=0, nodes_deleted=0,
                         properties_set=0,
                         relationships_created=0, relationships_deleted=0,
                         system_updates=0,
                         contains_updates=False,
                         contains_system_updates=False):
        attrs = (
            "constraints_added", "constraints_removed", "indexes_added",
            "indexes_removed", "labels_added", "labels_removed",
            "nodes_created", "nodes_deleted", "properties_set",
            "relationships_created", "relationships_deleted", "system_updates",
            "contains_updates", "contains_system_updates"
        )
        for attr in attrs:
            val = locals()[attr]
            self.assertIsInstance(getattr(summary.counters, attr), type(val))
            self.assertEqual(getattr(summary.counters, attr), val)

    def _get_summary(self, script, vars_=None):
        with self._get_session(script, vars_=vars_) as session:
            result = session.run("RETURN 1 AS n")
            return result.consume()

    def test_server_info(self):
        summary = self._get_summary("empty_summary_type_r.script")
        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._server))
        self.assertEqual(summary.server_info.agent, "Neo4j/4.4.0")
        expected_version = list(map(
            str, self._server.get_negotiated_bolt_version()
        ))
        while len(expected_version) < 2:
            expected_version.append("0")
        expected_version = ".".join(expected_version)
        self.assertEqual(summary.server_info.protocol_version,
                         expected_version)

    def test_database(self):
        summary = self._get_summary("empty_summary_type_r.script")
        self.assertEqual(summary.database, "apple")

    def test_query(self):
        def _test():
            if query_type is not None:
                script_name = "empty_summary_type_%s.script" % query_type
            else:
                script_name = "empty_summary_no_type.script"
            with self._get_session(script_name) as session:
                result = session.run("RETURN 1 AS n",
                                     params={"foo": types.CypherInt(123)})
                summary = result.consume()
            self.assertEqual(summary.query.text, "RETURN 1 AS n")
            self.assertEqual(summary.query.parameters,
                             {"foo": types.CypherInt(123)})
            self.assertEqual(summary.query_type, query_type)

        for query_type in ("r", "w", "rw", "s", None):
            with self.subTest(query_type=query_type):
                _test()

    def test_invalid_query_type(self):
        def _test():
            script_name = "empty_summary_type_%s.script" % query_type
            with self._get_session(script_name) as session:
                with self.assertRaises(types.DriverError) as e:
                    result = session.run("RETURN 1 AS n",
                                         params={"foo": types.CypherInt(123)})
                    result.consume()
            if get_driver_name() == "python":
                self.assertEqual(
                    e.exception.errorType,
                    "<class 'neo4j._exceptions.BoltProtocolError'>"
                )
            elif get_driver_name() == "java":
                self.assertEqual(
                    e.exception.errorType,
                    "org.neo4j.driver.exceptions.ProtocolException"
                )

        for query_type in ("wr",):
            with self.subTest(query_type=query_type):
                _test()

    def test_times(self):
        summary = self._get_summary("empty_summary_type_r.script")
        self.assertEqual(summary.result_available_after, 2001)
        self.assertEqual(summary.result_consumed_after, 2002)

    def test_no_times(self):
        summary = self._get_summary("no_summary.script")
        self.assertEqual(summary.result_available_after, None)
        self.assertEqual(summary.result_consumed_after, None)

    def test_no_notifications(self):
        summary = self._get_summary("empty_summary_type_r.script")
        self.assertEqual(summary.notifications, None)

    def test_empty_notifications(self):
        notifications = []
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(notifications)
            }
        )
        self.assertEqual(summary.notifications, notifications)

    def test_full_notification(self):
        notifications = [{
            "severity": "WARNING",
            "description": "If a part of a query contains multiple "
                           "disconnected patterns, ...",
            "code": "Neo.ClientNotification.Statement.CartesianProductWarning",
            "position": {"column": 9, "offset": 8, "line": 1},
            "title": "This query builds a cartesian product between..."
        }]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(notifications)
            }
        )
        self.assertEqual(summary.notifications, notifications)

    def test_notifications_without_position(self):
        notifications = [{
            "severity": "ANYTHING",
            "description": "If a part of a query contains multiple "
                           "disconnected patterns, ...",
            "code": "Neo.ClientNotification.Statement.CartesianProductWarning",
            "title": "This query builds a cartesian product between..."
        }]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={"#NOTIFICATIONS#": json.dumps(notifications)}
        )
        self.assertEqual(summary.notifications, notifications)

    def test_multiple_notifications(self):
        notifications = [{
            "severity": "WARNING",
            "description": "If a part of a query contains multiple "
                           "disconnected patterns, ...",
            "code": "Neo.ClientNotification.Statement.CartesianProductWarning",
            "title": "This query builds a cartesian product between... %i" % i
        } for i in range(1, 4)]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={"#NOTIFICATIONS#": json.dumps(notifications)}
        )
        self.assertEqual(summary.notifications, notifications)

    def test_plan(self):
        plan = {
            "args": {
                "planner-impl": "IDP",
                "Details": "n",
                "PipelineInfo": "Fused in Pipeline 0",
                "planner-version": "4.3",
                "runtime-version": "4.3",
                "runtime": "PIPELINED",
                "runtime-impl": "PIPELINED",
                "version": "CYPHER 4.3",
                "EstimatedRows": 1.5, "planner": "COST"
            },
            "operatorType": "ProduceResults@neo4j",
            "children": [
                {
                    "args": {
                        "Details": "(n)",
                        "EstimatedRows": 1.5,
                        "PipelineInfo": "Fused in Pipeline 0"
                    },
                    "operatorType": "Create@neo4j",
                    "children": [],
                    "identifiers": ["n"]
                }
            ],
            "identifiers": ["n"]
        }
        summary = self._get_summary(
            "summary_with_plan.script",
            vars_={"#PLAN#": json.dumps(plan)}
        )
        self.assertEqual(summary.plan, plan)

    def test_profile(self):
        profile = {
            "args": {
                "GlobalMemory": 136,
                "planner-impl": "IDP",
                "runtime": "PIPELINED",
                "runtime-impl": "PIPELINED",
                "version": "CYPHER 4.3",
                "DbHits": 1,
                "Details": "n",
                "PipelineInfo": "Fused in Pipeline 0",
                "planner-version": "4.3",
                "runtime-version": "4.3",
                "EstimatedRows": 1.1,
                "planner": "COST",
                "Rows": 1
            },
            "children": [
                {
                    "args": {
                        "Details": "(n)",
                        "PipelineInfo": "Fused in Pipeline 0",
                        "Time": 0,
                        "PageCacheMisses": 0,
                        "EstimatedRows": 1.1,
                        "DbHits": 1,
                        "Rows": 1,
                        "PageCacheHits": 0
                    },
                    "pageCacheMisses": 0,
                    "children": [],
                    "dbHits": 1,
                    "identifiers": ["n"],
                    "operatorType": "Create@neo4j",
                    "time": 0,
                    "rows": 1,
                    "pageCacheHitRatio": 0.1,
                    "pageCacheHits": 0
                }
            ],
            "dbHits": 1,
            "identifiers": ["n"],
            "operatorType": "ProduceResults@neo4j",
            "rows": 1
        }
        summary = self._get_summary(
            "summary_with_profile.script",
            vars_={"#PROFILE#": json.dumps(profile)}
        )
        self.assertEqual(summary.profile, profile)

    def test_empty_summary(self):
        summary = self._get_summary("empty_summary_type_r.script")
        self._assert_counters(summary)

    def test_full_summary_no_flags(self):
        summary = self._get_summary("full_summary.script")
        self._assert_counters(
            summary, constraints_added=1001, constraints_removed=1002,
            indexes_added=1003, indexes_removed=1004,
            labels_added=1005, labels_removed=1006,
            nodes_created=1007, nodes_deleted=1008,
            properties_set=1009,
            relationships_created=1010, relationships_deleted=1011,
            system_updates=1012,
            contains_updates=True, contains_system_updates=True
        )

    def test_no_summary(self):
        summary = self._get_summary("no_summary.script")
        self._assert_counters(summary)

    def test_partial_summary_constraints_added(self):
        summary = self._get_summary("partial_summary_constraints_added.script")
        self._assert_counters(
            summary, constraints_added=1234, contains_updates=True
        )

    def test_partial_summary_constraints_removed(self):
        summary = self._get_summary(
            "partial_summary_constraints_removed.script")
        self._assert_counters(
            summary, constraints_removed=1234, contains_updates=True
        )

    def test_partial_summary_contains_system_updates(self):
        summary = self._get_summary(
            "partial_summary_contains_system_updates.script"
        )
        self._assert_counters(summary, contains_system_updates=True)

    def test_partial_summary_contains_updates(self):
        summary = self._get_summary("partial_summary_contains_updates.script")
        self._assert_counters(summary, contains_updates=True)

    def test_partial_summary_not_contains_system_updates(self):
        summary = self._get_summary(
            "partial_summary_not_contains_system_updates.script"
        )
        self._assert_counters(
            summary, system_updates=1234, contains_system_updates=False
        )

    def test_partial_summary_not_contains_updates(self):
        summary = self._get_summary(
            "partial_summary_not_contains_updates.script"
        )
        self._assert_counters(
            summary, constraints_added=1234, contains_updates=False
        )

    def test_partial_summary_indexes_added(self):
        summary = self._get_summary("partial_summary_indexes_added.script")
        self._assert_counters(
            summary, indexes_added=1234, contains_updates=True
        )

    def test_partial_summary_indexes_removed(self):
        summary = self._get_summary("partial_summary_indexes_removed.script")
        self._assert_counters(
            summary, indexes_removed=1234, contains_updates=True
        )

    def test_partial_summary_labels_added(self):
        summary = self._get_summary("partial_summary_labels_added.script")
        self._assert_counters(
            summary, labels_added=1234, contains_updates=True
        )

    def test_partial_summary_labels_removed(self):
        summary = self._get_summary("partial_summary_labels_removed.script")
        self._assert_counters(
            summary, labels_removed=1234, contains_updates=True
        )

    def test_partial_summary_nodes_created(self):
        summary = self._get_summary("partial_summary_nodes_created.script")
        self._assert_counters(
            summary, nodes_created=1234, contains_updates=True
        )

    def test_partial_summary_nodes_deleted(self):
        summary = self._get_summary("partial_summary_nodes_deleted.script")
        self._assert_counters(
            summary, nodes_deleted=1234, contains_updates=True
        )

    def test_partial_summary_properties_set(self):
        summary = self._get_summary("partial_summary_properties_set.script")
        self._assert_counters(
            summary, properties_set=1234, contains_updates=True
        )

    def test_partial_summary_relationships_created(self):
        summary = self._get_summary(
            "partial_summary_relationships_created.script"
        )
        self._assert_counters(
            summary, relationships_created=1234, contains_updates=True
        )

    def test_partial_summary_relationships_deleted(self):
        summary = self._get_summary(
            "partial_summary_relationships_deleted.script"
        )
        self._assert_counters(
            summary, relationships_deleted=1234, contains_updates=True
        )

    def test_partial_summary_system_updates(self):
        summary = self._get_summary("partial_summary_system_updates.script")
        self._assert_counters(
            summary, system_updates=1234, contains_system_updates=True
        )
