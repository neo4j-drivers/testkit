import copy
import json
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_dns_resolved_server_address,
    get_driver_name,
)
from tests.stub.shared import StubServer
from tests.stub.summary._base import _TestSummaryBase


class _TestSummaryBaseBolt(_TestSummaryBase):
    """Test result summary contents via BOLT."""

    full_notifications_feat = types.Feature.API_DRIVER_NOTIFICATIONS_CONFIG
    version_folder = ()

    def setUp(self):
        super().setUp()
        self._server = StubServer(9000)

    def tearDown(self):
        self._server.done()
        super().tearDown()

    @contextmanager
    def _get_session(self, script, vars_=None, fetch_size=1000):
        uri = "bolt://%s" % self._server.address
        driver = Driver(
            self._backend,
            uri,
            types.AuthorizationToken("basic", principal="", credentials=""),
        )
        script_path = self.script_path(*self.version_folder, script)
        self._server.start(path=script_path, vars_=vars_)
        session = driver.session("w", fetch_size=fetch_size)
        try:
            yield session
        finally:
            session.close()
            driver.close()
            self._server.reset()

    def _get_summary(self, script, vars_=None):
        with self._get_session(script, vars_=vars_) as session:
            result = session.run("RETURN 1 AS n")
            list(result)
            return result.consume()

    def assert_plan_equal(self, actual, expected):
        missing_stats_detectable = self.driver_supports_features(
            types.Feature.API_SUMMARY_PROFILE_OPTIONAL_STATS
        )

        def adjust_expected(actual_child, expected_child):
            for key in [k for k, v in expected_child.items() if v is None]:
                if key not in actual_child:
                    expected_child.pop(key)
            for key in (k for k, v in actual_child.items() if v is None):
                expected_child.setdefault(key, None)

            if not missing_stats_detectable:
                for key, actual_value in actual_child.items():
                    if actual_value == 0 and expected_child.get(key) is None:
                        expected_child[key] = 0

            # drivers are free to represent lack of children with either:
            #   * an empty list
            #   * a null value
            #   * lack of the key
            expected_children = expected_child.get("children")
            expected_has_no_children = expected_children in (None, [])
            actual_children = actual_child.get("children")
            actual_has_no_children = actual_children in (None, [])

            if expected_has_no_children and actual_has_no_children:
                if "children" in actual_child:
                    expected_child["children"] = actual_children
                else:
                    expected_child.pop("children", None)
                return

            expected_children = expected_children or []
            actual_children = actual_children or []
            if len(expected_children) == len(actual_children):
                for ac, ec in zip(actual_children, expected_children):
                    adjust_expected(ac, ec)

        actual = copy.deepcopy(actual)
        expected = copy.deepcopy(expected)
        adjust_expected(actual, expected)
        self.assertEqual(actual, expected)


class _TestSummaryDiscardMixin(_TestSummaryBase):
    def _get_summary(self, script, vars_=None):
        with self._get_session(script, vars_=vars_, fetch_size=1) as session:
            result = session.run("RETURN 1 AS n")
            result.next()
            return result.consume()


class TestSummaryBasicInfo(_TestSummaryBase):
    required_features = types.Feature.BOLT_4_4,
    version_folder = "v4x4",

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
            driver = get_driver_name()
            if driver in ["python"]:
                self.assertEqual(
                    e.exception.errorType,
                    "<class 'neo4j._exceptions.BoltProtocolError'>"
                )
            elif driver in ["java"]:
                self.assertEqual(
                    e.exception.errorType,
                    "org.neo4j.driver.exceptions.ProtocolException"
                )
            elif driver in ["ruby"]:
                self.assertEqual(
                    e.exception.errorType,
                    "Neo4j::Driver::Exceptions::ProtocolException"
                )
            elif driver in ["go"]:
                self.assertEqual(e.exception.errorType, "ProtocolError")

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


class TestSummaryBasicInfoDiscard(
    _TestSummaryDiscardMixin,
    TestSummaryBasicInfo,
):
    def test_server_info(self):
        super().test_server_info()

    def test_database(self):
        super().test_database()

    def test_query(self):
        super().test_query()

    def test_invalid_query_type(self):
        super().test_invalid_query_type()

    def test_times(self):
        super().test_times()

    def test_no_times(self):
        super().test_no_times()


class TestSummaryNotifications4x4(_TestSummaryBase):
    required_features = types.Feature.BOLT_4_4,
    version_folder = "v4x4",

    def test_no_notifications(self):
        summary = self._get_summary("empty_summary_type_r.script")
        self.assertIn(summary.notifications, ([], None))

    def test_empty_notifications(self):
        notifications = []
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(notifications)
            }
        )
        self.assertIn(summary.notifications, ([], None))

    def test_full_notification(self):
        in_notifications = [{
            "severity": "WARNING",
            "description": "If a part of a query contains multiple "
                           "disconnected patterns, ...",
            "code": "Neo.ClientNotification.Statement.CartesianProductWarning",
            "position": {"column": 9, "offset": 8, "line": 1},
            "title": "This query builds a cartesian product between..."
        }]
        if self.driver_supports_features(self.full_notifications_feat):
            for n in in_notifications:
                n.update({"category": "GENERIC"})
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(in_notifications)
            }
        )
        out_notifications = [{
            "description": "If a part of a query contains multiple "
                           "disconnected patterns, ...",
            "code": "Neo.ClientNotification.Statement.CartesianProductWarning",
            "position": {"column": 9, "offset": 8, "line": 1},
            "title": "This query builds a cartesian product between..."
        }]

        if self.driver_supports_features(self.full_notifications_feat):
            for notification in out_notifications:
                notification.update({
                    "rawSeverityLevel": "WARNING",
                    "severityLevel": "WARNING",
                    "rawCategory": "GENERIC",
                    "category": "GENERIC",
                })
            self.assertEqual(summary.notifications, out_notifications)
        else:
            self.assertEqual(summary.notifications, out_notifications)

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
        if self.driver_supports_features(self.full_notifications_feat):
            for notification in notifications:
                notification.update({
                    "rawSeverityLevel": "ANYTHING",
                    "severityLevel": "UNKNOWN",
                    "rawCategory": "",
                    "category": "UNKNOWN",
                })
                del notification["severity"]
        self.assertEqual(summary.notifications, notifications)

    def test_multiple_notifications(self):
        notifications = [
            {
                "severity": "WARNING",
                "description": "If a part of a query contains multiple "
                               "disconnected patterns, ...",
                "code":
                    "Neo.ClientNotification.Statement.CartesianProductWarning",
                "title":
                    "This query builds a cartesian product between... %i" % i
            }
            for i in range(1, 4)
        ]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={"#NOTIFICATIONS#": json.dumps(notifications)}
        )
        if self.driver_supports_features(self.full_notifications_feat):
            for notification in notifications:
                notification.update({
                    "rawSeverityLevel": "WARNING",
                    "severityLevel": "WARNING",
                    "rawCategory": "",
                    "category": "UNKNOWN",
                })
                del notification["severity"]
        self.assertEqual(summary.notifications, notifications)


class TestSummaryNotifications4x4Discard(
    _TestSummaryDiscardMixin,
    TestSummaryNotifications4x4,
):
    def test_no_notifications(self):
        super().test_no_notifications()

    def test_empty_notifications(self):
        super().test_empty_notifications()

    def test_full_notification(self):
        super().test_full_notification()

    def test_notifications_without_position(self):
        super().test_notifications_without_position()

    def test_multiple_notifications(self):
        super().test_multiple_notifications()


SUCCESS_GQL_STATUS_OBJECT = {
    "gql_status": "00000",
    "status_description": "note: successful completion",
}

OMITTED_GQL_STATUS_OBJECT = {
    "gql_status": "00001",
    "status_description": "note: successful completion - omitted result",
}

NO_DATA_GQL_STATUS_OBJECT = {
    "gql_status": "02000",
    "status_description": "note: no data",
}


class TestSummaryNotifications5x6(_TestSummaryBase):
    required_features = types.Feature.BOLT_5_6,
    version_folder = "v5x6",

    def test_no_notifications(self):
        summary = self._get_summary("empty_summary_type_r.script")
        self.assertIn(summary.notifications, ([], None))

    def test_empty_notifications(self):
        statuses = [SUCCESS_GQL_STATUS_OBJECT]
        summary = self._get_summary(
            "summary_with_statuses.script",
            vars_={
                "#STATUSES#": json.dumps(statuses)
            }
        )
        self.assertIn(summary.notifications, ([], None))

    def test_full_notification(self):
        in_statuses = [
            {
                "gql_status": "01N01",
                "status_description": "warn: test subcat. Don't do this™.",
                "description": "Please, don't do this™.",
                "neo4j_code": "Neo.ClientNotification.Foo.Bar",
                "title": "Legacy warning title",
                "diagnostic_record": {
                    "OPERATION": "SOME_OP",
                    "OPERATION_CODE": "42",
                    "CURRENT_SCHEMA": "/foo",
                    "_status_parameters": {
                        "action": "this™",
                    },
                    "_severity": "WARNING",
                    "_classification": "GENERIC",
                    "_position": {"column": 9, "offset": 8, "line": 1},
                },
            },
            SUCCESS_GQL_STATUS_OBJECT
        ]
        summary = self._get_summary(
            "summary_with_statuses.script",
            vars_={
                "#STATUSES#": json.dumps(in_statuses)
            }
        )
        out_notifications = [{
            "description": "Please, don't do this™.",
            "code": "Neo.ClientNotification.Foo.Bar",
            "position": {"column": 9, "offset": 8, "line": 1},
            "title": "Legacy warning title",
            "rawSeverityLevel": "WARNING",
            "severityLevel": "WARNING",
            "rawCategory": "GENERIC",
            "category": "GENERIC",
        }]

        self.assertEqual(summary.notifications, out_notifications)

    def test_notification_without_position(self):
        in_statuses = [
            {
                "gql_status": "01N01",
                "status_description": "warn: test subcat. Don't do this™.",
                "description": "Please, don't do this™.",
                "neo4j_code": "Neo.ClientNotification.Foo.Bar",
                "title": "Legacy warning title",
                "diagnostic_record": {
                    "OPERATION": "SOME_OP",
                    "OPERATION_CODE": "42",
                    "CURRENT_SCHEMA": "/foo",
                    "_status_parameters": {
                        "action": "this™",
                    },
                    "_severity": "WARNING",
                    "_classification": "GENERIC",
                },
            },
            SUCCESS_GQL_STATUS_OBJECT
        ]
        summary = self._get_summary(
            "summary_with_statuses.script",
            vars_={
                "#STATUSES#": json.dumps(in_statuses)
            }
        )
        out_notification = {
            "description": "Please, don't do this™.",
            "code": "Neo.ClientNotification.Foo.Bar",
            "position": None,
            "title": "Legacy warning title",
            "rawSeverityLevel": "WARNING",
            "severityLevel": "WARNING",
            "rawCategory": "GENERIC",
            "category": "GENERIC",
        }

        self.assertEqual(len(summary.notifications), 1)
        notification = summary.notifications[0]
        notification.setdefault("position", None)

        self.assertEqual(notification, out_notification)

    def test_full_notifications_unknown_fields(self):
        in_statuses = [
            {
                "gql_status": "01N01",
                "status_description": "warn: test subcat. Don't do this™.",
                "description": "Please, don't do this™.",
                "neo4j_code": "Neo.ClientNotification.Foo.Bar",
                "title": "Legacy warning title",
                "diagnostic_record": {
                    "OPERATION": "SOME_OP",
                    "OPERATION_CODE": "42",
                    "CURRENT_SCHEMA": "/foo",
                    "_status_parameters": {
                        "action": "this™",
                    },
                    "_severity": "ANYSEV",
                    "_classification": "ANYCAT",
                    "_position": {"column": 9, "offset": 8, "line": 1},
                    "_🤡": "🎈",
                },
            },
            SUCCESS_GQL_STATUS_OBJECT
        ]
        summary = self._get_summary(
            "summary_with_statuses.script",
            vars_={
                "#STATUSES#": json.dumps(in_statuses)
            }
        )
        out_notifications = [{
            "code": "Neo.ClientNotification.Foo.Bar",
            "position": {"column": 9, "offset": 8, "line": 1},
            "description": "Please, don't do this™.",
            "title": "Legacy warning title",
            "rawSeverityLevel": "ANYSEV",
            "severityLevel": "UNKNOWN",
            "rawCategory": "ANYCAT",
            "category": "UNKNOWN",
        }]

        self.assertEqual(summary.notifications, out_notifications)

    def test_multiple_notifications(self):
        in_statuses = [
            {
                "gql_status": "01N01",
                "status_description":
                    f"warn: test subcat. Don't do this™ {i}.",
                "description": f"Please, don't do this™ {i}.",
                "neo4j_code": f"Neo.ClientNotification.Foo.Bar{i}",
                "title": f"Legacy warning title {i}",
                "diagnostic_record": {
                    "OPERATION": "SOME_OP",
                    "OPERATION_CODE": "42",
                    "CURRENT_SCHEMA": "/foo",
                    "_status_parameters": {
                        "action": "this™",
                    },
                    "_severity": "WARNING",
                    "_classification": "GENERIC",
                    "_position": {"column": 9, "offset": 8, "line": 1 + i},
                },  # noqa: E122 - looks better
            }
            for i in range(1, 4)
        ]
        in_statuses = [
            NO_DATA_GQL_STATUS_OBJECT,
            *in_statuses
        ]
        summary = self._get_summary(
            "summary_with_statuses.script",
            vars_={"#STATUSES#": json.dumps(in_statuses)}
        )
        out_notifications = [
            {
                "code": f"Neo.ClientNotification.Foo.Bar{i}",
                "position": {"column": 9, "offset": 8, "line": 1 + i},
                "description": f"Please, don't do this™ {i}.",
                "title": f"Legacy warning title {i}",
                "rawSeverityLevel": "WARNING",
                "severityLevel": "WARNING",
                "rawCategory": "GENERIC",
                "category": "GENERIC",
            }
            for i in range(1, 4)
        ]
        self.assertEqual(summary.notifications, out_notifications)


class TestSummaryNotifications5x6Discard(
    _TestSummaryDiscardMixin,
    TestSummaryNotifications5x6,
):
    def test_no_notifications(self):
        super().test_no_notifications()

    def test_empty_notifications(self):
        super().test_empty_notifications()

    def test_full_notification(self):
        super().test_full_notification()

    def test_notification_without_position(self):
        super().test_notification_without_position()

    def test_full_notifications_unknown_fields(self):
        super().test_full_notifications_unknown_fields()

    def test_multiple_notifications(self):
        super().test_multiple_notifications()


class _TestSummaryGqlStatusObjectsBase(_TestSummaryBase):
    def assert_is_non_notification_status(self, status):
        self.assertEqual(status.position, None)
        self.assertEqual(status.classification, "UNKNOWN")
        self.assertEqual(status.raw_classification, None)
        self.assertEqual(status.severity, "UNKNOWN")
        self.assertEqual(status.raw_severity, None)
        self.assertEqual(status.diagnostic_record, {
            "OPERATION": types.CypherString(""),
            "OPERATION_CODE": types.CypherString("0"),
            "CURRENT_SCHEMA": types.CypherString("/")
        })
        self.assertEqual(status.is_notification, False)

    def assert_is_success(self, status):
        self.assertEqual(status.gql_status, "00000")
        self.assertEqual(status.status_description,
                         "note: successful completion")
        self.assert_is_non_notification_status(status)

    def assert_is_omitted_result(self, status):
        self.assertEqual(status.gql_status, "00001")
        self.assertEqual(status.status_description,
                         "note: successful completion - omitted result")
        self.assert_is_non_notification_status(status)

    def assert_is_no_data(self, status):
        self.assertEqual(status.gql_status, "02000")
        self.assertEqual(status.status_description,
                         "note: no data")
        self.assert_is_non_notification_status(status)

    def assert_is_no_data_unknown_subclass(self, status):
        self.assertEqual(status.gql_status, "02N42")
        self.assertEqual(status.status_description,
                         "note: no data - unknown subcondition")
        self.assert_is_non_notification_status(status)


class TestSummaryGqlStatusObjects4x4(_TestSummaryGqlStatusObjectsBase):
    required_features = (
        types.Feature.BOLT_4_4,
        types.Feature.API_SUMMARY_GQL_STATUS_OBJECTS,
    )
    version_folder = "v4x4",

    def test_no_notifications(self):
        summary = self._get_summary("empty_summary_type_r.script")
        self.assertEqual(len(summary.gql_status_objects), 1)
        if self._server.count_requests("PULL"):
            self.assert_is_success(summary.gql_status_objects[0])
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )

    def test_no_notifications_no_data(self):
        summary = self._get_summary("empty_summary_type_r_no_data.script")
        self.assertEqual(len(summary.gql_status_objects), 1)
        if self._server.count_requests("PULL"):
            self.assert_is_no_data(summary.gql_status_objects[0])
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )

    def test_empty_notifications(self):
        notifications = []
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(notifications)
            }
        )
        self.assertEqual(len(summary.gql_status_objects), 1)
        if self._server.count_requests("PULL"):
            self.assert_is_success(summary.gql_status_objects[0])
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )

    @classmethod
    def make_test_notification(
        cls, i=None, severity="WARNING", category="HINT"
    ):
        if i is None:
            return {
                "severity": severity,
                "category": category,
                "description": "If a part of a query contains multiple "
                               "disconnected patterns, ...",
                "code": "Neo.ClientNotification.Statement."
                        "CartesianProductWarning",
                "position": {"column": 9, "offset": 8, "line": 1},
                "title": "This query builds a cartesian product between..."
            }
        else:
            return {
                "severity": severity,
                "category": category,
                "description": "If a part of a query contains multiple "
                               f"disconnected patterns, ... [{i}]",
                "code": "Neo.ClientNotification.Statement."
                        f"CartesianProductWarning{i}",
                "position": {"column": 9, "offset": 8, "line": 1 + i},
                "title": f"This query builds a cartesian product ... [{i}]"
            }

    def assert_is_test_notification_as_gql_status_object(
        self, status, i=None,
        raw_pos=..., description=...,
        severity="WARNING", parsed_severity="WARNING",
        category="HINT", parsed_category="HINT",
    ):
        raw_notification = self.make_test_notification(
            i, severity=severity, category=category
        )
        if raw_pos is ...:
            expected_pos = raw_notification["position"]
            raw_pos = expected_pos
        else:
            expected_pos = raw_pos
        assert isinstance(status, types.GqlStatusObject)
        if severity == "WARNING":
            self.assertEqual(status.gql_status, "01N42")
        else:
            self.assertEqual(status.gql_status, "03N42")
        if description is ...:
            description = raw_notification["description"]
        self.assertEqual(status.status_description, description)
        self.assertEqual(status.position, raw_pos)
        self.assertEqual(status.classification, parsed_category)
        self.assertEqual(status.raw_classification, category)
        self.assertEqual(status.severity, parsed_severity)
        self.assertEqual(status.raw_severity, severity)
        expected_diag_record = {
            "OPERATION": types.CypherString(""),
            "OPERATION_CODE": types.CypherString("0"),
            "CURRENT_SCHEMA": types.CypherString("/"),
        }
        if severity is not None:
            expected_diag_record["_severity"] = types.CypherString(
                severity
            )
        if category is not None:
            expected_diag_record["_classification"] = types.CypherString(
                category
            )
        if expected_pos is not None:
            expected_diag_record["_position"] = types.CypherMap({
                "column": types.CypherInt(expected_pos["column"]),
                "offset": types.CypherInt(expected_pos["offset"]),
                "line": types.CypherInt(expected_pos["line"]),
            })
        self.assertEqual(status.diagnostic_record, expected_diag_record)
        self.assertEqual(status.is_notification, True)

    def test_warning(self):
        in_notifications = [self.make_test_notification()]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(in_notifications)
            }
        )
        self.assertEqual(len(summary.gql_status_objects), 2)
        if self._server.count_requests("PULL"):
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[0]
            )
            self.assert_is_success(summary.gql_status_objects[1])
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[1]
            )

    def test_warning_no_data(self):
        in_notifications = [self.make_test_notification()]
        summary = self._get_summary(
            "summary_with_notifications_no_data.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(in_notifications)
            }
        )
        self.assertEqual(len(summary.gql_status_objects), 2)
        if self._server.count_requests("PULL"):
            self.assert_is_no_data(summary.gql_status_objects[0])
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )
        self.assert_is_test_notification_as_gql_status_object(
            summary.gql_status_objects[1]
        )

    def test_information(self):
        in_notifications = [
            self.make_test_notification(severity="INFORMATION")
        ]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(in_notifications)
            }
        )
        self.assertEqual(len(summary.gql_status_objects), 2)
        if self._server.count_requests("PULL"):
            self.assert_is_success(summary.gql_status_objects[0])
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )
        self.assert_is_test_notification_as_gql_status_object(
            summary.gql_status_objects[1],
            severity="INFORMATION", parsed_severity="INFORMATION"
        )

    def test_information_omitted_result(self):
        in_notifications = [
            self.make_test_notification(severity="INFORMATION")
        ]
        summary = self._get_summary(
            "summary_with_notifications_omitted_result.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(in_notifications)
            }
        )
        self.assertEqual(len(summary.gql_status_objects), 2)
        self.assert_is_omitted_result(summary.gql_status_objects[0])
        self.assert_is_test_notification_as_gql_status_object(
            summary.gql_status_objects[1],
            severity="INFORMATION", parsed_severity="INFORMATION"
        )

    def test_information_no_data(self):
        in_notifications = [
            self.make_test_notification(severity="INFORMATION")
        ]
        summary = self._get_summary(
            "summary_with_notifications_no_data.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(in_notifications)
            }
        )
        self.assertEqual(len(summary.gql_status_objects), 2)
        if self._server.count_requests("PULL"):
            self.assert_is_no_data(summary.gql_status_objects[0])
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )
        self.assert_is_test_notification_as_gql_status_object(
            summary.gql_status_objects[1],
            severity="INFORMATION", parsed_severity="INFORMATION"
        )

    def test_unknown_severity(self):
        in_notifications = [
            self.make_test_notification(severity="FOOBAR")
        ]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(in_notifications)
            }
        )
        self.assertEqual(len(summary.gql_status_objects), 2)
        if self._server.count_requests("PULL"):
            self.assert_is_success(summary.gql_status_objects[0])
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )
        self.assert_is_test_notification_as_gql_status_object(
            summary.gql_status_objects[1],
            severity="FOOBAR", parsed_severity="UNKNOWN"
        )

    def _test_notification_with_missing_data(
        self, del_key, severity="WARNING"
    ):
        notification = self.make_test_notification(severity=severity)
        del notification[del_key]
        in_notifications = [notification]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#NOTIFICATIONS#": json.dumps(in_notifications)
            }
        )

        self.assertEqual(len(summary.gql_status_objects), 2)
        if self._server.count_requests("PULL"):
            if severity == "WARNING":
                self.assert_is_success(summary.gql_status_objects[1])
                return_index = 0
            else:
                self.assert_is_success(summary.gql_status_objects[0])
                return_index = 1
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )
            return_index = 1
        return summary.gql_status_objects[return_index]

    def test_notification_with_missing_severity(self):
        status = self._test_notification_with_missing_data(
            "severity", severity="INFORMATION"
        )
        self.assert_is_test_notification_as_gql_status_object(
            status, severity=None, parsed_severity="UNKNOWN"
        )

    def test_notification_with_missing_category(self):
        status = self._test_notification_with_missing_data("category")
        self.assert_is_test_notification_as_gql_status_object(
            status, category=None, parsed_category="UNKNOWN"
        )

    def test_notification_with_missing_position(self):
        status = self._test_notification_with_missing_data("position")
        self.assert_is_test_notification_as_gql_status_object(
            status, raw_pos=None
        )

    def test_warn_with_missing_description(self):
        status = self._test_notification_with_missing_data("description")
        self.assert_is_test_notification_as_gql_status_object(
            status, description="warn: unknown warning",
        )

    def test_info_with_missing_description(self):
        status = self._test_notification_with_missing_data(
            "description", severity="INFORMATION"
        )
        self.assert_is_test_notification_as_gql_status_object(
            status, description="info: unknown notification",
            severity="INFORMATION", parsed_severity="INFORMATION",
        )

    def test_info_fallback_with_missing_description(self):
        status = self._test_notification_with_missing_data(
            "description", severity="BANANA"
        )
        self.assert_is_test_notification_as_gql_status_object(
            status, description="info: unknown notification",
            severity="BANANA", parsed_severity="UNKNOWN",
        )

    def test_multiple_notifications(self):
        notifications = [
            self.make_test_notification(i=1, severity="WARNING"),
            self.make_test_notification(i=2, severity="INFORMATION"),
            self.make_test_notification(i=3, severity="WARNING"),
            self.make_test_notification(i=4, severity="INFORMATION"),
        ]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={"#NOTIFICATIONS#": json.dumps(notifications)}
        )
        self.assertEqual(len(summary.gql_status_objects), 5)
        if self._server.count_requests("PULL"):
            # !!! following assertions only work with len(xyz_indexes) == 2 !!!
            warning_indexes = (0, 1)
            self.assert_is_success(summary.gql_status_objects[2])
            information_indexes = (3, 4)
        else:
            # For drivers that lazily PULL records.
            # They cannot know whether there is no data until the user
            # makes the driver try to pull records.
            self.assert_is_no_data_unknown_subclass(
                summary.gql_status_objects[0]
            )
            warning_indexes = (1, 2)
            information_indexes = (3, 4)
        # double check test invariants
        assert len(warning_indexes) == 2
        assert len(information_indexes) == 2
        try:
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[warning_indexes[0]], i=1
            )
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[warning_indexes[1]], i=3
            )
        except AssertionError:
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[warning_indexes[0]], i=3
            )
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[warning_indexes[1]], i=1
            )
        try:
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[information_indexes[0]], i=2,
                severity="INFORMATION", parsed_severity="INFORMATION"
            )
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[information_indexes[1]], i=4,
                severity="INFORMATION", parsed_severity="INFORMATION"
            )
        except AssertionError:
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[information_indexes[0]], i=4,
                severity="INFORMATION", parsed_severity="INFORMATION"
            )
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[information_indexes[1]], i=2,
                severity="INFORMATION", parsed_severity="INFORMATION"
            )


class TestSummaryGqlStatusObjects4x4Discard(
    _TestSummaryDiscardMixin,
    TestSummaryGqlStatusObjects4x4,
):
    def test_no_notifications(self):
        super().test_no_notifications()

    def test_no_notifications_no_data(self):
        super().test_no_notifications_no_data()

    def test_empty_notifications(self):
        super().test_empty_notifications()

    def test_warning(self):
        super().test_warning()

    def test_warning_no_data(self):
        super().test_warning_no_data()

    def test_information(self):
        super().test_information()

    def test_information_omitted_result(self):
        super().test_information_omitted_result()

    def test_information_no_data(self):
        super().test_information_no_data()

    def test_unknown_severity(self):
        super().test_unknown_severity()

    def test_notification_with_missing_severity(self):
        super().test_notification_with_missing_severity()

    def test_notification_with_missing_category(self):
        super().test_notification_with_missing_category()

    def test_notification_with_missing_position(self):
        super().test_notification_with_missing_position()

    def test_warn_with_missing_description(self):
        super().test_warn_with_missing_description()

    def test_info_with_missing_description(self):
        super().test_info_with_missing_description()

    def test_info_fallback_with_missing_description(self):
        super().test_info_fallback_with_missing_description()

    def test_multiple_notifications(self):
        super().test_multiple_notifications()


class TestSummaryGqlStatusObjects5x6(_TestSummaryGqlStatusObjectsBase):
    required_features = types.Feature.BOLT_5_6,
    version_folder = "v5x6",

    @classmethod
    def make_test_status(
        cls, status, condition, subcondition=None,
        classification="HINT", severity="WARNING",
        i=None, diag_record_extra=None,
    ):
        if diag_record_extra is None:
            diag_record_extra = {}
        if condition in ("successful completion", "no data"):
            prefix = "note: "
        elif condition == "informational":
            prefix = "info: "
        elif condition == "warning":
            prefix = "warn: "
        else:
            raise ValueError(f"Unknown condition: {condition}")

        if prefix not in ("info: ", "warn: ") and subcondition is not None:
            description = f"{prefix}{condition} - {subcondition}"
        else:
            description = f"{prefix}{condition}"

        if i is None:
            return {
                "gql_status": status,
                "status_description": description,
                "description": "The old notification description™.",
                "neo4j_code": "Neo.ClientNotification.Statement."
                              "CartesianProductWarning",
                "title": "This query builds a cartesian product between...",
                "diagnostic_record": {
                    "OPERATION": "",
                    "OPERATION_CODE": "0",
                    "CURRENT_SCHEMA": "/",
                    "_status_parameters": {"foo": 1},
                    "_severity": severity,
                    "_classification": classification,
                    "_position": {"column": 9, "offset": 8, "line": 1},
                    **diag_record_extra,
                }
            }
        else:
            return {
                "gql_status": status,
                "status_description": f"{description}. Bonus: {i}",
                "description": f"The old notification description™ {i}.",
                "neo4j_code": f"Neo.ClientNotification.Statement."
                              f"CartesianProductWarning{i}",
                "title": f"This query builds a cartesian product ... [{i}]",
                "diagnostic_record": {
                    "OPERATION": "",
                    "OPERATION_CODE": "0",
                    "CURRENT_SCHEMA": "/",
                    "_status_parameters": {"foo": 1 - i},
                    "_severity": severity,
                    "_classification": classification,
                    "_position": {"column": 9, "offset": 8, "line": 1 + i},
                    **diag_record_extra,
                }
            }

    def assert_is_test_gql_status_object(
        self, received_status, status, condition, subcondition=None,
        raw_classification="HINT", classification="HINT",
        severity="WARNING", raw_severity="WARNING", i=None,
        diag_record_extra=None,
    ):
        expected_status = self.make_test_status(
            status, condition, subcondition,
            classification=raw_classification, severity=raw_severity, i=i,
            diag_record_extra=diag_record_extra,
        )
        self.assertEqual(received_status.status_description,
                         expected_status["status_description"])
        self.assertEqual(received_status.position,
                         expected_status["diagnostic_record"]["_position"])
        self.assertEqual(received_status.classification, classification)
        self.assertEqual(received_status.raw_classification,
                         raw_classification)
        self.assertEqual(received_status.severity, severity)
        self.assertEqual(received_status.raw_severity, raw_severity)
        self.assertEqual(
            received_status.diagnostic_record,
            types.as_cypher_type(expected_status["diagnostic_record"]).value
        )
        self.assertEqual(received_status.is_notification, True)

    def assert_is_test_gql_status_object_raw(
        self, received_status, raw_status
    ):
        self.assertEqual(received_status.gql_status, raw_status["gql_status"])
        self.assertEqual(received_status.status_description,
                         raw_status["status_description"])
        diag_record = raw_status["diagnostic_record"] or {}
        self.assertEqual(received_status.position,
                         diag_record.get("_position"))
        self.assertEqual(received_status.raw_classification,
                         diag_record.get("_classification"))
        self.assertEqual(received_status.raw_severity,
                         diag_record.get("_severity"))
        self.assertEqual(
            received_status.diagnostic_record,
            types.as_cypher_type(raw_status["diagnostic_record"]).value
        )
        self.assertEqual(received_status.is_notification,
                         raw_status.get("neo4j_code") is not None)

    def test_success(self):
        summary = self._get_summary("empty_summary_type_r.script")
        self.assertEqual(len(summary.gql_status_objects), 1)
        self.assert_is_success(summary.gql_status_objects[0])

    def test_omitted_result(self):
        summary = self._get_summary(
            "empty_summary_type_r_omitted_result.script"
        )
        self.assertEqual(len(summary.gql_status_objects), 1)
        self.assert_is_omitted_result(summary.gql_status_objects[0])

    def test_no_data(self):
        summary = self._get_summary(
            "empty_summary_type_r_no_data.script"
        )
        self.assertEqual(len(summary.gql_status_objects), 1)
        self.assert_is_no_data(summary.gql_status_objects[0])

    def test_multiple_statuses(self):
        # order is not GQL compliant, but the driver should not touch it
        in_statuses = [
            self.make_test_status(
                "01N01", "warning",
                subcondition="test subcondition. Don't do this™.", i=1,
            ),
            SUCCESS_GQL_STATUS_OBJECT,
            NO_DATA_GQL_STATUS_OBJECT,
            self.make_test_status(
                "03N01", "informational",
                subcondition="test subcondition. Do that™.", i=2,
            ),
            self.make_test_status(
                "01N00", "warning",
                subcondition="test subcondition", i=3,
            ),
            self.make_test_status(
                "01N00", "warning",
                subcondition="test subcondition",
                classification="FOOBAR", severity=None, i=4,
            ),
            OMITTED_GQL_STATUS_OBJECT,
            self.make_test_status(
                "03N03", "informational",
                subcondition="test subcondition. Here we go again.™.", i=5,
            ),
        ]

        summary = self._get_summary(
            "summary_with_statuses.script",
            vars_={"#STATUSES#": json.dumps(in_statuses)}
        )

        self.assertEqual(len(summary.gql_status_objects), 8)
        self.assert_is_test_gql_status_object(
            summary.gql_status_objects[0], "01N01", "warning",
            subcondition="test subcondition. Don't do this™.", i=1,
        )
        self.assert_is_success(summary.gql_status_objects[1])
        self.assert_is_no_data(summary.gql_status_objects[2])
        self.assert_is_test_gql_status_object(
            summary.gql_status_objects[3], "03N01", "informational",
            subcondition="test subcondition. Do that™.", i=2,
        )
        self.assert_is_test_gql_status_object(
            summary.gql_status_objects[4], "01N00", "warning",
            subcondition="test subcondition", i=3,
        )
        self.assert_is_test_gql_status_object(
            summary.gql_status_objects[5], "01N00", "warning",
            subcondition="test subcondition",
            raw_classification="FOOBAR", classification="UNKNOWN",
            raw_severity=None, severity="UNKNOWN", i=4,
        )
        self.assert_is_omitted_result(summary.gql_status_objects[6])
        self.assert_is_test_gql_status_object(
            summary.gql_status_objects[7], "03N03", "informational",
            subcondition="test subcondition. Here we go again.™.", i=5,
        )

    def test_keeps_garbage_in_diagnostic_record(self):
        in_statuses = [
            self.make_test_status(
                "01N01", "warning", i=1, diag_record_extra={"_🤡": "🎈"},
            ),
        ]
        summary = self._get_summary(
            "summary_with_statuses.script",
            vars_={"#STATUSES#": json.dumps(in_statuses)}
        )
        self.assertEqual(len(summary.gql_status_objects), 1)
        self.assert_is_test_gql_status_object(
            summary.gql_status_objects[0], "01N01", "warning", i=1,
            diag_record_extra={"_🤡": "🎈"},
        )

    @staticmethod
    def _set_diagnostic_record(status, value):
        status["diagnostic_record"] = value
        return status

    @staticmethod
    def _set_diagnostic_record_entry(status, key, value):
        status["diagnostic_record"][key] = value
        return status

    @staticmethod
    def _del_from_diag_record(status, *keys):
        for key in keys:
            del status["diagnostic_record"][key]
        return status

    @staticmethod
    def _del(status, *keys):
        for key in keys:
            del status[key]
        return status

    def test_fill_diagnostic_record_values(self):
        def fill_expected_status(status_):
            diagnostic_record = status_.setdefault("diagnostic_record", {})
            if not isinstance(diagnostic_record, dict):
                return status_
            if "OPERATION" not in diagnostic_record:
                diagnostic_record["OPERATION"] = ""
            if "OPERATION_CODE" not in diagnostic_record:
                diagnostic_record["OPERATION_CODE"] = "0"
            if "CURRENT_SCHEMA" not in diagnostic_record:
                diagnostic_record["CURRENT_SCHEMA"] = "/"
            return status_

        in_statuses = [
            # missing diagnostic record should be filled with defaults
            self._del(
                self.make_test_status("01N01", "warning", i=1),
                "diagnostic_record"
            ),
            # missing OPERATION key should be filled with ""
            self._del_from_diag_record(
                self.make_test_status("01N01", "warning", i=2),
                "OPERATION"
            ),
            # missing OPERATION_CODE key should be filled with "0"
            self._del_from_diag_record(
                self.make_test_status("01N01", "warning", i=3),
                "OPERATION_CODE"
            ),
            # missing CURRENT_SCHEMA key should be filled with "/"
            self._del_from_diag_record(
                self.make_test_status("01N01", "warning", i=4),
                "CURRENT_SCHEMA"
            ),
            self._del_from_diag_record(
                self.make_test_status("01N01", "warning", i=5),

                "OPERATION", "OPERATION_CODE", "CURRENT_SCHEMA"
            ),
            # None values should be kept as is
            self._set_diagnostic_record_entry(
                self.make_test_status("01N01", "warning", i=6),
                "OPERATION", None
            ),
            self._set_diagnostic_record_entry(
                self.make_test_status("01N01", "warning", i=7),
                "OPERATION_CODE", None
            ),
            self._set_diagnostic_record_entry(
                self.make_test_status("01N01", "warning", i=8),
                "CURRENT_SCHEMA", None
            ),
            # invalid/unexpected types should be kept as is
            self._set_diagnostic_record_entry(
                self.make_test_status("01N01", "warning", i=9),
                "OPERATION", [123, None]
            ),
            self._set_diagnostic_record_entry(
                self.make_test_status("01N01", "warning", i=10),
                "OPERATION_CODE", {"foo": "bar", "baz": 42.2}
            ),
            self._set_diagnostic_record_entry(
                self.make_test_status("01N01", "warning", i=11),
                "CURRENT_SCHEMA", False
            ),
            # only the keys above have defaults are filled
            self._del_from_diag_record(
                self.make_test_status("01N01", "warning", i=12),
                "_status_parameters", "_severity",
                "_classification", "_position",
            ),
        ]

        summary = self._get_summary(
            "summary_with_statuses.script",
            vars_={"#STATUSES#": json.dumps(in_statuses)}
        )
        received_statuses = summary.gql_status_objects

        expected_statuses = list(map(fill_expected_status, in_statuses))

        self.assertEqual(len(received_statuses), len(expected_statuses))
        for received, expected in zip(received_statuses, expected_statuses):
            self.assert_is_test_gql_status_object_raw(received, expected)


class TestSummaryGqlStatusObjects5x6Discard(
    _TestSummaryDiscardMixin,
    TestSummaryGqlStatusObjects5x6,
):
    def test_success(self):
        super().test_success()

    def test_omitted_result(self):
        super().test_omitted_result()

    def test_no_data(self):
        super().test_no_data()

    def test_multiple_statuses(self):
        super().test_multiple_statuses()

    def test_keeps_garbage_in_diagnostic_record(self):
        super().test_keeps_garbage_in_diagnostic_record()

    def test_fill_diagnostic_record_values(self):
        super().test_fill_diagnostic_record_values()


class TestSummaryPlan4x4(_TestSummaryBase):
    required_features = (types.Feature.BOLT_4_4,)
    version_folder = ("v4x4",)

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
                "EstimatedRows": 1.5,
                "planner": "COST",
            },
            "operatorType": "ProduceResults@neo4j",
            "children": [
                {
                    "args": {
                        "Details": "(n)",
                        "EstimatedRows": 1.5,
                        "PipelineInfo": "Fused in Pipeline 0",
                    },
                    "operatorType": "Create@neo4j",
                    "children": [],
                    "identifiers": ["n"],
                }
            ],
            "identifiers": ["n"],
        }
        summary = self._get_summary(
            "summary_with_plan.script",
            vars_={"#PLAN#": json.dumps(plan)},
        )
        self.assert_plan_equal(summary.plan, plan)

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
                "Rows": 1,
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
                        "PageCacheHits": 0,
                    },
                    "pageCacheMisses": 0,
                    "children": [],
                    "dbHits": 1,
                    "identifiers": ["n"],
                    "operatorType": "Create@neo4j",
                    "time": 0,
                    "rows": 1,
                    "pageCacheHitRatio": 0.1,
                    "pageCacheHits": 0,
                }
            ],
            "dbHits": 1,
            "identifiers": ["n"],
            "operatorType": "ProduceResults@neo4j",
            "rows": 1,
        }
        summary = self._get_summary(
            "summary_with_profile.script",
            vars_={"#PROFILE#": json.dumps(profile)},
        )
        self.assert_plan_equal(summary.profile, profile)


class TestSummaryPlanDiscard4x4(_TestSummaryDiscardMixin, TestSummaryPlan4x4):
    def test_plan(self):
        super().test_plan()

    def test_profile(self):
        super().test_profile()


class TestSummaryPlan6x0(_TestSummaryBase):
    required_features = (types.Feature.BOLT_6_0,)
    version_folder = ("v6x0",)

    def test_plan(self):
        plan = {
            "args": {
                "planner-impl": "IDP",
                "string-representation": (  # noqa: PAR001
                    "Cypher 5"
                    "\n"
                    "\nPlanner COST"
                    "\n"
                    "\nRuntime PIPELINED"
                    "\n"
                    "\nRuntime version 2026.01"
                    "\n"
                    "\nBatch size 128"
                    "\n"
                    "\n+-----------------+----+---------+----------------+---------------------+"  # noqa: E501
                    "\n| Operator        | Id | Details | Estimated Rows | Pipeline            |"  # noqa: E501
                    "\n+-----------------+----+---------+----------------+---------------------+"  # noqa: E501
                    "\n| +ProduceResults |  0 | n       |             10 |                     |"  # noqa: E501
                    "\n| |               +----+---------+----------------+                     |"  # noqa: E501
                    "\n| +AllNodesScan   |  1 | n       |             10 | Fused in Pipeline 0 |"  # noqa: E501
                    "\n+-----------------+----+---------+----------------+---------------------+"  # noqa: E501
                    "\n"
                    "\nTotal database accesses: ?"
                    "\n"
                ),
                "runtime": "PIPELINED",
                "runtime-impl": "PIPELINED",
                "version": "5",
                "batch-size": 128,
                "Details": "n",
                "planner-version": "2026.01",
                "PipelineInfo": "Fused in Pipeline 0",
                "runtime-version": "2026.01",
                "Id": 0,
                "EstimatedRows": 10.0,
                "planner": "COST",
            },
            "children": [
                {
                    "args": {
                        "Details": "n",
                        "Id": 1,
                        "EstimatedRows": 10.0,
                        "PipelineInfo": "Fused in Pipeline 0",
                    },
                    "identifiers": ["n"],
                    "operatorType": "AllNodesScan@neo4j",
                }
            ],
            "identifiers": ["n"],
            "operatorType": "ProduceResults@neo4j",
        }
        summary = self._get_summary(
            "summary_with_plan.script",
            vars_={"#PLAN#": json.dumps(plan)},
        )
        self.assert_plan_equal(summary.plan, plan)

    def test_profile(self):
        profile = {
            "args": {
                "GlobalMemory": 312,
                "planner-impl": "IDP",
                "Memory": 0,
                "string-representation": (  # noqa: PAR001
                    "Cypher 5\n"
                    "\n"
                    "Planner COST\n"
                    "\n"
                    "Runtime PIPELINED\n"
                    "\n"
                    "Runtime version 2026.01\n"
                    "\n"
                    "Batch size 128\n"
                    "\n"
                    "+-----------------+----+---------+----------------+------+---------+----------------+------------------------+-----------+---------------------+\n"  # noqa: E501
                    "| Operator        | Id | Details | Estimated Rows | Rows | DB Hits | Memory (Bytes) | Page Cache Hits/Misses | Time (ms) | Pipeline            |\n"  # noqa: E501
                    "+-----------------+----+---------+----------------+------+---------+----------------+------------------------+-----------+---------------------+\n"  # noqa: E501
                    "| +ProduceResults |  0 | n       |             10 |    0 |       0 |              0 |                        |           |                     |\n"  # noqa: E501
                    "| |               +----+---------+----------------+------+---------+----------------+                        |           |                     |\n"  # noqa: E501
                    "| +AllNodesScan   |  1 | n       |             10 |    0 |       1 |            248 |                    0/0 |     0.274 | Fused in Pipeline 0 |\n"  # noqa: E501
                    "+-----------------+----+---------+----------------+------+---------+----------------+------------------------+-----------+---------------------+\n"  # noqa: E501
                    "\n"
                    "Total database accesses: 1, total allocated memory: 312\n"
                    ""
                ),
                "runtime": "PIPELINED",
                "runtime-impl": "PIPELINED",
                "version": "5",
                "DbHits": 0,
                "batch-size": 128,
                "Details": "n",
                "planner-version": "2026.01",
                "PipelineInfo": "Fused in Pipeline 0",
                "runtime-version": "2026.01",
                "Id": 0,
                "EstimatedRows": 10.0,
                "planner": "COST",
                "Rows": 0,
            },
            "children": [
                {
                    "args": {
                        "Details": "n",
                        "PipelineInfo": "Fused in Pipeline 0",
                        "Memory": 248,
                        "Time": 273643,
                        "Id": 1,
                        "EstimatedRows": 10.0,
                        "PageCacheMisses": 0,
                        "DbHits": 1,
                        "Rows": 0,
                        "PageCacheHits": 0,
                    },
                    "pageCacheMisses": 0,
                    "dbHits": 1,
                    "identifiers": ["n"],
                    "operatorType": "AllNodesScan@neo4j",
                    "time": 273643,
                    "rows": 0,
                    "pageCacheHitRatio": 0.0,
                    "pageCacheHits": 0,
                }
            ],
            "dbHits": 0,
            "identifiers": ["n"],
            "operatorType": "ProduceResults@neo4j",
            "rows": 0,
        }
        summary = self._get_summary(
            "summary_with_profile.script",
            vars_={"#PROFILE#": json.dumps(profile)},
        )
        self.assert_plan_equal(summary.profile, profile)

    def test_profile_no_db_hit(self):
        profile = {
            "args": {},
            "children": [
                {
                    "args": {},
                    "identifiers": ["n"],
                    "operatorType": "AllNodesScan@neo4j",
                    "time": 273643,
                    "rows": 1,
                    "pageCacheHitRatio": 1.2,
                    "pageCacheHits": 3,
                    "pageCacheMisses": 4,
                }
            ],
            "rows": 0,
            "pageCacheHitRatio": 0.0,
            "pageCacheHits": 0,
            "pageCacheMisses": 0,
            "time": 123456,
            "identifiers": ["n"],
            "operatorType": "ProduceResults@neo4j",
        }
        if not self.driver_supports_features(
            types.Feature.API_SUMMARY_PROFILE_OPTIONAL_STATS
        ):
            # cutting unified drivers some extra slack:
            # some drivers assume that the top-level profile element never
            # contains stats
            profile = {
                "args": {},
                "children": [profile],
                "identifiers": ["coolio!"],
                "operatorType": "TestKitWrapperForStrictDrivers",
            }
        summary = self._get_summary(
            "summary_with_profile.script",
            vars_={"#PROFILE#": json.dumps(profile)},
        )
        self.assert_plan_equal(summary.profile, profile)

    def test_profile_no_time(self):
        profile = {
            "args": {},
            "children": [
                {
                    "args": {},
                    "identifiers": ["n"],
                    "operatorType": "AllNodesScan@neo4j",
                    "dbHits": 0,
                    "rows": 0,
                    "pageCacheHitRatio": 1.2,
                    "pageCacheHits": 3,
                    "pageCacheMisses": 4,
                }
            ],
            "dbHits": 123,
            "rows": 1,
            "pageCacheHitRatio": 0.0,
            "pageCacheHits": 0,
            "pageCacheMisses": 0,
            "identifiers": ["n"],
            "operatorType": "ProduceResults@neo4j",
        }
        if not self.driver_supports_features(
            types.Feature.API_SUMMARY_PROFILE_OPTIONAL_STATS
        ):
            # cutting unified drivers some extra slack:
            # some drivers assume that the top-level profile element never
            # contains stats
            profile = {
                "args": {},
                "children": [profile],
                "identifiers": ["coolio!"],
                "operatorType": "TestKitWrapperForStrictDrivers",
            }
        summary = self._get_summary(
            "summary_with_profile.script",
            vars_={"#PROFILE#": json.dumps(profile)},
        )
        self.assert_plan_equal(summary.profile, profile)

    def test_profile_no_rows(self):
        profile = {
            "args": {},
            "children": [
                {
                    "args": {},
                    "identifiers": ["n"],
                    "operatorType": "AllNodesScan@neo4j",
                    "dbHits": 123,
                    "time": 0,
                    "pageCacheHitRatio": 0.0,
                    "pageCacheHits": 0,
                    "pageCacheMisses": 0,
                }
            ],
            "dbHits": 0,
            "time": 0,
            "pageCacheHitRatio": 1.2,
            "pageCacheHits": 3,
            "pageCacheMisses": 4,
            "identifiers": ["n"],
            "operatorType": "ProduceResults@neo4j",
        }
        if not self.driver_supports_features(
            types.Feature.API_SUMMARY_PROFILE_OPTIONAL_STATS
        ):
            # cutting unified drivers some extra slack:
            # some drivers assume that the top-level profile element never
            # contains stats
            profile = {
                "args": {},
                "children": [profile],
                "identifiers": ["coolio!"],
                "operatorType": "TestKitWrapperForStrictDrivers",
            }
        summary = self._get_summary(
            "summary_with_profile.script",
            vars_={"#PROFILE#": json.dumps(profile)},
        )
        self.assert_plan_equal(summary.profile, profile)

    def test_profile_no_page_cache_stats(self):
        profile = {
            "args": {},
            "children": [
                {
                    "args": {},
                    "identifiers": ["n"],
                    "operatorType": "AllNodesScan@neo4j",
                    "dbHits": 123,
                    "rows": 3456,
                    "time": 0,
                }
            ],
            "dbHits": 0,
            "rows": 5,
            "time": 0,
            "identifiers": ["n"],
            "operatorType": "ProduceResults@neo4j",
        }
        if not self.driver_supports_features(
            types.Feature.API_SUMMARY_PROFILE_OPTIONAL_STATS
        ):
            # cutting unified drivers some extra slack:
            # some drivers assume that the top-level profile element never
            # contains stats
            profile = {
                "args": {},
                "children": [profile],
                "identifiers": ["coolio!"],
                "operatorType": "TestKitWrapperForStrictDrivers",
            }
        summary = self._get_summary(
            "summary_with_profile.script",
            vars_={"#PROFILE#": json.dumps(profile)},
        )
        self.assert_plan_equal(summary.profile, profile)


class TestSummaryPlanDiscard6x0(_TestSummaryDiscardMixin, TestSummaryPlan6x0):
    def test_plan(self):
        super().test_plan()

    def test_profile(self):
        super().test_profile()


class TestSummaryCounters(_TestSummaryBase):
    required_features = types.Feature.BOLT_4_4,
    version_folder = "v4x4",

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
            "partial_summary_constraints_removed.script"
        )
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


class TestSummaryCountersDiscard(
    _TestSummaryDiscardMixin,
    TestSummaryCounters
):
    def test_empty_summary(self):
        super().test_empty_summary()

    def test_full_summary_no_flags(self):
        super().test_full_summary_no_flags()

    def test_no_summary(self):
        super().test_no_summary()

    def test_partial_summary_constraints_added(self):
        super().test_partial_summary_constraints_added()

    def test_partial_summary_constraints_removed(self):
        super().test_partial_summary_constraints_removed()

    def test_partial_summary_contains_system_updates(self):
        super().test_partial_summary_contains_system_updates()

    def test_partial_summary_contains_updates(self):
        super().test_partial_summary_contains_updates()

    def test_partial_summary_not_contains_system_updates(self):
        super().test_partial_summary_not_contains_system_updates()

    def test_partial_summary_not_contains_updates(self):
        super().test_partial_summary_not_contains_updates()

    def test_partial_summary_indexes_added(self):
        super().test_partial_summary_indexes_added()

    def test_partial_summary_indexes_removed(self):
        super().test_partial_summary_indexes_removed()

    def test_partial_summary_labels_added(self):
        super().test_partial_summary_labels_added()

    def test_partial_summary_labels_removed(self):
        super().test_partial_summary_labels_removed()

    def test_partial_summary_nodes_created(self):
        super().test_partial_summary_nodes_created()

    def test_partial_summary_nodes_deleted(self):
        super().test_partial_summary_nodes_deleted()

    def test_partial_summary_properties_set(self):
        super().test_partial_summary_properties_set()

    def test_partial_summary_relationships_created(self):
        super().test_partial_summary_relationships_created()

    def test_partial_summary_relationships_deleted(self):
        super().test_partial_summary_relationships_deleted()

    def test_partial_summary_system_updates(self):
        super().test_partial_summary_system_updates()
