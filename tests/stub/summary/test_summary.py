import json
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_dns_resolved_server_address,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class _TestSummaryBase(TestkitTestCase):
    """Test result summary contents."""

    full_notifications_feat = types.Feature.API_DRIVER_NOTIFICATIONS_CONFIG
    version_folder = ()

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
        script_path = self.script_path(*self.version_folder, script)
        self._server.start(path=script_path, vars_=vars_)
        session = driver.session("w", fetch_size=1000)
        try:
            yield session
        finally:
            session.close()
            driver.close()
            self._server.reset()

    def _get_summary(self, script, vars_=None):
        with self._get_session(script, vars_=vars_) as session:
            result = session.run("RETURN 1 AS n")
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


class TestSummaryNotifications4x4(_TestSummaryBase):
    required_features = types.Feature.BOLT_4_4,
    version_folder = "v4x4",

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
            "severity": "WARNING",
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
            self.assertEqual(summary.notifications, notifications)
        else:
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
            self.assertEqual(summary.notifications, notifications)
        else:
            self.assertEqual(summary.notifications, notifications)


SUCCESS_GQL_STATUS_OBJECT = {
    "gql_status": "00000",
    "status_description": "note: successful completion",
    "diagnostic_record": {
        "OPERATION": "",
        "OPERATION_CODE": "0",
        "CURRENT_SCHEMA": "/",
    },
}

NO_DATA_GQL_STATUS_OBJECT = {
    "gql_status": "02000",
    "status_description": "note: no data",
    "diagnostic_record": {
        "OPERATION": "",
        "OPERATION_CODE": "0",
        "CURRENT_SCHEMA": "/",
    },
}


class TestSummaryNotifications5x5(_TestSummaryBase):
    required_features = types.Feature.BOLT_5_5,
    version_folder = "v5x5",

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
        in_statuses = [
            {
                "gql_status": "01N01",
                "status_description": "warn: warning - test subcat. "
                                      "Don't do this™.",
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
            "summary_with_notifications.script",
            vars_={
                "#STATUSES#": json.dumps(in_statuses)
            }
        )
        out_notifications = [{
            "severity": "WARNING",
            "description": "warn: warning - test subcat. Don't do this™.",
            "code": "Neo.ClientNotification.Foo.Bar",
            "position": {"column": 9, "offset": 8, "line": 1},
            "title": "Legacy warning title",
            "rawSeverityLevel": "WARNING",
            "severityLevel": "WARNING",
            "rawCategory": "GENERIC",
            "category": "GENERIC",
        }]

        self.assertEqual(summary.notifications, out_notifications)

    def test_full_notifications_unknown_fields(self):
        in_statuses = [
            {
                "gql_status": "01N01",
                "status_description": "warn: warning - test subcat. "
                                      "Don't do this™.",
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
                },
            },
            SUCCESS_GQL_STATUS_OBJECT
        ]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={
                "#STATUSES#": json.dumps(in_statuses)
            }
        )
        out_notifications = [{
            "severity": "ANYSEV",
            "description": "warn: warning - test subcat. Don't do this™.",
            "code": "Neo.ClientNotification.Foo.Bar",
            "position": {"column": 9, "offset": 8, "line": 1},
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
                    f"warn: warning - test subcat. Don't do this™ {i}.",
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
                },
            }
            for i in range(1, 4)
        ]
        in_statuses = [
            NO_DATA_GQL_STATUS_OBJECT,
            *in_statuses
        ]
        summary = self._get_summary(
            "summary_with_notifications.script",
            vars_={"#STATUSES#": json.dumps(in_statuses)}
        )
        out_notifications = [
            {
                "severity": "WARNING",
                "description":
                    f"warn: warning - test subcat. Don't do this™ {i}.",
                "code": f"Neo.ClientNotification.Foo.Bar{i}",
                "position": {"column": 9, "offset": 8, "line": 1 + i},
                "title": f"Legacy warning title {i}",
                "rawSeverityLevel": "WARNING",
                "severityLevel": "WARNING",
                "rawCategory": "GENERIC",
                "category": "GENERIC",
            }
            for i in range(1, 4)
        ]
        self.assertEqual(summary.notifications, out_notifications)


class _TestSummaryGqlStatusObjectsBase(_TestSummaryBase):
    def assert_is_non_notification_status(self, status):
        self.assertEqual(status.position,
                         {"column": -1, "offset": -1, "line": -1})
        self.assertEqual(status.classification, "UNKNOWN")
        self.assertEqual(status.raw_classification, "")
        self.assertEqual(status.severity, "UNKNOWN")
        self.assertEqual(status.raw_severity, "")
        self.assertEqual(status.diagnostic_record, {
            "OPERATION": types.CypherString(""),
            "OPERATION_CODE": types.CypherString("0"),
            "CURRENT_SCHEMA": types.CypherString("/"),
            "_severity": types.CypherString(""),
            "_classification": types.CypherString(""),
            "_position": types.CypherMap({
                "column": types.CypherInt(-1),
                "offset": types.CypherInt(-1),
                "line": types.CypherInt(-1),
            }),
            "_status_parameters": types.CypherMap({}),
        })
        self.assertEqual(status.is_notification, False)

    def assert_is_success(self, status):
        self.assert_is_non_notification_status(status)
        self.assertEqual(status.gql_status, "00000")
        self.assertEqual(status.status_description,
                         "note: successful completion")

    def assert_is_omitted_result(self, status):
        self.assert_is_non_notification_status(status)
        self.assertEqual(status.gql_status, "00001")
        self.assertEqual(status.status_description,
                         "note: successful completion - omitted result")

    def assert_is_no_data(self, status):
        self.assert_is_non_notification_status(status)
        self.assertEqual(status.gql_status, "02000")
        self.assertEqual(status.status_description,
                         "note: no data")

    def assert_is_no_data_unknown_subclass(self, status):
        self.assert_is_non_notification_status(status)
        self.assertEqual(status.gql_status, "02N42")
        self.assertEqual(status.status_description,
                         "note: no data - unknown subcondition")


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
    def make_test_notification(cls, i=None, severity="WARNING"):
        if i is None:
            return {
                "severity": severity,
                "category": "HINT",
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
                "category": "HINT",
                "description": "If a part of a query contains multiple "
                               f"disconnected patterns, ... [{i}]",
                "code": "Neo.ClientNotification.Statement."
                        f"CartesianProductWarning{i}",
                "position": {"column": 9, "offset": 8, "line": 1 + i},
                "title": f"This query builds a cartesian product ... [{i}]"
            }

    def assert_is_test_notification_as_gql_status_object(
        self, status, i=None, severity="WARNING", parsed_severity="WARNING"
    ):
        raw_notification = self.make_test_notification(i, severity)
        raw_pos = raw_notification["position"]
        assert isinstance(status, types.GqlStatusObject)
        if severity == "WARNING":
            self.assertEqual(status.gql_status, "01N42")
        else:
            self.assertEqual(status.gql_status, "03N42")
        self.assertEqual(status.status_description,
                         raw_notification["description"])
        self.assertEqual(status.position, raw_pos)
        self.assertEqual(status.classification, "HINT")
        self.assertEqual(status.raw_classification,
                         raw_notification["category"])
        self.assertEqual(status.severity, parsed_severity)
        self.assertEqual(status.raw_severity,
                         raw_notification["severity"])
        self.assertEqual(status.diagnostic_record, {
            "OPERATION": types.CypherString(""),
            "OPERATION_CODE": types.CypherString("0"),
            "CURRENT_SCHEMA": types.CypherString("/"),
            "_severity": types.CypherString(""),
            "_classification": types.CypherString(""),
            "_position": types.CypherMap({
                "column": types.CypherInt(raw_pos["column"]),
                "offset": types.CypherInt(raw_pos["offset"]),
                "line": types.CypherInt(raw_pos["line"]),
            }),
            "_status_parameters": types.CypherMap({}),
        })
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
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[0],
                severity="INFORMATION",
                parsed_severity="INFORMATION"
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
                summary.gql_status_objects[1],
                severity="INFORMATION",
                parsed_severity="INFORMATION"
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
            severity="INFORMATION",
            parsed_severity="INFORMATION"
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
            self.assert_is_test_notification_as_gql_status_object(
                summary.gql_status_objects[0],
                severity="INFORMATION",
                parsed_severity="INFORMATION"
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
                summary.gql_status_objects[1],
                severity="FOOBAR",
                parsed_severity="UNKNOWN"
            )

    def test_notification_with_missing_data(self):
        notification = self.make_test_notification()
        for missing_key in ("severity", "category", "code", "position"):
            with self.subTest(missing_key=missing_key):
                notification = notification.copy()
                del notification[missing_key]
                in_notifications = [notification]
                summary = self._get_summary(
                    "summary_with_notifications.script",
                    vars_={
                        "#NOTIFICATIONS#": json.dumps(in_notifications)
                    }
                )

                # incomplete notifications should be ignored in the
                # GqlStatusObject list
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
            information_indexes = (2, 3)
            self.assert_is_success(summary.gql_status_objects[4])
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


class TestSummaryGqlStatusObjects5x5(_TestSummaryGqlStatusObjectsBase):
    def test_todo(self):
        self.assertTrue(False, "TODO: Implement test")


class TestSummaryPlan(_TestSummaryBase):
    required_features = types.Feature.BOLT_4_4,
    version_folder = "v4x4",

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
