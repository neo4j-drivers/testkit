import json

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestNotificationFilters(TestkitTestCase):
    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.NOTIFICATION_FILTERS)
    script_name = "summary_with_notifications.script"
    map_data = None

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._driver = None

    def tearDown(self):
        self._server.reset()

        if self._driver:
            self._driver.close()
        return super().tearDown()

    def _new_driver(self, notifications):
        uri = "bolt://%s" % self._server.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        return Driver(self._backend, uri, auth,
                      notification_filters=notifications)

    def _run_test_get_summary(self, filters, script_params):
        self._server.start(self.script_path(self.script_name),
                           vars_=script_params)
        self._driver = self._new_driver(filters)
        session = self._driver.session("w", database="neo4j")
        cursor = session.run("CREATE (:node)")
        return cursor.consume()

    def _test_single_notification_filter(self, cfg):
        notifications = json.dumps([{
            "severity": cfg["in_sev"],
            "category": cfg["in_cat"],
            "description": "ignore me",
            "code": "MadeUp",
            "title": "nonsense."
        }])
        script_params = {
            "#FILTERS#": cfg["bolt_filters"],
            "#EMIT#": notifications
        }
        summary = self._run_test_get_summary(cfg["tk_filters"], script_params)
        self._server.done()

        # todo consider removing from tests, we aren't testing mapping
        #  the server shutting down successfully is proof we configured.
        expected_notifications = [{
            "severity": cfg["in_sev"],
            "severityLevel": cfg["enum_sev"],
            "rawCategory": cfg["in_cat"],
            "category": cfg["enum_cat"],
            "description": "ignore me",
            "code": "MadeUp",
            "title": "nonsense."
        }]
        self.assertListEqual(expected_notifications, summary.notifications)

    def test_all_filter_notifications(self):
        cfgs = [{
            "name": "all",
            "tk_filters": ["ALL.ALL"],
            "in_sev": "WARNING",
            "enum_sev": "WARNING",
            "in_cat": "DEPRECATION",
            "enum_cat": "DEPRECATION",
            "bolt_filters": '["*.*"]',
        }, {
            "name": "all.query",
            "tk_filters": ["ALL.QUERY"],
            "in_sev": "WARNING",
            "in_cat": "QUERY",
            "enum_sev": "WARNING",
            "enum_cat": "QUERY",
            "bolt_filters": '["*.QUERY"]',
        }]
        for cfg in cfgs:
            with self.subTest(name=cfg["name"]):
                self._test_single_notification_filter(cfg)

    def test_warning_filter_notifications(self):
        cfgs = [{
            "name": "warning.all",
            "tk_filters": ["WARNING.ALL"],
            "in_sev": "WARNING",
            "in_cat": "DEPRECATION",
            "enum_sev": "WARNING",
            "enum_cat": "DEPRECATION",
            "bolt_filters": '["WARNING.*"]',
        }]

        # add cases
        for cfg in cfgs:
            with self.subTest(name=cfg["name"]):
                self._test_single_notification_filter(cfg)

    def test_info_filter_notifications(self):
        cfgs = [{
            "name": "info.all",
            "tk_filters": ["INFORMATION.ALL"],
            "in_sev": "INFORMATION",
            "in_cat": "DEPRECATION",
            "enum_sev": "INFORMATION",
            "enum_cat": "DEPRECATION",
            "bolt_filters": '["INFORMATION.*"]',
        }]
        # add cases
        for cfg in cfgs:
            with self.subTest(name=cfg["name"]):
                self._test_single_notification_filter(cfg)
