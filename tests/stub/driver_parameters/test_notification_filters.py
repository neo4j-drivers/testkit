import json

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestNotificationFilters(TestkitTestCase):
    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.NOTIFICATION_FILTERS)

    map_data = None
    _auth = types.AuthorizationToken("basic", principal="neo4j",
                                    credentials="pass")

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._uri = "bolt://%s" % self._server.address
        self._driver = None

    def tearDown(self):
        self._server.reset()

        if self._driver:
            self._driver.close()
        return super().tearDown()

    def _new_driver(self, notifications):
        return Driver(self._backend, self._uri, self._auth,
                      notification_filters=notifications)

    def _run_test_get_summary(self, filters, script_params):
        script_name = "notification_filters.script"
        self._server.start(self.script_path(script_name),
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
        self._run_test_get_summary(cfg["tk_filters"], script_params)
        self._server.done()

    def test_no_filter_notifications(self):
        cfg = {
            "tk_filters": ["None"],
            "in_sev": "WARNING",
            "in_cat": "DEPRECATION",
            "bolt_filters": '[]',
        }
        self._test_single_notification_filter(cfg)

    def test_default_server_filters(self):
        no_filter_script = "default_notification_filters.script"
        self._server.start(self.script_path(no_filter_script))
        self._driver = Driver(self._backend, self._uri, self._auth)

        session = self._driver.session("w", database="neo4j")
        cursor = session.run("CREATE (:node)")
        cursor.consume()
        self._server.done()

    def test_filter_notifications(self):
        cfgs = [{
            "name": "all",
            "tk_filters": ["ALL.ALL"],
            "in_sev": "WARNING",
            "in_cat": "DEPRECATION",
            "bolt_filters": '["*.*"]',
        }, {
            "name": "all.query",
            "tk_filters": ["ALL.QUERY"],
            "in_sev": "WARNING",
            "in_cat": "QUERY",
            "bolt_filters": '["*.QUERY"]',
        }, {
            "name": "warning.all",
            "tk_filters": ["WARNING.ALL"],
            "in_sev": "WARNING",
            "in_cat": "DEPRECATION",
            "bolt_filters": '["WARNING.*"]',
        }, {
            "name": "warning.deprecation",
            "tk_filters": ["WARNING.DEPRECATION"],
            "in_sev": "WARNING",
            "in_cat": "DEPRECATION",
            "bolt_filters": '["WARNING.DEPRECATION"]',
        }, {
            "name": "warning.hint",
            "tk_filters": ["WARNING.HINT"],
            "in_sev": "WARNING",
            "in_cat": "HINT",
            "bolt_filters": '["WARNING.HINT"]',
        }, {
            "name": "warning.query",
            "tk_filters": ["WARNING.QUERY"],
            "in_sev": "WARNING",
            "in_cat": "QUERY",
            "bolt_filters": '["WARNING.QUERY"]',
        }, {
            "name": "warning.unsupported",
            "tk_filters": ["WARNING.UNSUPPORTED"],
            "in_sev": "WARNING",
            "in_cat": "UNSUPPORTED",
            "bolt_filters": '["WARNING.UNSUPPORTED"]',
        },{
            "name": "info.all",
            "tk_filters": ["INFORMATION.ALL"],
            "in_sev": "INFORMATION",
            "in_cat": "DEPRECATION",
            "bolt_filters": '["INFORMATION.*"]',
        },{
            "name": "info.runtime",
            "tk_filters": ["INFORMATION.RUNTIME"],
            "in_sev": "INFORMATION",
            "in_cat": "RUNTIME",
            "bolt_filters": '["INFORMATION.RUNTIME"]',
        },{
            "name": "info.query",
            "tk_filters": ["INFORMATION.QUERY"],
            "in_sev": "INFORMATION",
            "in_cat": "QUERY",
            "bolt_filters": '["INFORMATION.QUERY"]',
        },{
            "name": "info.performance",
            "tk_filters": ["INFORMATION.PERFORMANCE"],
            "in_sev": "INFORMATION",
            "in_cat": "PERFORMANCE",
            "bolt_filters": '["INFORMATION.PERFORMANCE"]',
        }]
        for cfg in cfgs:
            with self.subTest(name=cfg["name"]):
                self._test_single_notification_filter(cfg)


