from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.session_notification_filters.notification_filters_base import (
    NotificationFiltersBase,
)
from tests.stub.shared import StubServer


class TestDriverNotificationFilters(TestkitTestCase, NotificationFiltersBase):
    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.API_DRIVER_NOTIFICATION_FILTERS)
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

    def _run_test_get_summary(self, filters, script_params,
                              script_name="driver_notification_filters.script"
                              ):
        self._server.start(self.script_path(script_name),
                           vars_=script_params)
        self._driver = self._new_driver(filters)
        session = self._driver.session("w", database="neo4j")
        cursor = session.run("CREATE (:node)")
        return cursor.consume()

    def test_default_server_filters(self):
        no_filter_script = "driver_default_notification_filters.script"
        self._run_test_get_summary(None, None, no_filter_script)
        self._server.done()

    def test_filter_notifications(self):
        for cfg in super().configs():
            with self.subTest(name=cfg["filters"]):
                self._run_test_get_summary(cfg["filters"], cfg["params"])
                self._server.done()
