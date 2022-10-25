from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.notification_filters.notification_filters_base import (
    NotificationFiltersBase,
)
from tests.stub.shared import StubServer


class TestSessionNotificationFilters(TestkitTestCase, NotificationFiltersBase):
    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.API_SESSION_NOTIFICATION_FILTERS)

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

    def _open_session(self, filters, script_name, script_params):
        self._server.start(self.script_path(script_name),
                           vars_=script_params)
        self._driver = Driver(self._backend, self._uri, self._auth)
        session = self._driver.session("r", database="neo4j",
                                       notification_filters=filters)
        return session

    def _run_test_get_summary(self, filters, script_params,
                              script_name="run_notification_filters.script"):
        session = self._open_session(filters, script_name, script_params)
        cursor = session.run("RETURN 1 as n")
        return cursor.consume()

    def _tx_test_get_summary(self, filters, script_params,
                             script_name="begin_notification_filters.script"):
        session = self._open_session(filters, script_name, script_params)
        tx = session.begin_transaction()
        cursor = tx.run("RETURN 1 as n")
        cursor.consume()
        tx.commit()

    def test_default_run_notification_filter(self):
        self._run_test_get_summary(None, None,
                                   "run_default_notification_filters.script")
        self._server.done()

    def test_default_begin_notification_filter(self):
        self._tx_test_get_summary(None, None,
                                  "begin_default_notification_filters.script")
        self._server.done()

    def test_run_notification_filter(self):
        for config in super().configs():
            with self.subTest(name=config["filters"]):
                self._run_test_get_summary(config["filters"], config["params"])
                self._server.done()

    def test_begin_notification_filter(self):
        for config in super().configs():
            with self.subTest(name=config["filters"]):
                self._tx_test_get_summary(config["filters"], config["params"])
                self._server.done()
