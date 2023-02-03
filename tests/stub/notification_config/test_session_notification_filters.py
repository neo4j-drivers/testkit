from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.stub.notification_config.notification_filters_base import (
    NotificationFiltersBase,
)


class TestSessionNotificationFilters(NotificationFiltersBase):
    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.API_SESSION_NOTIFICATION_CONFIG)

    def test_run_notification_filter(self):
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._run_test(config["protocol"], config["script"])
            self._server.reset()

    def test_begin_notification_filter(self):
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._tx_test(config["protocol"], config["script"])
            self._server.reset()

    def test_read_tx_notification_filter(self):
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._tx_func_test(config["protocol"], config["script"])
            self._server.reset()

    @staticmethod
    def _session(d, filters):
        return d.session("r", database="neo4j",
                         notification_min_severity=filters["min_sev"],
                         notification_disabled_categories=filters["dis_cats"])

    def _open_session(self, filters, script, script_params):
        self._server.start(self.script_path(script), vars_=script_params)
        self._driver = Driver(self._backend, self._uri, self._auth,
                              notification_min_severity="OFF")
        return self._session(self._driver, filters)

    def _run_test(self, filters, script_params,
                  script="notifications_filters_session_run.script"):
        session = self._open_session(filters, script, script_params)
        cursor = session.run("RETURN 1 as n")
        cursor.consume()
        self._server.done()

    @staticmethod
    def _tx_func(tx):
        cursor = tx.run("RETURN 1 as n")
        cursor.consume()

    def _tx_test(self, filters, script_params,
                 script="notifications_filters_session_begin.script"):
        session = self._open_session(filters, script, script_params)
        tx = session.begin_transaction()
        self._tx_func(tx)
        tx.commit()
        self._server.done()

    def _tx_func_test(self, filters, script_params,
                      script="notifications_filters_session_begin.script"):
        session = self._open_session(filters, script, script_params)
        session.read_transaction(self._tx_func)
        self._server.done()
