from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.stub.notification_filters.notification_filters_base import (
    NotificationFiltersBase,
)


class TestSessionNotificationFilters(NotificationFiltersBase):
    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.API_SESSION_NOTIFICATION_FILTERS)

    def test_default_run_notification_filter(self):
        self._run_test(None, None, "run_default_notification_filters.script")

    def test_default_begin_notification_filter(self):
        self._tx_test(None, None, "begin_default_notification_filters.script")

    def test_run_notification_filter(self):
        for config in self.configs():
            with self.subTest(name=config["filters"]):
                self._run_test(config["filters"], config["params"])

    def test_begin_notification_filter(self):
        for config in self.configs():
            with self.subTest(name=config["filters"]):
                self._tx_test(config["filters"], config["params"])

    def test_read_tx_notification_filter(self):
        for config in self.configs():
            with self.subTest(name=config["filters"]):
                self._tx_func_test(config["filters"], config["params"])

    def test_can_default_in_session(self):
        script = "session_use_server_default.script"
        self._server.start(self.script_path(script))

        self._driver = Driver(self._backend, self._uri, self._auth,
                              notification_filters=["NONE"])
        session = self._driver.session("r", database="neo4j",
                                       notification_filters=["SERVER_DEFAULT"])
        cursor = session.run("RETURN 1 as n")
        cursor.consume()
        self._server.done()

    def _open_session(self, filters, script, script_params):
        self._server.start(self.script_path(script), vars_=script_params)
        self._driver = Driver(self._backend, self._uri, self._auth)
        session = self._driver.session("r", database="neo4j",
                                       notification_filters=filters)
        return session

    def _run_test(self, filters, script_params,
                  script="run_notification_filters.script"):
        session = self._open_session(filters, script, script_params)
        cursor = session.run("RETURN 1 as n")
        cursor.consume()
        self._server.done()

    def _tx_func_test(self, filters, script_params,
                      script="begin_notification_filters.script"):
        session = self._open_session(filters, script, script_params)

        def tx_func(tx):
            cursor = tx.run("RETURN 1 as n")
            cursor.consume()

        session.read_transaction(tx_func)
        self._server.done()

    def _tx_test(self, filters, script_params,
                 script="begin_notification_filters.script"):
        session = self._open_session(filters, script, script_params)
        tx = session.begin_transaction()
        cursor = tx.run("RETURN 1 as n")
        cursor.consume()
        tx.commit()
        self._server.done()
