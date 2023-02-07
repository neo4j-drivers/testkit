from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.stub.notifications_config.notifications_base import (  # noqa: E501
    NotificationsBase,
)


class TestOptimizedNotificationsConfig(NotificationsBase):
    required_features = (types.Feature.BOLT_5_2,
                         types.Feature.API_DRIVER_NOTIFICATIONS_CONFIG,
                         types.Feature.API_SESSION_NOTIFICATIONS_CONFIG,
                         types.Feature.OPT_MINIMAL_NOTIFICATIONS_CONFIG)

    @staticmethod
    def _session(d, filters):
        return d.session("w", database="neo4j",
                         notifications_min_severity=filters["min_sev"],
                         notifications_disabled_categories=filters["dis_cats"])

    def _open_session(self, filters, script, script_params):
        self._server.start(self.script_path(script), vars_=script_params)
        self._driver = Driver(self._backend, self._uri, self._auth,
                              notifications_min_severity=filters["min_sev"],
                              notifications_disabled_categories=filters["dis_cats"])  # noqa: E501
        return self._session(self._driver, filters)

    def _run_test(self, filters, script_params,
                  script="notifications_config_driver.script"):
        session = self._open_session(filters, script, script_params)
        cursor = session.run("RETURN 1 as n")
        cursor.consume()
        self._server.done()

    def test_omit_session_config_on_run(self):
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._run_test(config["protocol"], config["script"])
            self._server.reset()

    def test_omit_disabled_categories_when_notifications_disabled(self):
        self._run_test({"min_sev": "OFF", "dis_cats": ["DEPRECATION"]},
                       {"#NOTIS#": '"noti_min_sev":"OFF", '})
        self._server.reset()

    def test_omit_disabled_categories_when_empty(self):
        self._run_test({"min_sev": "INFORMATION", "dis_cats": []},
                       {"#NOTIS#": '"noti_min_sev":"INFORMATION", '})
        self._server.reset()
