from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.stub.notifications_config.notifications_base import (
    NotificationsBase,
)


class TestSessionNotificationsConfig(NotificationsBase):
    required_features = (types.Feature.BOLT_5_2,
                         types.Feature.API_DRIVER_NOTIFICATIONS_CONFIG,
                         types.Feature.API_SESSION_NOTIFICATIONS_CONFIG)
    driver_params = {
        "min_severity": "WARNING",
        "disabled_categories": [
            "UNSUPPORTED", "UNRECOGNIZED", "DEPRECATION", "HINT"
        ]
    }

    def test_session_config_on_run(self):
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._run_test(config["protocol"], config["script"])
            self._server.reset()

    def test_session_config_on_begin(self):
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._tx_test(config["protocol"], config["script"])
            self._server.reset()

    def test_session_config_on_read_tx(self):
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._tx_func_test(config["protocol"], config["script"])
            self._server.reset()

    def test_session_overrides_driver_config_on_run(self):
        script = "notifications_config_override_session_run.script"
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._run_test(config["protocol"], config["script"],
                               script, self.driver_params)
            self._server.reset()

    def test_session_overrides_driver_config_on_begin(self):
        script = "notifications_config_override_session_begin.script"
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._tx_test(config["protocol"], config["script"],
                              script, self.driver_params)
            self._server.reset()

    def test_session_overrides_driver_config_on_read_tx(self):
        script = "notifications_config_override_session_begin.script"
        for config in self.configs():
            with self.subTest(name=config["protocol"]):
                self._tx_func_test(config["protocol"], config["script"],
                                   script, self.driver_params)
            self._server.reset()

    def test_session_config_with_database_not_specified(self):
        uris = [self._uri, self._uri.replace("bolt", "neo4j")]
        for uri in uris:
            with self.subTest(uri=uri):
                try:
                    host = self._server.address
                    script_path = self.script_path(
                        "notifications_config_session_router_run.script"
                    )
                    script_vars = {
                        "#NOTIS#": '"notifications_minimum_severity"'
                                   ': "WARNING", ',
                        "#HOST#": host
                    }
                    self._server.start(script_path, vars_=script_vars)
                    driver = Driver(self._backend, uri, self._auth)
                    session = driver.session(
                        None,
                        notifications_min_severity="WARNING"
                    )
                    cursor = session.run("RETURN 1 as n")
                    cursor.consume()
                    self._server.done()
                finally:
                    self._server.reset()
                    if driver is not None:
                        driver.close()

    @staticmethod
    def _session(d, filters):
        return d.session("r", database="neo4j",
                         notifications_min_severity=filters["min_sev"],
                         notifications_disabled_categories=filters["dis_cats"])

    def _open_session(self, filters, script, script_params, driver_params):
        self._server.start(self.script_path(script), vars_=script_params)
        if driver_params is not None:
            sev = driver_params["min_severity"]
            cats = driver_params["disabled_categories"]
            self._driver = Driver(self._backend, self._uri, self._auth,
                                  notifications_min_severity=sev,
                                  notifications_disabled_categories=cats)
        else:
            self._driver = Driver(self._backend, self._uri, self._auth)
        return self._session(self._driver, filters)

    def _run_test(self, filters, script_params,
                  script="notifications_config_session_run.script",
                  driver_params=None):
        session = self._open_session(filters, script, script_params,
                                     driver_params)
        cursor = session.run("RETURN 1 as n")
        cursor.consume()
        self._server.done()

    @staticmethod
    def _tx_func(tx):
        cursor = tx.run("RETURN 1 as n")
        cursor.consume()

    def _tx_test(self, filters, script_params,
                 script="notifications_config_session_begin.script",
                 driver_params=None):
        session = self._open_session(filters, script, script_params,
                                     driver_params)
        tx = session.begin_transaction()
        self._tx_func(tx)
        tx.commit()
        self._server.done()

    def _tx_func_test(self, filters, script_params,
                      script="notifications_config_session_begin.script",
                      driver_params=None):
        session = self._open_session(filters, script, script_params,
                                     driver_params)
        session.execute_read(self._tx_func)
        self._server.done()
