import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class NotificationsBase(TestkitTestCase):
    _auth = types.AuthorizationToken("basic", principal="neo4j",
                                     credentials="pass")
    _port = 9010

    def setUp(self):
        super().setUp()
        self._server = StubServer(self._port)
        self._uri = "bolt://%s" % self._server.address
        self._driver = None

    def tearDown(self):
        self._server.reset()
        if self._driver:
            self._driver.close()
        return super().tearDown()

    def _new_driver(self, min_sev=None, disabled_cats=None):
        return Driver(self._backend, self._uri, self._auth,
                      notifications_min_severity=min_sev,
                      notifications_disabled_categories=disabled_cats)

    def _run_test_get_summary(self, parameters, script_params,
                              script="notifications_config_driver.script"):
        self._server.start(self.script_path(script),
                           vars_=script_params)
        self._driver = self._new_driver(parameters["min_sev"],
                                        parameters["dis_cats"])
        session = self._driver.session("w", database="neo4j")
        cursor = session.run("CREATE (:node)")
        result = cursor.consume()
        self._server.done()
        return result

    @staticmethod
    def configs():
        return [
            {
                "protocol": {"min_sev": "OFF", "dis_cats": None},
                "script": {
                    "#NOTIS#": '"notifications_minimum_severity": "OFF", '
                }
            },
            {
                "protocol": {"min_sev": "INFORMATION", "dis_cats": None},
                "script": {
                    "#NOTIS#": '"notifications_minimum_severity": '
                               '"INFORMATION", '
                }
            },
            {
                "protocol": {"min_sev": "WARNING", "dis_cats": None},
                "script": {
                    "#NOTIS#": '"notifications_minimum_severity": "WARNING", '
                }
            },
            {
                "protocol": {"min_sev": None, "dis_cats": []},
                "script": {
                    "#NOTIS#": '"notifications_disabled_categories": [], '
                }
            },
            {
                "protocol": {
                    "min_sev": "INFORMATION",
                    "dis_cats": ["UNRECOGNIZED"]
                },
                "script": {
                    "#NOTIS#": '"notifications_minimum_severity": '
                               '"INFORMATION", '
                               '"notifications_disabled_categories": '
                               '["UNRECOGNIZED"], '
                }
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["UNRECOGNIZED", "UNSUPPORTED"]
                },
                "script": {
                    "#NOTIS#": '"notifications_minimum_severity": "WARNING", '
                               '"notifications_disabled_categories{}": '
                               '["UNRECOGNIZED", "UNSUPPORTED"], '
                }
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["PERFORMANCE"]
                },
                "script": {
                    "#NOTIS#": '"notifications_minimum_severity": "WARNING", '
                               '"notifications_disabled_categories": '
                               '["PERFORMANCE"], '
                }
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["DEPRECATION"]
                },
                "script": {
                    "#NOTIS#": '"notifications_minimum_severity": "WARNING", '
                               '"notifications_disabled_categories": '
                               '["DEPRECATION"], '
                }
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["GENERIC"]
                },
                "script": {
                    "#NOTIS#": '"notifications_minimum_severity": "WARNING", '
                               '"notifications_disabled_categories": '
                               '["GENERIC"], '
                }
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["HINT"]
                },
                "script": {
                    "#NOTIS#": '"notifications_minimum_severity": "WARNING", '
                               '"notifications_disabled_categories": '
                               '["HINT"], '
                }
            }
        ]
