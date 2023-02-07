from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class NotificationsBase(TestkitTestCase):
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
                "script": {"#NOTIS#": '"noti_min_sev":"OFF", '}
            },
            {
                "protocol": {"min_sev": "INFORMATION", "dis_cats": None},
                "script": {"#NOTIS#": '"noti_min_sev":"INFORMATION", '}
            },
            {
                "protocol": {"min_sev": "WARNING", "dis_cats": None},
                "script": {"#NOTIS#": '"noti_min_sev":"WARNING", '}
            },
            {
                "protocol": {
                    "min_sev": "INFORMATION",
                    "dis_cats": ["UNRECOGNIZED"]
                },
                "script": {"#NOTIS#": '"noti_min_sev":"INFORMATION", '
                                      '"noti_dis_cats":["UNRECOGNIZED"], '}
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["UNRECOGNIZED", "UNSUPPORTED"]
                },
                "script": {
                    "#NOTIS#": '"noti_min_sev":"WARNING", '
                               '"noti_dis_cats":["UNRECOGNIZED", "UNSUPPORTED"], '  # noqa: E501
                }
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["PERFORMANCE"]
                },
                "script": {"#NOTIS#": '"noti_min_sev":"WARNING", '
                                      '"noti_dis_cats":["PERFORMANCE"], '}
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["DEPRECATION"]
                },
                "script": {"#NOTIS#": '"noti_min_sev":"WARNING", '
                                      '"noti_dis_cats":["DEPRECATION"], '}
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["GENERIC"]
                },
                "script": {"#NOTIS#": '"noti_min_sev":"WARNING", '
                                      '"noti_dis_cats":["GENERIC"], '}
            },
            {
                "protocol": {
                    "min_sev": "WARNING",
                    "dis_cats": ["HINT"]
                },
                "script": {"#NOTIS#": '"noti_min_sev":"WARNING", '
                                      '"noti_dis_cats":["HINT"], '}
            }
        ]
