from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class NotificationFiltersBase(TestkitTestCase):
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
                              script="driver_notification_filters.script"):
        self._server.start(self.script_path(script),
                           vars_=script_params)
        self._driver = self._new_driver(filters)
        session = self._driver.session("w", database="neo4j")
        cursor = session.run("CREATE (:node)")
        result = cursor.consume()
        self._server.done()
        return result

    @staticmethod
    def configs():
        return [
            {
                "filters": ["NONE"],
                "params": {"#FILTERS#": "[]"}
            },
            {
                "filters": ["ALL.ALL"],
                "params": {"#FILTERS#": '["*.*"]'}
            },
            {
                "filters": ["ALL.HINT"],
                "params": {"#FILTERS#": '["*.HINT"]'}
            },
            {
                "filters": ["ALL.UNRECOGNIZED"],
                "params": {"#FILTERS#": '["*.UNRECOGNIZED"]'}
            },
            {
                "filters": ["ALL.UNSUPPORTED"],
                "params": {"#FILTERS#": '["*.UNSUPPORTED"]'}
            },
            {
                "filters": ["ALL.PERFORMANCE"],
                "params": {"#FILTERS#": '["*.PERFORMANCE"]'}
            },
            {
                "filters": ["ALL.DEPRECATION"],
                "params": {"#FILTERS#": '["*.DEPRECATION"]'}
            },
            {
                "filters": ["ALL.GENERIC"],
                "params": {"#FILTERS#": '["*.GENERIC"]'}
            },
            {
                "filters": ["WARNING.ALL"],
                "params": {"#FILTERS#": '["WARNING.*"]'}
            },
            {
                "filters": ["WARNING.HINT"],
                "params": {"#FILTERS#": '["WARNING.HINT"]'}
            },
            {
                "filters": ["WARNING.UNRECOGNIZED"],
                "params": {"#FILTERS#": '["WARNING.UNRECOGNIZED"]'}
            },
            {
                "filters": ["WARNING.UNSUPPORTED"],
                "params": {"#FILTERS#": '["WARNING.UNSUPPORTED"]'}
            },
            {
                "filters": ["WARNING.PERFORMANCE"],
                "params": {"#FILTERS#": '["WARNING.PERFORMANCE"]'}
            },
            {
                "filters": ["WARNING.DEPRECATION"],
                "params": {"#FILTERS#": '["WARNING.DEPRECATION"]'}
            },
            {
                "filters": ["WARNING.GENERIC"],
                "params": {"#FILTERS#": '["WARNING.GENERIC"]'}
            },
            {
                "filters": ["WARNING.ALL"],
                "params": {"#FILTERS#": '["WARNING.*"]'}
            },
            {
                "filters": ["INFORMATION.HINT"],
                "params": {"#FILTERS#": '["INFORMATION.HINT"]'}
            },
            {
                "filters": ["INFORMATION.UNRECOGNIZED"],
                "params": {"#FILTERS#": '["INFORMATION.UNRECOGNIZED"]'}
            },
            {
                "filters": ["INFORMATION.UNSUPPORTED"],
                "params": {"#FILTERS#": '["INFORMATION.UNSUPPORTED"]'}
            },
            {
                "filters": ["INFORMATION.PERFORMANCE"],
                "params": {"#FILTERS#": '["INFORMATION.PERFORMANCE"]'}
            },
            {
                "filters": ["INFORMATION.DEPRECATION"],
                "params": {"#FILTERS#": '["INFORMATION.DEPRECATION"]'}
            },
            {
                "filters": ["INFORMATION.GENERIC"],
                "params": {"#FILTERS#": '["INFORMATION.GENERIC"]'}
            },
            {
                "filters": ["INFORMATION.PERFORMANCE",
                            "WARNING.ALL", "ALL.DEPRECATION"],
                "params": {"#FILTERS#": '["INFORMATION.PERFORMANCE", '
                                        '"WARNING.*", "*.DEPRECATION"]'}
            }
        ]
