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
                              script_name="driver_notification_filters.script"
                              ):
        self._server.start(self.script_path(script_name),
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
                "filters": ["None"],
                "params": {"#FILTERS#": "[]"}
            },
            {
                "filters": ["ALL.ALL"],
                "params": {"#FILTERS#": '["*.*"]'}
            },
            {
                "filters": ["ALL.QUERY"],
                "params": {"#FILTERS#": '["*.QUERY"]'}
            },
            {
                "filters": ["WARNING.ALL"],
                "params": {"#FILTERS#": '["WARNING.*"]'}
            },
            {
                "filters": ["WARNING.DEPRECATION"],
                "params": {"#FILTERS#": '["WARNING.DEPRECATION"]'}
            },
            {
                "filters": ["WARNING.HINT"],
                "params": {"#FILTERS#": '["WARNING.HINT"]'}
            },
            {
                "filters": ["WARNING.QUERY"],
                "params": {"#FILTERS#": '["WARNING.QUERY"]'}
            },
            {
                "filters": ["WARNING.UNSUPPORTED"],
                "params": {"#FILTERS#": '["WARNING.UNSUPPORTED"]'}
            },
            {
                "filters": ["INFORMATION.ALL"],
                "params": {"#FILTERS#": '["INFORMATION.*"]'}
            },
            {
                "filters": ["INFORMATION.RUNTIME"],
                "params": {"#FILTERS#": '["INFORMATION.RUNTIME"]'}
            },
            {
                "filters": ["INFORMATION.QUERY"],
                "params": {"#FILTERS#": '["INFORMATION.QUERY"]'}
            },
            {
                "filters": ["INFORMATION.PERFORMANCE"],
                "params": {"#FILTERS#": '["INFORMATION.PERFORMANCE"]'}
            },
            {
                "filters": ["INFORMATION.PERFORMANCE",
                            "WARNING.ALL", "ALL.QUERY"],
                "params": {"#FILTERS#": '["INFORMATION.PERFORMANCE", '
                                        '"WARNING.*", "*.QUERY"]'}
            }
        ]
