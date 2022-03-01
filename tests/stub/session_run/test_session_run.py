from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestSessionRun(TestkitTestCase):

    required_features = types.Feature.BOLT_4_4,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic", principal="",
                                                       credentials=""))
        self._session = None

    def tearDown(self):
        if self._session is not None:
            self._session.close()
        self._server.reset()
        super().tearDown()

    def test_discard_on_session_close_untouched_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Driver requires result.next() to send PULL")
        self._server.start(
            path=self.script_path("session_discard_result.script")
        )
        self._session = self._driver.session("r", fetch_size=2)
        self._session.run("RETURN 1 AS n")
        # closing session while tx is open and result is not consumed at all
        self._session.close()
        self._session = None
        self._server.done()

    def test_discard_on_session_close_unfinished_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Driver sends RESET instead of ROLLBACK")
        self._server.start(
            path=self.script_path("session_discard_result.script"),
            vars=[]
        )
        self._session = self._driver.session("r", fetch_size=2)
        result = self._session.run("RETURN 1 AS n")
        result.next()
        # closing session while tx is open and result is not fully consumed
        self._session.close()
        self._session = None
        self._server.done()

    def test_discard_on_session_close_consumed_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Test makes backend/driver hang")
        self._server.start(
            path=self.script_path("session_discard_result.script")
        )
        self._session = self._driver.session("r", fetch_size=2)
        result = self._session.run("RETURN 1 AS n")
        result.consume()
        # closing session while tx is open and result has been manually
        # consumed
        self._session.close()
        self._session = None
        self._server.done()

    def test_no_discard_on_session_close_finished_result(self):
        self._server.start(
            path=self.script_path("session_consume_result.script")
        )
        self._session = self._driver.session("r", fetch_size=2)
        result = self._session.run("RETURN 1 AS n")
        list(result)  # pull all results
        # closing session while tx is open
        self._session.close()
        self._session = None
        self._server.done()

    def test_raises_error_on_session_run(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript", "dotnet"]:
            self.skipTest("Driver reports error too late.")
        self._server.start(
            path=self.script_path("session_error_on_run.script")
        )
        self._session = self._driver.session("r")
        with self.assertRaises(types.DriverError) as exc:
            self._session.run("RETURN 1 AS n")
        self.assertEqual(exc.exception.code, "Neo.ClientError.MadeUp.Code")
        self._server.done()
