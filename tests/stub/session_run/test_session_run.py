from nutkit.frontend import Driver
from nutkit import protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestSessionRun(TestkitTestCase):
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
        self._server.done()
        super().tearDown()

    def test_discard_on_session_close_untouched_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Driver requires result.next() to send PULL")
        self._server.start(
            path=self.script_path("session_discard_result.script")
        )
        self._session = self._driver.session("r", fetchSize=2)
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
            path=self.script_path("session_discard_result.script")
        )
        self._session = self._driver.session("r", fetchSize=2)
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
        self._session = self._driver.session("r", fetchSize=2)
        result = self._session.run("RETURN 1 AS n")
        result.consume()
        # closing session while tx is open and result has been manually consumed
        self._session.close()
        self._session = None
        self._server.done()

    def test_no_discard_on_session_close_finished_result(self):
        self._server.start(
            path=self.script_path("session_consume_result.script")
        )
        self._session = self._driver.session("r", fetchSize=2)
        result = self._session.run("RETURN 1 AS n")
        list(result)  # pull all results
        # closing session while tx is open
        self._session.close()
        self._session = None
        self._server.done()
