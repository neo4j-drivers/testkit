from nutkit.frontend import Driver
from nutkit import protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestTxRun(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken(scheme="basic"))
        self._session = None

    def tearDown(self):
        if self._session is not None:
            self._session.close()
        self._server.done()
        super().tearDown()

    def test_rollback_tx_on_session_close_untouched_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("Driver does not allow closing a session with a "
                          "pending transaction")
        if get_driver_name() in ["javascript"]:
            self.skipTest("Driver requires result.next() to send PULL")
        self._server.start(
            path=self.script_path("tx_discard_then_rollback.script")
        )
        self._session = self._driver.session("r", fetchSize=2)
        tx = self._session.beginTransaction()
        tx.run("RETURN 1 AS n")
        # closing session while tx is open and result is not consumed at all
        self._session.close()
        self._session = None
        self._server.done()

    def test_rollback_tx_on_session_close_unfinished_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("Go driver does not allow closing a session with a "
                          "pending transaction")
        if get_driver_name() in ["javascript"]:
            self.skipTest("Sends RESET instead of ROLLBACK.")
        self._server.start(
            path=self.script_path("tx_discard_then_rollback.script")
        )
        self._session = self._driver.session("r", fetchSize=2)
        tx = self._session.beginTransaction()
        result = tx.run("RETURN 1 AS n")
        result.next()
        # closing session while tx is open and result is not fully consumed
        self._session.close()
        self._session = None
        self._server.done()

    def test_rollback_tx_on_session_close_consumed_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("Go driver does not allow closing a session with a "
                          "pending transaction")
        if get_driver_name() in ["javascript"]:
            self.skipTest("Driver sends RESET instead of ROLLBACK")
        self._server.start(
            path=self.script_path("tx_discard_then_rollback.script")
        )
        self._session = self._driver.session("r", fetchSize=2)
        tx = self._session.beginTransaction()
        result = tx.run("RETURN 1 AS n")
        result.consume()
        # closing session while tx is open and result has been manually consumed
        self._session.close()
        self._session = None
        self._server.done()

    def test_rollback_tx_on_session_close_finished_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("Go driver does not allow closing a session with a "
                          "pending transaction")
        if get_driver_name() in ["javascript"]:
            self.skipTest("Driver sends RESET instead of ROLLBACK")
        self._server.start(
            path=self.script_path("tx_pull_then_rollback.script")
        )
        self._session = self._driver.session("r", fetchSize=2)
        tx = self._session.beginTransaction()
        result = tx.run("RETURN 1 AS n")
        list(result)  # pull all results
        # closing session while tx is open
        self._session.close()
        self._session = None
        self._server.done()
