from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


# Verifies that session.run parameters are sent as expected on the wire.
# These are the different cases tests:
#   Read mode
#   Write mode
#   Bookmarks + write mode
#   Transaction meta data + write mode
#   Transaction timeout + write mode
#   Read mode + transaction meta data + transaction timeout + bookmarks
class TestSessionRunParameters(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken(scheme="basic"))

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        super().tearDown()

    def _run(self, access_mode, params=None, bookmarks=None, tx_meta=None,
             timeout=None):
        session = self._driver.session(access_mode, bookmarks=bookmarks)
        try:
            result = session.run("RETURN 1 as n", params=params, txMeta=tx_meta,
                                 timeout=timeout)
            result.next()
        finally:
            session.close()

    def _start_server(self, script):
        self._server.start(path=self.script_path(script))

    def test_access_mode_read(self):
        self._start_server("access_mode_read.script")
        self._run("r")
        self._driver.close()
        self._server.done()

    def test_access_mode_write(self):
        self._start_server("access_mode_write.script")
        self._run("w")
        self._driver.close()
        self._server.done()

    def test_bookmarks(self):
        self._start_server("bookmarks.script")
        self._run("w", bookmarks=["b1", "b2"])
        self._driver.close()
        self._server.done()

    def test_tx_meta(self):
        self._start_server("tx_meta.script")
        self._run("w", tx_meta={"akey": "aval"})
        self._driver.close()
        self._server.done()

    def test_timeout(self):
        self._start_server("timeout.script")
        self._run("w", timeout=17)
        self._driver.close()
        self._server.done()

    def test_combined(self):
        self._start_server("combined.script")
        self._run("r", params={"p": types.CypherInt(1)}, bookmarks=["b0"],
                  tx_meta={"k": "v"}, timeout=11)
        self._driver.close()
        self._server.done()

    def test_empty_query(self):
        if get_driver_name() in ["javascript", "java"]:
            self.skipTest("rejects empty string")
        # Driver should pass empty string to server
        self._start_server("empty_query.script")
        session = self._driver.session("w")
        session.run("").next()
        session.close()
        self._driver.close()
        self._server.done()
