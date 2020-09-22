
import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


# Verifies that session.Run parameters are sent as expected on the wire.
# These are the different cases tests:
#   Read mode
#   Write mode
#   Bookmarks + write mode
#   Transaction meta data + write mode
#   Transaction timeout + write mode
#   Read mode + transaction meta data + transaction timeout + bookmarks
class TxBeginParameters(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()
        auth = AuthorizationToken()
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))

    def tearDown(self):
        self._driver.close()
        self._backend.close()
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analys.
        self._server.reset()

    def _run(self, accessMode, params=None, bookmarks=None, txMeta=None, timeout=None):
        session = self._driver.session(accessMode, bookmarks=bookmarks)
        try:
            result = session.run("RETURN 1 as n", params=params, txMeta=txMeta, timeout=timeout)
            result.next()
        finally:
            session.close()

    def test_accessmode_read(self):
        if not self._driverName in ["go", "java"]:
            self.skipTest("Session accessmode not implemented in backend")
        self._server.start(os.path.join(scripts_path, "sessionrun_accessmode_read.script"))
        self._run("r")
        self._server.done()

    def test_accessmode_write(self):
        if not self._driverName in ["go", "java"]:
            self.skipTest("Session accessmode not implemented in backend")
        self._server.start(os.path.join(scripts_path, "sessionrun_accessmode_write.script"))
        self._run("w")
        self._server.done()

    def test_bookmarks(self):
        if not self._driverName in ["go"]:
            self.skipTest("Session bookmarks not implemented in backend")
        self._server.start(os.path.join(scripts_path, "sessionrun_bookmarks.script"))
        self._run("w", bookmarks=["b1", "b2"])
        self._server.done()

    def test_txmeta(self):
        if not self._driverName in ["go"]:
            self.skipTest("Session tx metadata not implemented in backend")
        self._server.start(os.path.join(scripts_path, "sessionrun_txmeta.script"))
        self._run("w", txMeta={"akey": "aval"})
        self._server.done()

    def test_timeout(self):
        if not self._driverName in ["go"]:
            self.skipTest("Session timeout not implemented in backend")
        self._server.start(os.path.join(scripts_path, "sessionrun_timeout.script"))
        self._run("w", timeout=17)
        self._server.done()

    def test_combined(self):
        if not self._driverName in ["go"]:
            self.skipTest("Session params not implemented in backend")
        self._server.start(os.path.join(scripts_path, "sessionrun_combined_params.script"))
        self._run("r", params={"p": types.CypherInt(1)}, bookmarks=["b0"], txMeta={"k": "v"}, timeout=11)
        self._server.done()


