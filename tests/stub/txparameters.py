import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


# Verifies that session.beginTransaction parameters are sent as expected on the wire.
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
            tx = session.beginTransaction(txMeta, timeout)
            # Need to do something on the transaction, driver might do lazy begin
            tx.run("RETURN 1 as n")
            tx.commit()
        finally:
            session.close()

    def test_accessmode_read(self):
        script = "txbegin_accessmode_read.script"
        if self._driverName in ["go"]:
            script = "txbegin_accessmode_read_pull_all.script"
        else:
            self.skipTest("Tx begin accessmode not implemented in backend")
        self._server.start(os.path.join(scripts_path, script))
        self._run("r")
        self._server.done()

    def test_accessmode_write(self):
        script = "txbegin_accessmode_write.script"
        if self._driverName in ["go"]:
            script = "txbegin_accessmode_write_pull_all.script"
        else:
            self.skipTest("Tx begin accessmode not implemented in backend")
        self._server.start(os.path.join(scripts_path, script))
        self._run("w")
        self._server.done()

    def test_bookmarks(self):
        script = "txbegin_bookmarks.script"
        if self._driverName in ["go"]:
            script = "txbegin_bookmarks_pull_all.script"
        else:
            self.skipTest("Tx begin bookmarks not implemented in backend")
        self._server.start(os.path.join(scripts_path, script))
        self._run("w", bookmarks=["b1", "b2"])
        self._server.done()

    def test_txmeta(self):
        script = "txbegin_txmeta.script"
        if self._driverName in ["go"]:
            script = "txbegin_txmeta_pull_all.script"
        else:
            self.skipTest("Tx begin meta not implemented in backend")
        self._server.start(os.path.join(scripts_path, script))
        self._run("w", txMeta={"akey": "aval"})
        self._server.done()

    def test_timeout(self):
        script = "txbegin_timeout.script"
        if self._driverName in ["go"]:
            script = "txbegin_timeout_pull_all.script"
        else:
            self.skipTest("Tx begin timeout not implemented in backend")
        self._server.start(os.path.join(scripts_path, script))
        self._run("w", timeout=17)
        self._server.done()

    def test_combined(self):
        script = "txbegin_combined_params.script"
        if self._driverName in ["go"]:
            script = "txbegin_combined_params_pull_all.script"
        else:
            self.skipTest("Tx begin params not implemented in backend")
        self._server.start(os.path.join(scripts_path, script))
        self._run("r", params={"p": types.CypherInt(1)}, bookmarks=["b0"], txMeta={"k": "v"}, timeout=11)
        self._server.done()


