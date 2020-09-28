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
class SessionRunParameters(unittest.TestCase):
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
        script = "sessionrun_accessmode_read.script"
        if self._driverName in ["go"]:
            script = "sessionrun_accessmode_read_pull_all.script"
        elif self._driverName not in ["java", "dotnet"]:
            self.skipTest("Session accessmode not implemented in backend")

        self._server.start(path=os.path.join(scripts_path, script))
        self._run("r")
        self._server.done()

    def test_accessmode_write(self):
        script = "sessionrun_accessmode_write.script"
        if self._driverName in ["go"]:
            script = "sessionrun_accessmode_write_pull_all.script"
        self._server.start(path=os.path.join(scripts_path, script))
        self._run("w")
        self._server.done()

    def test_bookmarks(self):
        script = "sessionrun_bookmarks.script"
        if self._driverName in ["go"]:
            script = "sessionrun_bookmarks_pull_all.script"
        elif self._driverName not in ["dotnet"]:
            self.skipTest("Session bookmarks not implemented in backend")
        self._server.start(path=os.path.join(scripts_path, script))
        self._run("w", bookmarks=["b1", "b2"])
        self._server.done()

    def test_txmeta(self):
        script = "sessionrun_txmeta.script"
        if self._driverName in ["go"]:
            script = "sessionrun_txmeta_pull_all.script"
        elif self._driverName not in ["dotnet"]:
            self.skipTest("Session txmeta not implemented in backend")
        self._server.start(path=os.path.join(scripts_path, script))
        self._run("w", txMeta={"akey": "aval"})
        self._server.done()

    def test_timeout(self):
        script = "sessionrun_timeout.script"
        if self._driverName in ["go"]:
            script = "sessionrun_timeout_pull_all.script"
        elif self._driverName not in ["dotnet"]:
            self.skipTest("Session timeout not implemented in backend")
        self._server.start(path=os.path.join(scripts_path, script))
        self._run("w", timeout=17)
        self._server.done()

    def test_combined(self):
        script = "sessionrun_combined_params.script"
        if self._driverName in ["go"]:
            script = "sessionrun_combined_params_pull_all.script"
        elif self._driverName not in ["dotnet"]:
            self.skipTest("Session parameters not implemented in backend")
        self._server.start(path=os.path.join(scripts_path, script))
        self._run("r", params={"p": types.CypherInt(1)}, bookmarks=["b0"], txMeta={"k": "v"}, timeout=11)
        self._server.done()


