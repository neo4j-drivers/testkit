import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types

script_accessmode_read = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {"mode": "r"}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
"""

script_accessmode_write = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "w"}
"""

script_bookmarks = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {"bookmarks": ["b1", "b2"]}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "w"}
"""

script_txmeta = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {"tx_metadata": {"akey": "aval"}}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "w"}
"""

script_timeout = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {"tx_timeout": 17}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "w"}
"""

script_combined = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: RUN "RETURN 1 as n" {"p": 1} {"bookmarks": ["b0"], "tx_metadata": {"k": "v"}, "mode": "r", "tx_timeout": 11}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "w"}
"""


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
        if self._driverName not in ["go", "java", "dotnet", "javascript"]:
            self.skipTest("Session accessmode not implemented in backend")

        self._server.start(script=script_accessmode_read)
        self._run("r")
        self._driver.close()
        self._server.done()

    def test_accessmode_write(self):
        self._server.start(script=script_accessmode_write)
        self._run("w")
        self._driver.close()
        self._server.done()

    def test_bookmarks(self):
        if self._driverName not in ["go", "dotnet", "javascript"]:
            self.skipTest("Session bookmarks not implemented in backend")
        self._server.start(script=script_bookmarks)
        self._run("w", bookmarks=["b1", "b2"])
        self._driver.close()
        self._server.done()

    def test_txmeta(self):
        if self._driverName not in ["go", "dotnet", "javascript"]:
            self.skipTest("Session txmeta not implemented in backend")
        self._server.start(script=script_txmeta)
        self._run("w", txMeta={"akey": "aval"})
        self._driver.close()
        self._server.done()

    def test_timeout(self):
        if self._driverName not in ["go", "dotnet", "javascript"]:
            self.skipTest("Session timeout not implemented in backend")
        self._server.start(script=script_timeout)
        self._run("w", timeout=17)
        self._driver.close()
        self._server.done()

    def test_combined(self):
        if self._driverName not in ["go", "dotnet", "javascript"]:
            self.skipTest("Session parameters not implemented in backend")
        self._server.start(script=script_combined)
        self._run("r", params={"p": types.CypherInt(1)}, bookmarks=["b0"], txMeta={"k": "v"}, timeout=11)
        self._driver.close()
        self._server.done()

