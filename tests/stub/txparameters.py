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

C: BEGIN {"mode": "r"}
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
   <EXIT>
"""
script_accessmode_write = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: BEGIN {}
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
   <EXIT>
"""
script_bookmarks = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: BEGIN {"bookmarks": ["b1", "b2"]}
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
   <EXIT>
"""
script_txmeta = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: BEGIN {"tx_metadata": {"akey": "aval"}}
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
   <EXIT>

"""
script_timeout = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: BEGIN {"tx_timeout": 17}
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
   <EXIT>
"""
script_combined = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: BEGIN {"bookmarks": ["b0"], "tx_metadata": {"k": "v"}, "mode": "r", "tx_timeout": 11}
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
   <EXIT>
"""


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
        if self._driverName not in ["dotnet", "go"]:
            self.skipTest("Tx begin accessmode not implemented in backend")
        self._server.start(script=script_accessmode_read)
        self._run("r")
        self._server.done()

    def test_accessmode_write(self):
        if self._driverName not in ["dotnet", "go"]:
            self.skipTest("Tx begin accessmode not implemented in backend")
        self._server.start(script=script_accessmode_write)
        self._run("w")
        self._server.done()

    def test_bookmarks(self):
        if self._driverName not in ["dotnet", "go"]:
            self.skipTest("Tx begin bookmarks not implemented in backend")
        self._server.start(script=script_bookmarks)
        self._run("w", bookmarks=["b1", "b2"])
        self._server.done()

    def test_txmeta(self):
        if self._driverName not in ["dotnet", "go"]:
            self.skipTest("Tx begin meta not implemented in backend")
        self._server.start(script=script_txmeta)
        self._run("w", txMeta={"akey": "aval"})
        self._server.done()

    def test_timeout(self):
        if self._driverName not in ["dotnet", "go"]:
            self.skipTest("Tx begin timeout not implemented in backend")
        self._server.start(script=script_timeout)
        self._run("w", timeout=17)
        self._server.done()

    def test_combined(self):
        if self._driverName not in ["dotnet", "go"]:
            self.skipTest("Tx begin params not implemented in backend")
        self._server.start(script=script_combined)
        self._run("r", params={"p": types.CypherInt(1)}, bookmarks=["b0"], txMeta={"k": "v"}, timeout=11)
        self._server.done()


