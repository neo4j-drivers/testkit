import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types

script_router = """
!: BOLT 4.0
!: AUTO RESET
!: AUTO HELLO
!: AUTO GOODBYE

C: RUN "CALL dbms.routing.getRoutingTable($context)" {"context": {}} {"mode": "r", "db": "system"}
S: SUCCESS {"fields": ["ttl", "servers"]}
S: RECORD [1000, [{"addresses": ["$host:9002"], "role":"READ"}, {"addresses": ["$host:9003"], "role":"WRITE"}]]
S: SUCCESS {"type": "r"}
S: <EXIT>
"""
script_read = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {"mode": "r"}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
   <EXIT>
"""
script_write = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "w"}
   <EXIT>
"""
script_tx_read = """
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
script_tx_write = """
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


class Routing(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._driverName = get_driver_name()
        self._routingServer = StubServer(9001)
        self._routingServer.start(script=script_router, vars={"$host": self._routingServer.host})
        self._readServer = StubServer(9002)
        self._writeServer = StubServer(9003)
        uri = "neo4j://%s" % self._routingServer.address
        # Driver is configured to talk to "routing" stub server
        self._driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))

    def tearDown(self):
        self._driver.close()
        self._backend.close()
        self._routingServer.reset()
        self._readServer.reset()
        self._writeServer.reset()

    # Checks that routing is used to connect to correct server and that parameters for
    # session run is passed on to the target server (not the router).
    def test_session_run_read(self):
        self._readServer.start(script=script_read)
        session = self._driver.session('r')
        session.run("RETURN 1 as n")
        session.close()

    # Same test as for session.run but for transaction run.
    def test_tx_run_read(self):
        self._readServer.start(script=script_tx_read)
        session = self._driver.session('r')
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()

    # Checks that write server is used
    def test_session_run_write(self):
        self._writeServer.start(script=script_write)
        session = self._driver.session('w')
        session.run("RETURN 1 as n")
        session.close()

    # Checks that write server is used
    def test_tx_run_write(self):
        self._writeServer.start(script=script_tx_write)
        session = self._driver.session('w')
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()

