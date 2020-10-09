import unittest

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types

script_router = """
!: BOLT #VERSION#
!: AUTO RESET
!: AUTO HELLO
!: AUTO GOODBYE

C: #GET_ROUTING_TABLE#
S: SUCCESS {"fields": ["ttl", "servers"]}
S: RECORD [1000, [{"addresses": ["#HOST#:9002"], "role":"READ"}, {"addresses": ["#HOST#:9003"], "role":"WRITE"}]]
S: SUCCESS {"type": "r"}
"""
script_read = """
!: BOLT #VERSION#
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: #RUN_READ#
C: #PULL#
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
   <EXIT>
"""
script_write = """
!: BOLT #VERSION#
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: #RUN_WRITE#
C: #PULL#
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "w"}
   <EXIT>
"""
script_tx_read = """
!: BOLT #VERSION#
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: #BEGIN_READ#
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
C: #PULL#
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
   <EXIT>
"""
script_tx_write = """
!: BOLT #VERSION#
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: #BEGIN_WRITE#
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
C: #PULL#
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
   <EXIT>
"""


vars_v3 = {
    "#VERSION#":           '3',
    "#GET_ROUTING_TABLE#": 'RUN \"CALL dbms.cluster.routing.getRoutingTable($context)\" {\"context\": {\"address\": \"#ADDRESS_STR\"}} {#MODE_STR}',
    "#PULL#":              'PULL_ALL',
    "#RUN_READ#":          'RUN "RETURN 1 as n" {} {"mode": "r"}',
    "#RUN_WRITE#":         'RUN "RETURN 1 as n" {} {}',
    "#BEGIN_READ#":        'BEGIN {"mode": "r"}',
    "#BEGIN_WRITE#":       'BEGIN {}',

    # Variables for language specifics
    "#ADDRESS_STR": '',
    "#MODE_STR": '"mode": "r", "db": "system"',
}



# Bolt version 4 and later needs named databases when querying for routing table.
dbname = "adb"
vars_v4 = {
    "#VERSION#":           '4.0',
    "#GET_ROUTING_TABLE#": 'RUN \"CALL dbms.routing.getRoutingTable($context, $#DATABASE_STR)\" {\"context\": {\"address\": \"#ADDRESS_STR\"}, \"#DATABASE_STR\": \"%s\"} {#MODE_STR}' % (dbname),
    "#PULL#":              'PULL {"n": 1000}',
    "#RUN_READ#":          'RUN "RETURN 1 as n" {} {"mode": "r", "db": "%s"}' % (dbname),
    "#RUN_WRITE#":         'RUN "RETURN 1 as n" {} {"db": "%s"}' % (dbname),
    "#BEGIN_READ#":        'BEGIN {"mode": "r", "db": "%s"}' % (dbname),
    "#BEGIN_WRITE#":       'BEGIN {"db": "%s"}' % (dbname),

    # Variables for language specifics
    "#DATABASE_STR": 'db',
    "#ADDRESS_STR": '',
    "#MODE_STR": '"mode": "r", "db": "system"',
}



# Current version of the protocol. Older protocol version should be subclasses to make
# it easier to remove when no longer supported.
class Routing(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._driverName = get_driver_name()
        self._routingServer = StubServer(9001)
        self._readServer = StubServer(9002)
        self._writeServer = StubServer(9003)

        # Bolt version hook
        self.setupVersion()

        self._vars["#HOST#"] = self._routingServer.host

        self.runServer(self._routingServer)

        uri = "neo4j://%s" % self._routingServer.address
        # Driver is configured to talk to "routing" stub server
        self._driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))

    def setupVersion(self):
        self._database = dbname
        self._vars = vars_v4

        # Now append language specific variables.
        if self._driverName in ['dotnet']:
            self._vars["#ADDRESS_STR"] = os.environ.get("TEST_STUB_HOST") + ":#PORT"
            self._vars["#DATABASE_STR"] = 'database'
            self._vars["#PORT"] = ''

    def runServer(self, server):
        self._vars["#PORT"] = str(server.port)
        server.start(script=script_router, vars=self._vars)

    def tearDown(self):
        self._driver.close()
        self._backend.close()
        self._routingServer.reset()
        self._readServer.reset()
        self._writeServer.reset()

    # Checks that routing is used to connect to correct server and that parameters for
    # session run is passed on to the target server (not the router).
    def test_session_run_read(self):
        if self._driverName not in ['go', 'dotnet']:
            self.skipTest("Session with named database not implemented in backend")

        self.runServer(self._readServer)
        session = self._driver.session('r', database=self._database)
        session.run("RETURN 1 as n")
        session.close()

    # Same test as for session.run but for transaction run.
    def test_tx_run_read(self):
        if self._driverName not in ['go']:
            self.skipTest("Session with named database not implemented in backend")

        self.runServer(self._readServer)
        session = self._driver.session('r', database=self._database)
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()

    # Checks that write server is used
    def test_session_run_write(self):
        if self._driverName not in ['go']:
            self.skipTest("Session with named database not implemented in backend")

        self.runServer(self._writeServer)
        session = self._driver.session('w', database=self._database)
        session.run("RETURN 1 as n")
        session.close()

    # Checks that write server is used
    def test_tx_run_write(self):
        if self._driverName not in ['go']:
            self.skipTest("Session with named database not implemented in backend")

        self.runServer(self._writeServer)
        session = self._driver.session('w', database=self._database)
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()


class RoutingV3(Routing):
    def setupVersion(self):
        self._database = None
        self._vars = vars_v3

        # Now append language specific variables.
        if self._driverName in ['dotnet']:
            self._vars["#ADDRESS_STR"] = os.environ.get("TEST_STUB_HOST") + ":#PORT"
            self._vars["#MODE_STR"] = ''
            self._vars["#PORT"] = ''


