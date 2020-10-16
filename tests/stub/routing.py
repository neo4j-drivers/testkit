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
C: #PULL_ROUTING_TABLE#
S: SUCCESS {"fields": ["ttl", "servers"]}
S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9002"], "role":"READ"}, {"addresses": ["#HOST#:9003"], "role":"WRITE"}]]
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


# Current version of the protocol. Older protocol version should be subclasses to make
# it easier to remove when no longer supported.
class Routing(unittest.TestCase):

    def setUp(self):
        self._backend = new_backend()
        self._driverName = get_driver_name()

        self._routingServer = StubServer(9001)
        # Hook for overriding classes to change script variables
        self.setupVars()
        self._routingServer.start(script=script_router, vars=self._vars)

        self._readServer = StubServer(9002)
        self._writeServer = StubServer(9003)

        uri = "neo4j://%s" % self._routingServer.address
        # Driver is configured to talk to "routing" stub server
        self._driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))

    def setupVars(self):
        # Bolt version 4 and later needs named databases when querying for routing table.
        self._database = 'adb'
        vars = {
            'dbName': self._database,
            # Just the name of the cypher parameter, varies between drivers
            'dbParamName': 'db',
            'config': '{"mode": "r", "db": "system"}',
            # In >= 4.1 an address was added to context to solve issues when setting up routing tables running in docker.
            # This has been backported to 4.0 in both driver and server since it isn't breaking.
            'context': '{}',
            'routes_pull_n': 1000,
        }


        # Now append/update language specific variables.
        if self._driverName in ['dotnet']:
            vars['dbParamName'] = 'database'
            vars['context'] = '{{"address":"{host}:9001"}}'.format(host=os.environ.get("TEST_STUB_HOST"))
            vars['routes_pull_n'] = -1

        routing = 'RUN "CALL dbms.routing.getRoutingTable($context, ${dbParamName})" {{"context": {context}, "{dbParamName}":"{dbName}"}} {config}'
        routing = routing.format(**vars)

        self._vars = {"#VERSION#": '4.0',
                      "#GET_ROUTING_TABLE#": routing,
                      "#PULL_ROUTING_TABLE#": 'PULL {{"n": {routes_pull_n}}}'.format(**vars),
                      "#PULL#": 'PULL {"n": 1000}',
                      "#RUN_READ#": 'RUN "RETURN 1 as n" {{}} {{"mode": "r", "db": "{dbName}"}}'.format(**vars),
                      "#RUN_WRITE#": 'RUN "RETURN 1 as n" {{}} {{"db": "{dbName}"}}'.format(**vars),
                      "#BEGIN_READ#": 'BEGIN {{"mode": "r", "db": "{dbName}"}}'.format(**vars),
                      "#BEGIN_WRITE#": 'BEGIN {{"db": "{dbName}"}}'.format(**vars),
                      "#HOST#": self._routingServer.host}

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

        self._readServer.start(script=script_read, vars=self._vars)
        session = self._driver.session('r', database=self._database)
        session.run("RETURN 1 as n")
        session.close()
        self._routingServer.done()
        self._readServer.done()

    # Same test as for session.run but for transaction run.
    def test_tx_run_read(self):
        if self._driverName not in ['go', 'dotnet']:
            self.skipTest("Session with named database not implemented in backend")

        self._readServer.start(script=script_tx_read, vars=self._vars)
        session = self._driver.session('r', database=self._database)
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()
        self._routingServer.done()
        self._readServer.done()

    # Checks that write server is used
    def test_session_run_write(self):
        if self._driverName not in ['go', 'dotnet']:
            self.skipTest("Session with named database not implemented in backend")

        self._writeServer.start(script=script_write, vars=self._vars)
        session = self._driver.session('w', database=self._database)
        session.run("RETURN 1 as n")
        session.close()
        self._routingServer.done()
        self._writeServer.done()

    # Checks that write server is used
    def test_tx_run_write(self):
        if self._driverName not in ['go', 'dotnet']:
            self.skipTest("Session with named database not implemented in backend")

        self._writeServer.start(script=script_tx_write, vars=self._vars)
        session = self._driver.session('w', database=self._database)
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()
        self._routingServer.done()
        self._writeServer.done()


class RoutingV3(Routing):

    def setupVars(self, port=None):
        # Bolt version 4 and later needs named databases when querying for routing table.
        self._database = None
        vars = {
            # In >= 4.1 an address was added to context to solve issues when setting up routing tables running in docker.
            # This has been backported to all prior protocol versions by some drivers since it is ignored by the database.
            'context': '{}',
            'config': '{"mode": "r"}',
        }

        if self._driverName in ['dotnet']:
            # Actually a 4.1 feature
            vars['context'] = '{{"address":"{host}:9001"}}'.format(host=os.environ.get("TEST_STUB_HOST"))
            # Request writer when fetching routing table
            vars['config'] = '{}'

        routing ='RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {{"context": {context}}} {config}'
        routing = routing.format(**vars)

        self._vars = {"#VERSION#": '3',
                      "#GET_ROUTING_TABLE#": routing,
                      "#PULL_ROUTING_TABLE#": 'PULL_ALL',
                      "#PULL#": 'PULL_ALL',
                      "#RUN_READ#": 'RUN "RETURN 1 as n" {} {"mode": "r"}',
                      "#RUN_WRITE#": 'RUN "RETURN 1 as n" {} {}',
                      "#BEGIN_READ#": 'BEGIN {"mode": "r"}',
                      "#BEGIN_WRITE#": 'BEGIN {}',
                      "#HOST#": self._routingServer.host}

