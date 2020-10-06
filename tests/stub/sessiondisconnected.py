import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


# Should match user-agent of disconnect_on_hello.script
# Indirectly tests implementation of custom user-agent
customUserAgent = "Modesty"

# Scripts that disconnects on different parts of a session
script_on_hello = """
!: BOLT 4

C: HELLO {"user_agent": "Modesty", "scheme": "basic", "principal": "neo4j", "credentials": "pass"}
S: <EXIT>
"""
script_on_run = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {}
S: <EXIT>
C: PULL {"n": 1000}
"""
script_on_pull = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {}
C: PULL {"n": 1000}
S: <EXIT>
"""


class SessionRunDisconnected(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()
        auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth, userAgent=customUserAgent)
        self._session = self._driver.session("w")

    def tearDown(self):
        self._session.close()
        self._driver.close()
        self._backend.close()
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analys.
        self._server.reset()

    # Helper function that runs the sequence and returns the name of the step at which the
    # error happened.
    def _run(self):
        try:
            result = self._session.run("RETURN 1 as n")
        except types.DriverError:
            return "after run"

        try:
            record = result.next()
        except types.DriverError:
            return "after first next"

        try:
            nullRecord = result.next()
        except types.DriverError:
            return "after last next"

        return "success"

    def test_disconnect_on_hello(self):
        # Verifies how the driver handles when server disconnects right after driver sent bolt
        # hello message.
        if not self._driverName in ["go"]:
            self.skipTest("No support for custom user-agent in testkit backend")
        self._server.start(script=script_on_hello)
        step = self._run()
        self._server.done()

        expectedStep = "after first next"
        if self._driverName in ["go"]:
            # Go reports this error earlier
            expectedStep = "after run"
        self.assertEqual(step, expectedStep)

    def test_disconnect_on_run(self):
        # Verifies how the driver handles when server disconnects right after driver sent bolt
        # run message.
        self._server.start(script=script_on_run)
        step = self._run()
        self._server.done()

        expectedStep = "after first next"
        if self._driverName in ["go"]:
            # Go reports this error earlier
            expectedStep = "after run"
        self.assertEqual(step, expectedStep)

    def test_disconnect_on_pull(self):
        # Verifies how the driver handles when server disconnects right after driver sent bolt
        # pull message.
        self._server.start(script=script_on_pull)
        step = self._run()
        self._server.done()

        expectedStep = "after first next"
        if self._driverName in ["go"]:
            # Go reports this error earlier
            expectedStep = "after run"
        self.assertEqual(step, expectedStep)

