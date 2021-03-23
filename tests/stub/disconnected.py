from tests.shared import new_backend, get_driver_name, TestkitTestCase
from tests.stub.shared import StubServer
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


# Should match user-agent of disconnect_on_hello.script
# Indirectly tests implementation of custom user-agent
customUserAgent = "Modesty"

# Scripts that disconnects on different parts of a session
script_on_hello = """
!: BOLT 4

C: HELLO {"user_agent": "Modesty", "scheme": "basic", "principal": "neo4j", "credentials": "pass" #EXTRA_HELLO_PARAMS# }
S: <EXIT>
"""
script_on_run = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {}
S: <EXIT>
"""
script_on_pull = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET

C: RUN "RETURN 1 as n" {} {}
C: PULL {"n": 1000}
S: <EXIT>
"""


class SessionRunDisconnected(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()
        auth = AuthorizationToken(scheme="basic", principal="neo4j",
                                  credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth,
                              userAgent=customUserAgent)
        self._session = self._driver.session("w")

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        super().tearDown()

    # Helper function that runs the sequence and returns the name of the step
    # at which the error happened.
    def _run(self):
        try:
            result = self._session.run("RETURN 1 as n")
        except types.DriverError:
            return "after run"

        try:
            result.next()
        except types.DriverError:
            return "after first next"

        try:
            result.next()
        except types.DriverError:
            return "after last next"

        return "success"

    def test_disconnect_on_hello(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt HELLO message.
        if self._driverName in ['python']:
            self.skipTest("No support for custom user-agent in backend")
        self._server.start(script=script_on_hello, vars=self.get_vars())
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expectedStep = "after first next"
        if self._driverName in ["go", "java", "dotnet"]:
            expectedStep = "after run"
        self.assertEqual(step, expectedStep)

    def test_disconnect_on_run(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt run message.
        if self._driverName in ['python']:
            self.skipTest("Too raw error handling")
        self._server.start(script=script_on_run)
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expectedStep = "after first next"
        if self._driverName in ["go"]:
            # Go reports this error earlier
            expectedStep = "after run"
        self.assertEqual(step, expectedStep)

    def test_disconnect_on_pull(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt PULL message.
        if self._driverName in ['python']:
            self.skipTest("Too raw error handling")
        self._server.start(script=script_on_pull)
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expectedStep = "after first next"
        if self._driverName in ["go"]:
            # Go reports this error earlier
            expectedStep = "after run"
        self.assertEqual(step, expectedStep)

    def get_vars(self):
        return {
            "#EXTRA_HELLO_PARAMS#": self.get_extra_hello_props()
        }

    def get_extra_hello_props(self):
        if self._driverName == "javascript":
            return ', "realm": "", "ticket": ""'
        elif self._driverName == "java":
            return ', "realm": "", "routing": null'
        else:
            return ""
