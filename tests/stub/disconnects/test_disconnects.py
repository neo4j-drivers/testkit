from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer

# Should match user-agent of disconnect_on_hello.script
# Indirectly tests implementation of custom user-agent
customUserAgent = "Modesty"


class TestDisconnects(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()
        auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
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
        self._server.start(path=self.script_path("exit_after_hello.script"),
                           vars=self.get_vars())
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after first next"
        if self._driverName in ["go", "java", "dotnet", "python"]:
            expected_step = "after run"
        self.assertEqual(step, expected_step)

    def test_disconnect_after_hello(self):
        # Verifies how the driver handles when server disconnects right after
        # acknowledging a HELLO message with SUCCESS.
        self._server.start(
            path=self.script_path("exit_after_hello_success.script"),
            vars=self.get_vars()
        )
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after first next"
        if self._driverName in ["go", "dotnet", "python"]:
            expected_step = "after run"
        self.assertEqual(step, expected_step)

    def test_disconnect_on_run(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt run message.
        self._server.start(path=self.script_path("exit_after_run.script"))
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after first next"
        if self._driverName in ["go", "python"]:
            # Go reports this error earlier
            expected_step = "after run"
        self.assertEqual(step, expected_step)

    def test_disconnect_on_pull(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt PULL message.
        self._server.start(path=self.script_path("exit_after_pull.script"))
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after first next"
        self.assertEqual(step, expected_step)

    # FIXME: This test doesn't really fit here. It tests FAILURE handling, not
    #        handling sudden loss of connectivity.
    def test_fail_on_reset(self):
        self._server.start(path=self.script_path(
            "failure_on_reset_after_success.script"
        ))
        step = self._run()
        self._session.close()
        accept_count = self._server.count_responses("<ACCEPT>")
        hangup_count = self._server.count_responses("<HANGUP>")
        active_connections = accept_count - hangup_count
        self._server.done()
        self.assertEqual(step, "success")
        self.assertEqual(accept_count, 1)
        self.assertEqual(hangup_count, 1)
        self.assertEqual(active_connections, 0)

    def test_client_says_goodbye(self):
        self._server.start(path=self.script_path("explicit_goodbye_after_run.script"))
        result = self._session.run("RETURN 1 AS n")
        result.next()
        self._session.close()
        self.assertEqual(self._server.count_requests("GOODBYE"), 0)
        self._driver.close()
        self._server.done()
        self.assertEqual(self._server.count_requests("GOODBYE"), 1)

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
