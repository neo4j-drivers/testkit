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
        self._last_exc = None

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
        except types.DriverError as exc:
            self._last_exc = exc
            return "after run"

        try:
            result.next()
        except types.DriverError as exc:
            self._last_exc = exc
            return "after first next"

        try:
            result.next()
        except types.DriverError as exc:
            self._last_exc = exc
            return "after last next"

        return "success"

    # Helper function that runs the sequence and returns the name of the step
    # at which the error happened.
    def _run_tx(self):
        try:
            tx = self._session.beginTransaction()
        except types.DriverError as exc:
            self._last_exc = exc
            return "after begin"
        try:
            result = tx.run("RETURN 1 as n")
        except types.DriverError as exc:
            self._last_exc = exc
            return "after run"

        try:
            result.next()
        except types.DriverError as exc:
            self._last_exc = exc
            return "after first next"

        try:
            result.next()
        except types.DriverError as exc:
            self._last_exc = exc
            return "after last next"
        try:
            tx.commit()
        except types.DriverError as exc:
            self._last_exc = exc
            return "after commit"

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
        if get_driver_name() in ["go"]:
            self.skipTest("Crashed backend")
        self._server.start(
            path=self.script_path("exit_after_hello_success.script"),
            vars=self.get_vars()
        )
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after first next"
        if self._driverName in ["go", "python", "java"]:
            expected_step = "after run"
        self.assertEqual(step, expected_step)

    def test_disconnect_session_on_run(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt run message.
        self._server.start(path=self.script_path("exit_after_run.script"),
                           vars=self.get_vars())
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after first next"
        if self._driverName in ["go", "python", "java"]:
            # Go reports this error earlier
            expected_step = "after run"
        self.assertEqual(step, expected_step)

    def test_disconnect_on_pull(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt PULL message.
        self._server.start(path=self.script_path("exit_after_pull.script"),
                           vars=self.get_vars())
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after first next"
        self.assertEqual(step, expected_step)

    def test_disconnect_session_on_pull_after_record(self):
        # Verifies how the driver handles when server disconnects after driver
        # sent bolt RUN message and received a RECORD but no summary.
        self._server.start(path=self.script_path("exit_after_record.script"),
                           vars=self.get_vars())
        step = self._run()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after last next"
        self.assertEqual(step, expected_step)

    def test_disconnect_on_tx_begin(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt BEGIN message.
        if self._driverName in ["go"]:
            self.skipTest("Driver fails on session.close")
        self._server.start(path=self.script_path("exit_after_tx_begin.script"),
                           vars=self.get_vars())
        step = self._run_tx()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after begin"
        if self._driverName in ["python", "go"]:
            expected_step = "after run"
        elif self._driverName in ["javascript", "dotnet"]:
            expected_step = "after first next"
        self.assertEqual(step, expected_step)

    def test_disconnect_on_tx_run(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt RUN message within a transaction.
        if self._driverName in ["go"]:
            self.skipTest("Driver fails on session.close")
        self._server.start(path=self.script_path("exit_after_tx_run.script"),
                           vars=self.get_vars())
        step = self._run_tx()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after run"
        if self._driverName in ["javascript", "dotnet"]:
            expected_step = "after first next"
        self.assertEqual(step, expected_step)

    def test_disconnect_on_tx_pull(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt PULL message within a transaction.
        if self._driverName in ["go"]:
            self.skipTest("Driver fails on session.close")
        self._server.start(path=self.script_path("exit_after_tx_pull.script"),
                           vars=self.get_vars())
        step = self._run_tx()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after first next"
        self.assertEqual(step, expected_step)

    def test_disconnect_session_on_tx_pull_after_record(self):
        # Verifies how the driver handles when server disconnects after driver
        # sent bolt RUN message and received a RECORD but no summary within a
        # transaction.
        if self._driverName in ["go"]:
            self.skipTest("Driver fails on session.close")
        self._server.start(path=self.script_path("exit_after_tx_record.script"),
                           vars=self.get_vars())
        step = self._run_tx()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after last next"
        self.assertEqual(step, expected_step)

    def test_disconnect_session_on_tx_commit(self):
        # Verifies how the driver handles when server disconnects right after
        # driver sent bolt run message.
        self._server.start(path=self.script_path("exit_after_tx_commit.script"),
                           vars=self.get_vars())
        step = self._run_tx()
        self._session.close()
        self._driver.close()
        self._server.done()

        expected_step = "after commit"
        self.assertEqual(step, expected_step)
        if get_driver_name() in ['python']:
            self.assertEqual(
                "<class 'neo4j.exceptions.IncompleteCommit'>",
                self._last_exc.errorType
            )

    # FIXME: This test doesn't really fit here. It tests FAILURE handling, not
    #        handling sudden loss of connectivity.
    def test_fail_on_reset(self):
        if self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            self.skipTest("The failing RESET never gets issued by an optimized "
                          "driver.")
        self._server.start(
            path=self.script_path("failure_on_reset_after_success.script"),
            vars=self.get_vars()
        )
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
        self._server.start(
            path=self.script_path("explicit_goodbye_after_run.script"),
            vars=self.get_vars()
        )
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
            return ', "realm": ""'
        elif self._driverName == "dotnet":
            return ', "routing": null'
        else:
            return ""
