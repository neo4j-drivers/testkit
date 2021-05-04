from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestRetry(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        super().tearDown()

    def test_read(self):
        self._server.start(path=self.script_path("read.script"))
        num_retries = 0

        def once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                        credentials="pass")
        driver = Driver(self._backend,
                        "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.readTransaction(once)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 1)

        session.close()
        driver.close()
        self._server.done()

    def _run_with_transient_error(self, script, err):
        # We could probably use AUTO RESET in the script but this makes the
        # diffs more obvious.
        vars = {
            "#EXTRA_RESET_1#": "",
            "#EXTRA_RESET_2#": "",
            "#ERROR#": err,
        }
        if self._driverName not in ["go", "python"]:
            vars["#EXTRA_RESET_2#"] = "C: RESET\nS: SUCCESS {}"
        if self._driverName in ["java", "javascript"]:
            vars["#EXTRA_RESET_1#"] = "C: RESET\nS: SUCCESS {}"

        self._server.start(path=self.script_path(script), vars=vars)
        num_retries = 0

        def twice(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                        credentials="pass")
        driver = Driver(self._backend,
                        "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.writeTransaction(twice)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 2)

        session.close()
        driver.close()
        self._server.done()

    def test_retry_database_unavailable(self):
        # Simple case, correctly classified transient error
        self._run_with_transient_error(
            "retry_with_fail_after_commit.script",
            "Neo.TransientError.Database.DatabaseUnavailable"
        )

    def test_retry_made_up_transient(self):
        # Driver should retry all transient error (with some exceptions), make
        # up a transient error and the driver should retry.
        self._run_with_transient_error(
            "retry_with_fail_after_commit.script",
            "Neo.TransientError.Completely.MadeUp"
        )

    def test_disconnect_on_commit(self):
        # Should NOT retry when connection is lost on unconfirmed commit.
        # The rule could be relaxed on read transactions therefore we test on
        # writeTransaction.  An error should be raised to indicate the failure
        if self._driverName in ["java", 'dotnet']:
            self.skipTest("Keeps retrying on commit despite connection "
                          "being dropped")
        self._server.start(path=self.script_path("commit_disconnect.script"))
        num_retries = 0

        def once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            result.next()
        auth = types.AuthorizationToken(scheme="basic")
        driver = Driver(self._backend,
                        "bolt://%s" % self._server.address, auth)
        session = driver.session("w")

        with self.assertRaises(types.DriverError):  # Check further...
            session.writeTransaction(once)

        self.assertEqual(num_retries, 1)
        session.close()
        driver.close()
        self._server.done()
