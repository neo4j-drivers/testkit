from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestRetryClustering(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._routingServer = StubServer(9001)
        self._readServer = StubServer(9002)
        self._writeServer = StubServer(9003)
        self._uri = ("neo4j://%s?region=china&policy=my_policy"
                     % self._routingServer.address)
        self._auth = types.AuthorizationToken(scheme="basic", principal="p",
                                              credentials="c")
        self._userAgent = "007"

    def tearDown(self):
        self._routingServer.reset()
        self._readServer.reset()
        self._writeServer.reset()
        super().tearDown()

    def test_read(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['java']:
            self.skipTest("needs ROUTE bookmark list support")
        self._routingServer.start(
            path=self.script_path("clustering", "router_no_retry.script"),
            vars=self.get_vars()
        )
        self._readServer.start(path=self.script_path("read.script"))
        num_retries = 0

        def once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        session = driver.session("r")
        x = session.readTransaction(once)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 1)

        session.close()
        driver.close()
        self._readServer.done()
        self._routingServer.done()

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

    def test_retry_ForbiddenOnReadOnlyDatabase(self):
        if get_driver_name() in ['dotnet']:
            self.skipTest("Behaves strange")

        self._run_with_transient_error(
            "retry_with_fail_after_pull.script",
            "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase"
        )

    def test_retry_NotALeader(self):
        if get_driver_name() in ['dotnet']:
            self.skipTest("Behaves strange")

        self._run_with_transient_error(
            "retry_with_fail_after_pull.script",
            "Neo.ClientError.Cluster.NotALeader"
        )

    def test_retry_ForbiddenOnReadOnlyDatabase_ChangingWriter(self):
        if get_driver_name() in ['dotnet']:
            self.skipTest("Behaves strange")

        self._routingServer.start(
            path=self.script_path("clustering",
                                  "router_swap_reader_and_writer.script"),
            vars=self.get_vars()
        )
        # We could probably use AUTO RESET in the script but this makes the
        # diffs more obvious.
        vars = {
            "#EXTRA_RESET_1#": "",
            "#EXTRA_RESET_2#": "",
            "#ERROR#": "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase",
        }
        if get_driver_name() not in ["go", "python"]:
            vars["#EXTRA_RESET_2#"] = "C: RESET\nS: SUCCESS {}"
        if get_driver_name() in ["java", "javascript"]:
            vars["#EXTRA_RESET_1#"] = "C: RESET\nS: SUCCESS {}"

        self._writeServer.start(
            path=self.script_path("retry_with_fail_after_pull_server1.script"),
            vars=vars
        )
        self._readServer.start(
            path=self.script_path("retry_with_fail_after_pull_server2.script"),
            vars=vars
        )

        num_retries = 0

        def twice(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)

        session = driver.session("r")
        x = session.writeTransaction(twice)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 2)

        session.close()
        driver.close()
        self._writeServer.done()
        self._routingServer.done()
        self._readServer.done()

    def _run_with_transient_error(self, script, err):
        self._routingServer.start(
            path=self.script_path("clustering", "router.script"),
            vars=self.get_vars()
        )
        # We could probably use AUTO RESET in the script but this makes the
        # diffs more obvious.
        vars = {
            "#EXTRA_RESET_1#": "",
            "#EXTRA_RESET_2#": "",
            "#ERROR#": err,
        }
        if get_driver_name() not in ["go", "python"]:
            vars["#EXTRA_RESET_2#"] = "C: RESET\nS: SUCCESS {}"
        if get_driver_name() in ["java", "javascript"]:
            vars["#EXTRA_RESET_1#"] = "C: RESET\nS: SUCCESS {}"

        self._writeServer.start(path=self.script_path(script), vars=vars)
        num_retries = 0

        def twice(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)

        session = driver.session("r")
        x = session.writeTransaction(twice)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 2)

        session.close()
        driver.close()
        self._writeServer.done()
        self._routingServer.done()

    def get_vars(self):
        host = self._routingServer.host
        v = {
            "#VERSION#": "4.3",
            "#HOST#": host,
            "#ROUTINGCTX#":
                '{"address": "' + host
                + ':9001", "region": "china", "policy": "my_policy"}',
            "#EXTRA_HELLO_PROPS#": self.get_extra_hello_props(),
        }
        v["#HELLO_ROUTINGCTX#"] = v["#ROUTINGCTX#"]

        return v

    def get_extra_hello_props(self):
        if get_driver_name() in ["java"]:
            return ', "realm": ""'
        elif get_driver_name() in ["javascript"]:
            return ', "realm": "", "ticket": ""'
        return ""
