from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestRetryClustering(TestkitTestCase):

    required_features = types.Feature.BOLT_4_3,

    def setUp(self):
        super().setUp()
        self._routingServer = StubServer(9001)
        self._readServer = StubServer(9002)
        self._writeServer = StubServer(9003)
        self._uri = ("neo4j://%s?region=china&policy=my_policy"
                     % self._routingServer.address)
        self._auth = types.AuthorizationToken("basic", principal="p",
                                              credentials="c")
        self._userAgent = "007"

    def tearDown(self):
        self._routingServer.reset()
        self._readServer.reset()
        self._writeServer.reset()
        super().tearDown()

    def test_read(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["java"]:
            self.skipTest("needs ROUTE bookmark list support")
        self._routingServer.start(
            path=self.script_path("clustering", "router_no_retry.script"),
            vars_=self.get_vars()
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
        x = session.read_transaction(once)
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

    def test_disconnect_on_commit(self):
        # Should NOT retry when connection is lost on unconfirmed commit.
        # The rule could be relaxed on read transactions therefore we test on
        # writeTransaction.  An error should be raised to indicate the failure
        if get_driver_name() in ["java", "dotnet", "javascript"]:
            self.skipTest("Keeps retrying on commit despite connection "
                          "being dropped")
        self._routingServer.start(
            path=self.script_path("clustering", "router.script"),
            vars_=self.get_vars()
        )
        self._writeServer.start(
            path=self.script_path("commit_disconnect.script")
        )
        num_retries = 0

        def once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            result.next()

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        session = driver.session("w")

        with self.assertRaises(types.DriverError) as e:  # Check further...
            session.write_transaction(once)
        if get_driver_name() in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.IncompleteCommit'>",
                e.exception.errorType
            )

        self.assertEqual(num_retries, 1)
        session.close()
        driver.close()
        self.assertEqual(self._routingServer.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._writeServer.count_responses("<ACCEPT>"), 1)
        self._routingServer.done()
        self._readServer.done()

    def test_retry_ForbiddenOnReadOnlyDatabase(self):  # noqa: N802
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Behaves strange")

        self._run_with_transient_error(
            "retry_with_fail_after_pull.script",
            "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase"
        )

    def test_retry_NotALeader(self):  # noqa: N802
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Behaves strange")

        self._run_with_transient_error(
            "retry_with_fail_after_pull.script",
            "Neo.ClientError.Cluster.NotALeader"
        )

    def test_retry_ForbiddenOnReadOnlyDatabase_ChangingWriter(self):  # noqa: N802,E501
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Behaves strange")

        self._routingServer.start(
            path=self.script_path("clustering",
                                  "router_swap_reader_and_writer.script"),
            vars_=self.get_vars()
        )
        # We could probably use AUTO RESET in the script but this makes the
        # diffs more obvious.
        vars_ = {
            "#EXTRA_RESET_1#": "",
            "#EXTRA_RESET_2#": "",
            "#ERROR#": "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase",
        }
        if not self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            vars_.update({
                "#EXTRA_RESET_1#": "{*\n    C: RESET\n    S: SUCCESS {}\n*}",
                "#EXTRA_RESET_2#": "{*\n    C: RESET\n    S: SUCCESS {}\n*}",
            })

        self._writeServer.start(
            path=self.script_path("retry_with_fail_after_pull_server1.script"),
            vars_=vars_
        )
        self._readServer.start(
            path=self.script_path("retry_with_fail_after_pull_server2.script"),
            vars_=vars_
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
        x = session.write_transaction(twice)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 2)

        session.close()
        driver.close()
        self._writeServer.done()
        self._routingServer.done()
        self._readServer.done()

    def test_should_not_retry_non_retryable_tx_failures(self):
        def _test():
            self._routingServer.start(
                path=self.script_path("clustering",
                                      "router.script"),
                vars_=self.get_vars()
            )
            self._writeServer.start(
                path=self.script_path("tx_pull_yielding_failure.script"),
                vars_={
                    "#FAILURE#": '{"code": "%s", "message": "message"}'
                                 % failure[0]
                }
            )
            num_retries = 0

            def once(tx):
                nonlocal num_retries
                num_retries = num_retries + 1
                result = tx.run("RETURN 1 as n")
                result.next()

            driver = Driver(self._backend, self._uri, self._auth,
                            self._userAgent)
            session = driver.session("w")

            with self.assertRaises(types.DriverError) as exc:
                session.write_transaction(once)

            self.assertEqual(exc.exception.code, failure[1])

            self.assertEqual(num_retries, 1)
            session.close()
            driver.close()
            self._routingServer.done()
            self._writeServer.done()  #

        failures = []
        failures.append(
            ["Neo.TransientError.Transaction.Terminated",
                "Neo.ClientError.Transaction.Terminated"])
        failures.append(
            ["Neo.TransientError.Transaction.LockClientStopped",
                "Neo.ClientError.Transaction.LockClientStopped"])

        for failure in (failures):
            with self.subTest(failure=failure):
                _test()
            self._routingServer.reset()
            self._writeServer.reset()

    def _run_with_transient_error(self, script, err):
        self._routingServer.start(
            path=self.script_path("clustering", "router.script"),
            vars_=self.get_vars()
        )
        # We could probably use AUTO RESET in the script but this makes the
        # diffs more obvious.
        vars_ = {
            "#EXTRA_RESET_1#": "",
            "#EXTRA_RESET_2#": "",
            "#ERROR#": err,
        }
        if not self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            vars_.update({
                "#EXTRA_RESET_1#": "{*\n    C: RESET\n    S: SUCCESS {}\n*}",
                "#EXTRA_RESET_2#": "{*\n    C: RESET\n    S: SUCCESS {}\n*}",
            })

        self._writeServer.start(path=self.script_path(script), vars_=vars_)
        num_retries = 0

        def twice(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)

        session = driver.session("r")
        x = session.write_transaction(twice)
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
        }
        v["#HELLO_ROUTINGCTX#"] = v["#ROUTINGCTX#"]

        return v
