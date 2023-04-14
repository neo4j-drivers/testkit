import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestRetry(TestkitTestCase):

    required_features = types.Feature.BOLT_4_3,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._driver_name = get_driver_name()

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and its output is dumped for analysis.
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

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        driver = Driver(self._backend,
                        "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.execute_read(once)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 1)

        session.close()
        driver.close()
        self._server.done()

    def _run_with_transient_error(self, script, err):
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

        self._server.start(path=self.script_path(script), vars_=vars_)
        num_retries = 0

        def twice(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        driver = Driver(self._backend,
                        "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.execute_write(twice)
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
        if self._driver_name in ["java", "dotnet"]:
            self.skipTest("Keeps retrying on commit despite connection "
                          "being dropped")
        self._server.start(path=self.script_path("commit_disconnect.script"))
        num_retries = 0

        def once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            result.next()

        auth = types.AuthorizationToken("basic", principal="", credentials="")
        driver = Driver(self._backend,
                        "bolt://%s" % self._server.address, auth)
        session = driver.session("w")

        with self.assertRaises(types.DriverError) as e:  # Check further...
            session.execute_write(once)
        if get_driver_name() in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.IncompleteCommit'>",
                e.exception.errorType
            )

        self.assertEqual(num_retries, 1)
        session.close()
        driver.close()
        self._server.done()

    def test_no_retry_on_syntax_error(self):
        # Should NOT retry when error isn't transient
        def _test():
            self._server.start(
                path=self.script_path(mode + "_syntax_error.script")
            )
            num_retries = 0
            exception = None

            def once(tx):
                nonlocal exception
                nonlocal num_retries
                num_retries = num_retries + 1
                with self.assertRaises(types.DriverError) as exc_:
                    result = tx.run("RETURN 1")
                    result.next()
                exception = exc_.exception
                raise exception

            auth = types.AuthorizationToken("basic", principal="",
                                            credentials="")
            driver = Driver(self._backend,
                            "bolt://%s" % self._server.address, auth)
            session = driver.session(mode[0])

            with self.assertRaises(types.DriverError):  # TODO: check further
                getattr(session, "execute_" + mode)(once)
            # TODO: remove the condition when go sends the error code
            if get_driver_name() not in ["go"]:
                self.assertEqual(exception.code,
                                 "Neo.ClientError.Statement.SyntaxError")

            self.assertEqual(num_retries, 1)
            session.close()
            driver.close()
            self._server.done()

        for mode in ("read", "write"):
            with self.subTest(mode=mode):
                _test()
            self._server.reset()

    def test_should_not_retry_non_retryable_tx_failures(self):
        def _test():
            self._server.start(
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

            auth = types.AuthorizationToken("basic", principal="",
                                            credentials="")
            driver = Driver(self._backend,
                            "bolt://%s" % self._server.address, auth)
            session = driver.session("w")

            with self.assertRaises(types.DriverError) as exc:
                session.execute_write(once)

            self.assertEqual(exc.exception.code, failure[1])

            self.assertEqual(num_retries, 1)
            session.close()
            driver.close()
            self._server.done()

        failures = [
            [
                "Neo.TransientError.Transaction.Terminated",
                "Neo.ClientError.Transaction.Terminated"
            ], [
                "Neo.TransientError.Transaction.LockClientStopped",
                "Neo.ClientError.Transaction.LockClientStopped"
            ]
        ]

        for failure in failures:
            with self.subTest(failure=failure):
                _test()
            self._server.reset()
