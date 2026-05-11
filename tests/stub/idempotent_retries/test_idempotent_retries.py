from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestIdempotentRetries(TestkitTestCase):

    required_features = (
        types.Feature.BOLT_6_0,
        types.Feature.IDEMPOTENT_RETRIES,
    )

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def _driver(self, disable_auto_commit_retries=None):
        uri = "bolt://%s" % self._server.address
        auth = types.AuthorizationToken("basic", principal="", credentials="")
        return Driver(
            self._backend, uri, auth,
            disable_auto_commit_retries=disable_auto_commit_retries,
        )

    def _run_return_one(
        self, should_succeed, error_code="", driver_config=None,
        session_config=None, error_on_pull=False,
    ):
        with self._driver(
            disable_auto_commit_retries=driver_config
        ) as driver:
            with driver.session(
                "r", disable_auto_commit_retries=session_config
            ) as session:
                if should_succeed:
                    result = session.run("RETURN 1 AS n")
                    records = list(result)
                    self.assertEqual(
                        types.Record(values=[types.CypherInt(1)]),
                        records[0]
                    )
                    self.assertEqual(1, len(records))
                else:
                    with self.assertRaises(types.DriverError) as exc:
                        # TODO: remove driver name check once js and .net work
                        if get_driver_name() in [
                            "javascript",
                            "dotnet"
                        ] or error_on_pull:
                            session.run("RETURN 1 AS n").next()
                        else:
                            session.run("RETURN 1 AS n")
                    self.assertEqual(
                        exc.exception.code,
                        error_code
                    )

    def test_retries_idempotent_error(self):
        self._server.start(
            path=self.script_path(
                "idempotent_error_then_success_on_run.script"
            )
        )
        with self._driver() as driver:
            with driver.session("r") as session:
                self._run_return_one(session, True)
        self._server.done()

    def test_idempotent_retry_does_not_resend_telemetry(self):
        self._server.start(
            path=self.script_path(
                "idempotent_error_on_run_with_telemetry.script"
            )
        )
        self._run_return_one(True)
        telemetry_requests = self._server.count_requests("TELEMETRY")
        self.assertEqual(telemetry_requests, 1)
        self._server.done()

    def test_retries_idempotent_error_on_session_run(self):
        self._server.start(
            path=self.script_path(
                "idempotent_error_then_regular_error_on_run.script"
            )
        )
        self._run_return_one(
            False,
            "Neo.ClientError.MadeUp.Code",
        )
        self._server.done()

    def test_throws_second_error_on_session_run(self):
        self._server.start(
            path=self.script_path("two_idempotent_errors_on_run.script")
        )
        self._run_return_one(
            False,
            "Neo.ClientError.MadeUp.Code",
        )
        self._server.done()

    def test_throws_idempotent_error_on_pull(self):
        self._server.start(
            path=self.script_path("idempotent_error_on_pull.script")
        )
        self._run_return_one(
            False,
            "Neo.ClientError.MadeUp.Idempotent",
            error_on_pull=True,
        )
        self._server.done()

    def test_throws_idempotent_error_on_telemetry(self):
        self._server.start(
            path=self.script_path("idempotent_error_on_telemetry.script")
        )
        self._run_return_one(
            False,
            "Neo.ClientError.MadeUp.Idempotent",
        )
        self._server.done()

    def test_session_and_driver_configs(self):
        for (driver_config, session_config, should_retry) in (
            (True, None, False),
            (True, False, True),
            (True, True, False),
            (None, None, True),
            (None, False, True),
            (None, True, False),
            (False, None, True),
            (False, False, True),
            (False, True, False),
        ):
            with self.subTest(
                driver_config=driver_config,
                session_config=session_config,
                should_retry=should_retry,
            ):
                if should_retry:
                    script = "idempotent_error_then_success_on_run.script"
                else:
                    script = "idempotent_error_without_retry_on_run.script"
                self._server.start(path=self.script_path(script))
                self._run_return_one(
                    should_retry,
                    "Neo.ClientError.MadeUp.Idempotent",
                    driver_config,
                    session_config,
                )
                self._server.done()
            self._server.reset()

    def test_explicit_tx_does_not_retry(self):
        script = "idempotent_error_without_retry_explicit_tx.script"
        self._server.start(path=self.script_path(script))
        with self._driver() as driver:
            with driver.session("r") as session:
                with self.assertRaises(types.DriverError) as exc:
                    tx = session.begin_transaction()
                    if get_driver_name() in ["javascript"]:
                        tx.run("RETURN 1").next()
                    else:
                        tx.run("RETURN 1")
                    tx.commit()
                self.assertEqual(
                    exc.exception.code,
                    "Neo.ClientError.MadeUp.Idempotent"
                )
        self._server.done()
