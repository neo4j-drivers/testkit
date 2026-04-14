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
        types.Feature.IDEMPOTENT_RETRIES
    )

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic", principal="",
                                                       credentials=""))

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def test_retries_idempotent_error(self):
        self._server.start(
            path=self.script_path("idempotent_error_on_run.script")
        )
        with self._driver.session("r") as session:
            result = session.run("RETURN 1 AS n")
            records = result.list()
            self.assertEqual(types.Record(values=[types.CypherInt(1)]),
                             records[0])
        self._server.done()

    def test_idempotent_retry_does_not_resend_telemetry(self):
        self._server.start(
            path=self.script_path(
                "idempotent_error_on_run_with_telemetry.script"
            )
        )
        with self._driver.session("r") as session:
            result = session.run("RETURN 1 AS n")
            records = result.list()
            self.assertEqual(types.Record(values=[types.CypherInt(1)]),
                             records[0])
            telemetry_requests = self._server.get_requests("TELEMETRY")
            self.assertEqual(len(telemetry_requests), 1)
        self._server.done()

    def test_retries_idempotent_error_on_session_run(self):
        self._server.start(
            path=self.script_path(
                "idempotent_error_followed_by_regular_error.script"
            )
        )
        with self._driver.session("r") as session:
            with self.assertRaises(types.DriverError) as exc:
                # TODO: remove this block once all languages work
                if get_driver_name() in ["javascript", "dotnet"]:
                    session.run("RETURN 1 AS n").next()
                else:
                    session.run("RETURN 1 AS n")
            self.assertEqual(exc.exception.code, "Neo.ClientError.MadeUp.Code")
        self._server.done()

    def test_throws_second_error_on_session_run(self):
        self._server.start(
            path=self.script_path("two_idempotent_errors_on_run.script")
        )
        with self._driver.session("r") as session:
            with self.assertRaises(types.DriverError) as exc:
                # TODO: remove this block once all languages work
                if get_driver_name() in ["javascript", "dotnet"]:
                    session.run("RETURN 1 AS n").next()
                else:
                    session.run("RETURN 1 AS n")
            self.assertEqual(exc.exception.code, "Neo.ClientError.MadeUp.Code")
        self._server.done()
