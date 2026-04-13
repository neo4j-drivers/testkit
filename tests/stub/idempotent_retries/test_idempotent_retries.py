from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestIdempotentRetries(TestkitTestCase):

    required_features = types.Feature.BOLT_6_0,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic", principal="",
                                                       credentials=""))
        self._session = None

    def tearDown(self):
        if self._session is not None:
            self._session.close()
        self._server.reset()
        super().tearDown()

    def test_retries_idempotent_error(self):
        self._server.start(
            path=self.script_path("idempotent_error_on_run.script")
        )
        self._session = self._driver.session("r")
        result = self._session.run("RETURN 1 AS n")
        records = result.list()
        print(result)
        self.assertEqual(types.Record(values=[types.CypherInt(1)]),
                         records[0])
        self._session.close()
        self._server.done()

    def test_retries_idempotent_error_on_session_run(self):
        self._server.start(
            path=self.script_path(
                "idempotent_error_followed_by_regular_error.script"
            )
        )
        self._session = self._driver.session("r")
        with self.assertRaises(types.DriverError) as exc:
            # TODO: remove this block once all languages work
            if get_driver_name() in ["javascript"]:
                self._session.run("RETURN 1 AS n").next()
            else:
                self._session.run("RETURN 1 AS n")
        self.assertEqual(exc.exception.code, "Neo.ClientError.MadeUp.Code")
        self._server.done()

    def test_throws_second_error_on_session_run(self):
        self._server.start(
            path=self.script_path("two_idempotent_errors_on_run.script")
        )
        self._session = self._driver.session("r")
        with self.assertRaises(types.DriverError) as exc:
            # TODO: remove this block once all languages work
            if get_driver_name() in ["javascript"]:
                self._session.run("RETURN 1 AS n").next()
            else:
                self._session.run("RETURN 1 AS n")
        self.assertEqual(exc.exception.code, "Neo.ClientError.MadeUp.Code")
        self._server.done()
