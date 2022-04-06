from contextlib import contextmanager

from nutkit.frontend import Driver
import nutkit.protocol as types
from nutkit.protocol.error_type import ErrorType
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestTxLifetime(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9000)

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        super().tearDown()

    @contextmanager
    def _start_session(self, script):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path("v4x4", script))
        session = driver.session("r", fetch_size=2)
        try:
            yield session
        finally:
            session.close()
            driver.close()

    def _asserts_tx_closed_error(self, exc):
        driver = get_driver_name()
        assert isinstance(exc, types.DriverError)
        if driver in ["python"]:
            self.assertEqual(ErrorType.TRANSACTION_ERROR.value, exc.errorType)
            self.assertIn("closed", exc.msg.lower())
        elif driver in ["java"]:
            self.assertEqual(ErrorType.CLIENT_ERROR.value, exc.errorType)
        elif driver in ["javascript", "go", "dotnet"]:
            self.assertIn("transaction", exc.msg.lower())
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def _asserts_tx_managed_error(self, exc):
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual(exc.errorType, "<class 'AttributeError'>")
            self.assertIn("managed", exc.msg.lower())
        elif driver in ["go"]:
            self.assertIn("retryable transaction", exc.msg.lower())
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def _test_unmanaged_tx(self, first_action, second_action):
        exc = None
        script = "tx_inf_results_until_end.script"
        with self._start_session(script) as session:
            tx = session.begin_transaction()
            res = tx.run("Query")
            res.consume()
            getattr(tx, first_action)()
            if second_action == "close":
                getattr(tx, second_action)()
            elif second_action == "run":
                with self.assertRaises(types.DriverError) as exc:
                    tx.run("Query").consume()
            else:
                with self.assertRaises(types.DriverError) as exc:
                    getattr(tx, second_action)()

        self._server.done()
        self.assertEqual(
            self._server.count_requests("ROLLBACK"),
            int(first_action in ["rollback", "close"])
        )
        self.assertEqual(
            self._server.count_requests("COMMIT"),
            int(first_action == "commit")
        )
        if exc is not None:
            self._asserts_tx_closed_error(exc.exception)

    def test_unmanaged_tx_raises_tx_closed_exec(self):
        for first_action in ("commit", "rollback", "close"):
            for second_action in ("commit", "rollback", "close", "run"):
                with self.subTest(first_action=first_action,
                                  second_action=second_action):
                    self._test_unmanaged_tx(first_action, second_action)
                self._server.reset()
