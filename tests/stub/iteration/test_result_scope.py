from contextlib import contextmanager

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestResultScope(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)

    def tearDown(self):
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

    def _assert_result_out_of_scope_exception(self, exc):
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual(exc.errorType,
                             "<class 'neo4j.exceptions.ResultConsumedError'>")
            self.assertIn("closed", exc.msg.lower())
            self.assertIn("transaction", exc.msg.lower())
        elif driver in ["java"]:
            self.assertEqual(
                exc.errorType,
                "org.neo4j.driver.exceptions.ResultConsumedException"
            )
        elif driver in ["dotnet"]:
            self.assertEqual(exc.errorType, "ResultConsumedError")
        elif driver in ["javascript"]:
            self.assertIn(exc.msg, [
                "Result is already consumed",
                "Streaming has already started/consumed with a previous "
                "records or summary subscription."
            ])
        elif driver in ["go"]:
            self.assertEqual(exc.msg, "result cursor is not available anymore")
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def _assert_result_consumed_exception(self, exc):
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual(exc.errorType,
                             "<class 'neo4j.exceptions.ResultConsumedError'>")
            self.assertIn("consume", exc.msg.lower())
        elif driver in ["java"]:
            self.assertEqual(
                exc.errorType,
                "org.neo4j.driver.exceptions.ResultConsumedException"
            )
        elif driver in ["dotnet"]:
            self.assertEqual(exc.errorType, "ResultConsumedError")
        elif driver in ["javascript"]:
            self.assertIn(exc.msg, [
                "Result is already consumed",
                "Streaming has already started/consumed with a previous "
                "records or summary subscription."
            ])
        elif driver in ["go"]:
            self.assertEqual(exc.msg, "result cursor is not available anymore")
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def test_result_is_invalid_after_tx_ends_tx_func(self):
        def work(tx):
            result = tx.run("RETURN 1 AS n")
            if consume_inside_tx:
                result.consume()
            return result

        def _test(result_method_):
            with self._start_session(
                "tx_inf_results_until_end.script"
            ) as session:
                result = session.read_transaction(work)
                with self.assertRaises(types.DriverError) as exc:
                    if result_method_ == "peek":
                        result.peek()
                    else:
                        result.next()
                self._assert_result_out_of_scope_exception(exc.exception)
                self._server.done()

        for consume_inside_tx in (True, False):
            for result_method in ("next", "peek"):
                with self.subTest(result_method=result_method,
                                  consume_inside_tx=consume_inside_tx):
                    if result_method == "peek":
                        self.skip_if_missing_driver_features(
                            types.Feature.API_RESULT_PEEK
                        )
                    _test(result_method)
                self._server.reset()

    def test_result_is_invalid_after_consume_tx_func(self):
        def work(tx):
            result = tx.run("RETURN 1 AS n")
            result.consume()
            result.consume()  # can still consume
            with self.assertRaises(types.DriverError) as exc:
                if result_method == "peek":
                    result.peek()
                else:
                    result.next()
            self._assert_result_consumed_exception(exc.exception)

        def test():
            with self._start_session(
                "tx_inf_results_until_end.script"
            ) as session:
                session.read_transaction(work)
                self._server.done()

        for result_method in ("next", "peek"):
            with self.subTest(result_method=result_method):
                if result_method == "peek":
                    self.skip_if_missing_driver_features(
                        types.Feature.API_RESULT_PEEK
                    )
                test()
            self._server.reset()

    def test_result_is_invalid_after_tx_ends_tx_run(self):
        def test(tx_end_, result_method_):
            with self._start_session(
                "tx_inf_results_until_end.script"
            ) as session:
                tx = session.begin_transaction()
                result = tx.run("RETURN 1 AS n")
                # closing tx while result ins unconsumed
                if tx_end_ == "commit":
                    tx.commit()
                elif tx_end_ == "close":
                    tx.close()
                else:
                    tx.rollback()
                with self.assertRaises(types.DriverError) as exc:
                    if result_method_ == "peek":
                        result.peek()
                    else:
                        result.next()
                self._assert_result_out_of_scope_exception(exc.exception)
                self._server.done()

        for tx_end in ("commit", "rollback", "close"):
            for result_method in ("next", "peek"):
                with self.subTest(tx_end=tx_end, result_method=result_method):
                    if result_method == "peek":
                        self.skip_if_missing_driver_features(
                            types.Feature.API_RESULT_PEEK
                        )
                    test(tx_end, result_method)
                self._server.reset()

    def test_result_is_invalid_after_consume_tx_run(self):
        def test(result_method_):
            with self._start_session(
                "tx_inf_results_until_end.script"
            ) as session:
                tx = session.begin_transaction()
                result = tx.run("RETURN 1 AS n")
                result.consume()
                result.consume()  # can still consume

                with self.assertRaises(types.DriverError) as exc:
                    if result_method_ == "peek":
                        result.peek()
                    else:
                        result.next()
                tx.commit()  # the tx should still be intact
                self._assert_result_consumed_exception(exc.exception)

        for result_method in ("next", "peek"):
            with self.subTest(result_method=result_method):
                if result_method == "peek":
                    self.skip_if_missing_driver_features(
                        types.Feature.API_RESULT_PEEK
                    )
                test(result_method)
            self._server.reset()

    def test_result_is_invalid_after_consume_session_run(self):
        def test(result_method_):
            with self._start_session(
                "inf_results_until_end.script"
            ) as session:
                result = session.run("RETURN 1 AS n")
                result.consume()
                result.consume()  # can still consume

                with self.assertRaises(types.DriverError) as exc:
                    if result_method_ == "peek":
                        result.peek()
                    else:
                        result.next()
                self._assert_result_consumed_exception(exc.exception)

        for result_method in ("next", "peek"):
            with self.subTest(result_method=result_method):
                if result_method == "peek":
                    self.skip_if_missing_driver_features(
                        types.Feature.API_RESULT_PEEK
                    )
                test(result_method)
            self._server.reset()
