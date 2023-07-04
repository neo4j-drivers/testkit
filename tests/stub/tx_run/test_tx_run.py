from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestTxRun(TestkitTestCase):

    required_features = types.Feature.BOLT_4_4,

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._server1 = StubServer(9010)
        self._server2 = StubServer(9011)
        self._session = None
        self._driver = None

    def tearDown(self):
        if self._session is not None:
            self._session.close()
        if self._driver is not None:
            self._driver.close()
        self._router.reset()
        self._server1.reset()
        self._server2.reset()
        super().tearDown()

    def _create_direct_driver(self):
        uri = "bolt://%s" % self._server1.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic", principal="",
                                                       credentials=""))

    def _create_routing_driver(self):
        uri = "neo4j://%s" % self._router.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic", principal="",
                                                       credentials=""))

    def test_rollback_tx_on_session_close_untouched_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Driver requires result.next() to send PULL")
        self._create_direct_driver()
        self._server1.start(
            path=self.script_path("tx_discard_then_rollback.script")
        )
        self._session = self._driver.session("r", fetch_size=2)
        tx = self._session.begin_transaction()
        tx.run("RETURN 1 AS n")
        # closing session while tx is open and result is not consumed at all
        self._session.close()
        self._session = None
        self._server1.done()

    def test_rollback_tx_on_session_close_unfinished_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Sends RESET instead of ROLLBACK.")
        self._server1.start(
            path=self.script_path("tx_discard_then_rollback.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("r", fetch_size=2)
        tx = self._session.begin_transaction()
        result = tx.run("RETURN 1 AS n")
        result.next()
        # closing session while tx is open and result is not fully consumed
        self._session.close()
        self._session = None
        self._server1.done()

    def test_rollback_tx_on_session_close_consumed_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Driver sends RESET instead of ROLLBACK")
        self._server1.start(
            path=self.script_path("tx_discard_then_rollback.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("r", fetch_size=2)
        tx = self._session.begin_transaction()
        result = tx.run("RETURN 1 AS n")
        result.consume()
        # closing session while tx is open and result has been manually
        # consumed
        self._session.close()
        self._session = None
        self._server1.done()

    def test_rollback_tx_on_session_close_finished_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Driver sends RESET instead of ROLLBACK")
        self._create_direct_driver()
        self._server1.start(
            path=self.script_path("tx_pull_then_rollback.script")
        )
        self._session = self._driver.session("r", fetch_size=2)
        tx = self._session.begin_transaction()
        result = tx.run("RETURN 1 AS n")
        list(result)  # pull all results
        # closing session while tx is open
        self._session.close()
        self._session = None
        self._server1.done()

    def _eager_tx_func_run(self, script, routing=False):
        if routing:
            self._create_routing_driver()
            self._router.start(
                path=self.script_path("router_switch_server.script"),
                vars_={"#HOST#": self._server1.host}
            )
            self._server2.start(path=self.script_path("tx_commit.script"))
        else:
            self._create_direct_driver()
        self._server1.start(path=self.script_path(script))

        tx_func_count = 0

        def work(tx):
            nonlocal tx_func_count
            tx_func_count += 1
            list(tx.run("RETURN 1 AS n"))

        self._session = self._driver.session("w")
        exc = None
        try:
            self._session.execute_write(work)
        except types.DriverError as e:
            exc = e

        self._session.close()
        self._session = None

        return exc, tx_func_count

    @driver_feature(types.Feature.OPT_EAGER_TX_BEGIN)
    def test_eager_begin_on_tx_func_run_with_disconnect_on_begin(self):
        exc, tx_func_count = self._eager_tx_func_run(
            "tx_disconnect_on_begin.script", routing=True
        )
        # Driver should retry tx on disconnect after BEGIN and call the tx func
        # exactly once (after the disconnect). The disconnect should make the
        # driver fetch a new routing table which will point to server2 the
        # second time. This server will let the tx succeed.
        self.assertIsNone(exc)
        self.assertEqual(tx_func_count, 1)
        self._router.done()
        self.assertEqual(self._router.count_requests("ROUTE"), 2)
        self._server1.done()
        self._server2.done()

    @driver_feature(types.Feature.OPT_EAGER_TX_BEGIN)
    def test_eager_begin_on_tx_func_run_with_error_on_begin(self):
        exc, tx_func_count = self._eager_tx_func_run(
            "tx_error_on_begin.script", routing=False
        )
        # Driver should raise error on non-transient error after BEGIN, and
        # never call the tx func.
        self.assertEqual("Neo.ClientError.MadeUp.Code", exc.code)
        self.assertEqual(tx_func_count, 0)
        self._server1.done()

    def _eager_tx_run(self, script):
        self._create_direct_driver()
        self._server1.start(path=self.script_path(script))

        self._session = self._driver.session("w")
        with self.assertRaises(types.DriverError) as exc:
            self._session.begin_transaction()

        self._session.close()
        self._session = None

        return exc.exception

    @driver_feature(types.Feature.OPT_EAGER_TX_BEGIN)
    def test_eager_begin_on_tx_run_with_disconnect_on_begin(self):
        exc = self._eager_tx_run("tx_disconnect_on_begin.script")
        if get_driver_name() in ["python"]:
            self.assertEqual("<class 'neo4j.exceptions.ServiceUnavailable'>",
                             exc.errorType)

    @driver_feature(types.Feature.OPT_EAGER_TX_BEGIN)
    def test_eager_begin_on_tx_run_with_error_on_begin(self):
        exc = self._eager_tx_run("tx_error_on_begin.script")
        self.assertEqual("Neo.ClientError.MadeUp.Code", exc.code)

    def test_raises_error_on_tx_run(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript", "dotnet"]:
            self.skipTest("Driver reports error too late.")
        self._server1.start(
            path=self.script_path("tx_error_on_run.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("r")
        tx = self._session.begin_transaction()
        with self.assertRaises(types.DriverError) as exc:
            tx.run("RETURN 1 AS n")
        self.assertEqual(exc.exception.code, "Neo.ClientError.MadeUp.Code")
        tx.rollback()

    def test_raises_error_on_tx_func_run(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript", "dotnet"]:
            self.skipTest("Driver reports error too late.")
        work_call_count = 0

        def work(tx):
            nonlocal work_call_count
            self.assertEqual(work_call_count, 0)
            work_call_count += 1

            with self.assertRaises(types.DriverError) as exc_:
                tx.run("RETURN 1 AS n")
            self.assertEqual(exc_.exception.code,
                             "Neo.ClientError.MadeUp.Code")
            raise exc_.exception

        if get_driver_name() in ["javascript_", "dotnet_"]:
            self.skipTest("Driver reports error too late.")
        self._server1.start(
            path=self.script_path("tx_error_on_run.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("r")
        with self.assertRaises(types.DriverError) as exc:
            self._session.execute_read(work)
        self.assertEqual(exc.exception.code, "Neo.ClientError.MadeUp.Code")

    def _test_failed_tx_run(self, rollback):
        self._server1.start(
            path=self.script_path("tx_error_on_run.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("r")
        tx = self._session.begin_transaction()
        with self.assertRaises(types.DriverError) as exc:
            tx.run("RETURN 1 AS n").consume()
        self.assertEqual(exc.exception.code, "Neo.ClientError.MadeUp.Code")
        if rollback:
            tx.rollback()
        self._session.close()
        self._session = None
        self._driver.close()
        self._driver = None

    def test_failed_tx_run_allows_rollback(self):
        self._test_failed_tx_run(rollback=True)

    def test_failed_tx_run_allows_skipping_rollback(self):
        self._test_failed_tx_run(rollback=False)

    def test_should_prevent_pull_after_tx_termination_on_run(self):
        def _test():
            self._create_direct_driver()
            script = "tx_res0_success_res1_error_on_run.script"
            self._server1.start(path=self.script_path(script))
            self._session = self._driver.session("r", fetch_size=2)
            tx = self._session.begin_transaction()
            res = tx.run("RETURN 1 AS n")

            # initiate another stream that fails on RUN
            with self.assertRaises(types.DriverError) as exc:
                failed_res = tx.run("invalid")
            self.assertEqual(exc.exception.code,
                             "Neo.ClientError.Statement.SyntaxError")
            self._assert_is_client_exception(exc)

            # there must be no further PULL and an exception must be raised
            with self.assertRaises(types.DriverError) as exc:
                if iterate == "true":
                    for _i in range(0, 3):
                        res.next()
                else:
                    fetch_all = types.Feature.OPT_RESULT_LIST_FETCH_ALL
                    if self.driver_supports_features(fetch_all):
                        res.list()
                    else:
                        # only explicit iteration is tested if fetch all is
                        # not supported
                        list(res)
            # the streaming result surfaces the termination exception
            self.assertEqual(exc.exception.code,
                             "Neo.ClientError.Statement.SyntaxError")
            self._assert_is_client_exception(exc)

            tx.close()
            self._session.close()
            self._session = None
            self._server1.done()

        for iterate in ["true", "false"]:
            with self.subTest(iterate=iterate):
                _test()
            self._server1.reset()

    def test_should_prevent_discard_after_tx_termination_on_run(self):
        self._create_direct_driver()
        script = "tx_res0_success_res1_error_on_run.script"
        self._server1.start(path=self.script_path(script))
        self._session = self._driver.session("r", fetch_size=2)
        tx = self._session.begin_transaction()
        res = tx.run("RETURN 1 AS n")

        # initiate another stream that fails on RUN
        with self.assertRaises(types.DriverError) as exc:
            failed_res = tx.run("invalid")
        self.assertEqual(exc.exception.code,
                         "Neo.ClientError.Statement.SyntaxError")
        self._assert_is_client_exception(exc)

        with self.assertRaises(types.DriverError) as exc:
            res.consume()
        # the streaming result surfaces the termination exception
        self.assertEqual(exc.exception.code,
                         "Neo.ClientError.Statement.SyntaxError")
        self._assert_is_client_exception(exc)

        tx.close()
        self._session.close()
        self._session = None
        self._server1.done()

    def test_should_prevent_run_after_tx_termination_on_run(self):
        self._create_direct_driver()
        script = "tx_error_on_run.script"
        self._server1.start(path=self.script_path(script))
        self._session = self._driver.session("r")
        tx = self._session.begin_transaction()
        with self.assertRaises(types.DriverError) as exc:
            tx.run("invalid")
        self.assertEqual(exc.exception.code,
                         "Neo.ClientError.MadeUp.Code")
        self._assert_is_client_exception(exc)

        with self.assertRaises(types.DriverError) as exc:
            tx.run("invalid")
        # new actions on the transaction result in a tx terminated
        # exception, a subclass of the client exception
        self._assert_is_tx_terminated_exception(exc)

        tx.close()
        self._session.close()
        self._session = None
        self._server1.done()

    def test_should_prevent_run_after_tx_termination_on_pull(self):
        def _test():
            self._create_direct_driver()
            script = "tx_error_on_pull.script"
            self._server1.start(path=self.script_path(script))
            self._session = self._driver.session("r", fetch_size=2)
            tx = self._session.begin_transaction()
            res = tx.run("failing on pull")

            # res fails on PULL
            with self.assertRaises(types.DriverError) as exc:
                if iterate == "true":
                    for _i in range(0, 3):
                        res.next()
                else:
                    fetch_all = types.Feature.OPT_RESULT_LIST_FETCH_ALL
                    if self.driver_supports_features(fetch_all):
                        res.list()
                    else:
                        # only explicit iteration is tested if fetch all is
                        # not supported
                        list(res)
            self.assertEqual(exc.exception.code,
                             "Neo.ClientError.MadeUp.Code")
            self._assert_is_client_exception(exc)

            with self.assertRaises(types.DriverError) as exc:
                tx.run("invalid")
            # new actions on the transaction result in a tx terminated
            # exception, a subclass of the client exception
            self._assert_is_tx_terminated_exception(exc)

            tx.close()
            self._session.close()
            self._session = None
            self._server1.done()

        for iterate in ["true", "false"]:
            with self.subTest(iterate=iterate):
                _test()
            self._server1.reset()

    def test_should_prevent_pull_after_tx_termination_on_pull(self):
        def _test():
            self._create_direct_driver()
            script = "tx_res0_success_res1_error_on_pull.script"
            self._server1.start(path=self.script_path(script))
            self._session = self._driver.session("r", fetch_size=2)
            tx = self._session.begin_transaction()
            res = tx.run("RETURN 1 AS n")

            # initiate another stream that fails on PULL
            with self.assertRaises(types.DriverError) as exc:
                failed_res = tx.run("failing on pull")
                failed_res.next()
            self.assertEqual(exc.exception.code,
                             "Neo.ClientError.MadeUp.Code")
            self._assert_is_client_exception(exc)

            # there must be no further PULL and an exception must be raised
            with self.assertRaises(types.DriverError):
                if iterate == "true":
                    for _i in range(0, 3):
                        res.next()
                else:
                    fetch_all = types.Feature.OPT_RESULT_LIST_FETCH_ALL
                    if self.driver_supports_features(fetch_all):
                        res.list()
                    else:
                        # only explicit iteration is tested if fetch all is
                        # not supported
                        list(res)
            # the streaming result surfaces the termination exception
            self.assertEqual(exc.exception.code,
                             "Neo.ClientError.MadeUp.Code")
            self._assert_is_client_exception(exc)

            tx.close()
            self._session.close()
            self._session = None
            self._server1.done()

        for iterate in ["true", "false"]:
            with self.subTest(iterate=iterate):
                _test()
            self._server1.reset()

    def _assert_is_client_exception(self, e):
        driver = get_driver_name()
        if driver in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.ClientException",
                e.exception.errorType
            )
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def _assert_is_tx_terminated_exception(self, e):
        driver = get_driver_name()
        if driver in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.TransactionTerminatedException",
                e.exception.errorType
            )
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
