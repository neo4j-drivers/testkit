import nutkit.protocol as types
from nutkit.protocol.error_type import ErrorType
from tests.shared import get_driver_name

from ._common import IterationTestBase


class TestResultList(IterationTestBase):

    required_features = (types.Feature.BOLT_4_4,
                         types.Feature.API_RESULT_LIST)

    def _assert_connection_error(self, error):
        self.assertIsInstance(error, types.DriverError)
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual("<class 'neo4j.exceptions.ServiceUnavailable'>",
                             error.errorType)
        elif driver in ["java"]:
            self.assertEqual(
                ErrorType.SERVICE_UNAVAILABLE_ERROR.value,
                error.errorType
            )
        elif driver in ["javascript"]:
            self.assertEqual(
                "ServiceUnavailable",
                error.code
            )
        elif driver in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::ServiceUnavailableException",
                error.errorType
            )
        elif driver in ["dotnet"]:
            self.assertEqual("ServiceUnavailableError", error.errorType)
        elif driver in ["go"]:
            self.assertEqual("ConnectivityError", error.errorType)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def test_result_list_with_0_records(self):
        with self._session("yield_0_records.script") as session:
            result = session.run("RETURN 1 AS n")
            records = result.list()
            self.assertEqual(records, [])

    def test_result_list_with_1_records(self):
        with self._session("yield_1_record.script") as session:
            result = session.run("RETURN 1 AS n")
            records = result.list()
            self.assertEqual(records, [
                types.Record(values=[types.CypherInt(1)])
            ])

    def test_result_list_with_2_records(self):
        def _test():
            with self._session("yield_2_records.script",
                               fetch_size=fetch_size) as session:
                result = session.run("RETURN 1 AS n")
                records = result.list()
                self.assertEqual(records, [
                    types.Record(values=[types.CypherInt(i)])
                    for i in range(1, 3)
                ])

        for fetch_size in (1, 2):
            with self.subTest("fetch_size-%i" % fetch_size):
                _test()

    def test_result_list_with_disconnect(self):
        with self._session("disconnect_on_pull.script") as session:
            result = session.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.list()
            self._assert_connection_error(exc.exception)

    def test_result_list_with_failure(self):
        err = "Neo.TransientError.Completely.MadeUp"

        with self._session("error_on_pull.script",
                           vars_={"#ERROR#": err}) as session:
            result = session.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.list()
            self.assertEqual(err, exc.exception.code)

    def test_result_list_with_failure_tx_run(self):
        err = "Neo.TransientError.Completely.MadeUp"

        with self._session("tx_error_on_pull.script",
                           vars_={"#ERROR#": err}) as session:
            tx = session.begin_transaction()
            result = tx.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.list()
            self.assertEqual(err, exc.exception.code)

    def test_result_list_with_failure_tx_func_run(self):
        err = "Neo.TransientError.Completely.MadeUp"
        work_call_count = 0

        def work(tx):
            nonlocal work_call_count
            work_call_count += 1
            result = tx.run("RETURN 1 AS n")
            if work_call_count == 1:
                with self.assertRaises(types.DriverError) as exc:
                    result.list()
                self.assertEqual(err, exc.exception.code)
                raise exc.exception
            else:
                return result.list()

        with self._session("tx_error_on_pull.script",
                           vars_={"#ERROR#": err}) as session:
            records = session.read_transaction(work)
            self.assertEqual(records, [
                types.Record(values=[types.CypherInt(1)])
            ])

    def _result_list_script(self, transaction=False, next_first=False):
        optimized = self.driver_supports_features(
            types.Feature.OPT_RESULT_LIST_FETCH_ALL
        )
        if not optimized:
            # There is no difference on the server side.
            # Regardless of the mixture of `next` and `list` calls, the driver
            # will always pull batch by batch following the configured
            # `fetch_size`.
            next_first = False
        script = "%spull_%s%slist.script"
        return script % (
            "tx_" if transaction else "",
            "2_then_" if next_first else "",
            "optimized_" if optimized else ""
        )

    def test_session_run_result_list_pulls_all_records_at_once(self):
        self._test_session_run_result_list_pulls_all_records_at_once(next_first=False)  # noqa: E501

    def test_session_run_result_list_pulls_all_records_at_once_next_before_list(self):  # noqa: E501
        self._test_session_run_result_list_pulls_all_records_at_once(next_first=True)  # noqa: E501

    def _test_session_run_result_list_pulls_all_records_at_once(self, next_first):  # noqa: E501
        script = self._result_list_script(next_first=next_first)

        with self._session(script, fetch_size=2) as session:
            result = session.run("RETURN 1 AS n")
            i_start = 1
            if next_first:
                record = result.next()
                self.assertEqual(
                    record,
                    types.Record(values=[types.CypherInt(i_start)])
                )
                i_start += 1
            records = result.list()
            self.assertEqual(records, [
                types.Record(values=[types.CypherInt(i)])
                for i in range(i_start, 6)
            ])

    def test_tx_run_result_list_pulls_all_records_at_once(self):
        self._test_tx_run_result_list_pulls_all_records_at_once(next_first=False)  # noqa: E501

    def test_tx_run_result_list_pulls_all_records_at_once_next_before_list(self):  # noqa: E501
        self._test_tx_run_result_list_pulls_all_records_at_once(next_first=True)  # noqa: E501

    def _test_tx_run_result_list_pulls_all_records_at_once(self, next_first):
        script = self._result_list_script(transaction=True,
                                          next_first=next_first)

        with self._session(script, fetch_size=2) as session:
            tx = session.begin_transaction()
            result = tx.run("RETURN 1 AS n")
            i_start = 1
            if next_first:
                record = result.next()
                self.assertEqual(
                    record,
                    types.Record(values=[types.CypherInt(i_start)])
                )
                i_start += 1
            records = result.list()
            self.assertEqual(records, [
                types.Record(values=[types.CypherInt(i)])
                for i in range(i_start, 6)
            ])
            tx.commit()

    def test_tx_func_result_list_pulls_all_records_at_once(self):
        self._test_tx_func_result_list_pulls_all_records_at_once(next_first=False)  # noqa: E501

    def test_tx_func_result_list_pulls_all_records_at_once_next_before_list(self):  # noqa: E501
        self._test_tx_func_result_list_pulls_all_records_at_once(next_first=True)  # noqa: E501

    def _test_tx_func_result_list_pulls_all_records_at_once(self, next_first):
        def work(tx):
            result = tx.run("RETURN 1 AS n")
            i_start = 1
            if next_first:
                record = result.next()
                self.assertEqual(
                    record,
                    types.Record(values=[types.CypherInt(i_start)])
                )
                i_start += 1
            records = result.list()
            self.assertEqual(records, [
                types.Record(values=[types.CypherInt(i)])
                for i in range(i_start, 6)
            ])

        script = self._result_list_script(transaction=True,
                                          next_first=next_first)

        with self._session(script, fetch_size=2) as session:
            session.read_transaction(work)
