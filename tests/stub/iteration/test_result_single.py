import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    get_driver_name,
)

from ._common import IterationTestBase


class TestResultSingle(IterationTestBase):

    required_features = types.Feature.BOLT_4_0,

    def _assert_not_exactly_one_record_error(self, error):
        self.assertIsInstance(error, types.DriverError)
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual("<class 'ToBeDecided'>",
                             error.errorType)
        elif driver in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::NoSuchRecordException",
                error.errorType)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def _assert_connection_error(self, error):
        self.assertIsInstance(error, types.DriverError)
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual("<class 'neo4j.exceptions.ServiceUnavailable'>",
                             error.errorType)
        elif driver in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::ServiceUnavailableException",
                error.errorType)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    @driver_feature(types.Feature.API_RESULT_SINGLE)
    def test_result_single_with_0_records(self):
        with self._session("yield_0_records.script") as session:
            result = session.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.single()
            self._assert_not_exactly_one_record_error(exc.exception)

    @driver_feature(types.Feature.API_RESULT_SINGLE)
    def test_result_single_with_1_records(self):
        with self._session("yield_1_record.script") as session:
            result = session.run("RETURN 1 AS n")
            record = result.single()
            self.assertIsInstance(record, types.Record)
            self.assertEqual(record.values, [types.CypherInt(1)])

    @driver_feature(types.Feature.API_RESULT_SINGLE)
    def test_result_single_with_2_records(self):
        def _test():
            with self._session("yield_2_records.script",
                               fetch_size=fetch_size) as session:
                result = session.run("RETURN 1 AS n")
                with self.assertRaises(types.DriverError) as exc:
                    result.single()
                self._assert_not_exactly_one_record_error(exc.exception)

        for fetch_size in (1, 2):
            with self.subTest("fetch_size-%i" % fetch_size):
                _test()

    @driver_feature(types.Feature.API_RESULT_SINGLE)
    def test_result_single_with_disconnect(self):
        with self._session("disconnect_on_pull.script") as session:
            result = session.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.single()
            self._assert_connection_error(exc.exception)

    @driver_feature(types.Feature.API_RESULT_SINGLE)
    def test_result_single_with_failure(self):
        err = "Neo.TransientError.Completely.MadeUp"

        with self._session("error_on_pull.script",
                           vars_={"#ERROR#": err}) as session:
            result = session.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.single()
            self.assertEqual(err, exc.exception.code)

    @driver_feature(types.Feature.API_RESULT_SINGLE)
    def test_result_single_with_failure_tx_run(self):
        err = "Neo.TransientError.Completely.MadeUp"

        with self._session("tx_error_on_pull.script",
                           vars_={"#ERROR#": err}) as session:
            tx = session.begin_transaction()
            result = tx.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.single()
            self.assertEqual(err, exc.exception.code)

    @driver_feature(types.Feature.API_RESULT_SINGLE)
    def test_result_single_with_failure_tx_func_run(self):
        err = "Neo.TransientError.Completely.MadeUp"
        work_call_count = 0

        def work(tx):
            nonlocal work_call_count
            work_call_count += 1
            result = tx.run("RETURN 1 AS n")
            if work_call_count == 1:
                with self.assertRaises(types.DriverError) as exc:
                    result.single()
                self.assertEqual(err, exc.exception.code)
                raise exc.exception
            else:
                return result.single()

        with self._session("tx_error_on_pull.script",
                           vars_={"#ERROR#": err}) as session:
            record = session.read_transaction(work)
            self.assertIsInstance(record, types.Record)
            self.assertEqual(record.values, [types.CypherInt(1)])
