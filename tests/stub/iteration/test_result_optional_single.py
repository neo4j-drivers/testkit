import nutkit.protocol as types
from tests.shared import get_driver_name

from ._common import IterationTestBase


class TestResultSingleOptional(IterationTestBase):

    required_features = (types.Feature.BOLT_4_4,
                         types.Feature.API_RESULT_SINGLE_OPTIONAL)

    def _assert_not_exactly_one_record_warning(self, warnings):
        self.assertEqual(1, len(warnings))
        warning = warnings[0]
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertIn("multiple", warning)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def _assert_connection_error(self, error):
        self.assertIsInstance(error, types.DriverError)
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual("<class 'neo4j.exceptions.ServiceUnavailable'>",
                             error.errorType)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def test_result_single_optional_with_0_records(self):
        with self._session("yield_0_records.script") as session:
            result = session.run("RETURN 1 AS n")
            optional_record = result.single_optional()
            self.assertIsNone(optional_record.record)
            self.assertEqual(optional_record.warnings, [])

    def test_result_single_optional_with_1_records(self):
        with self._session("yield_1_record.script") as session:
            result = session.run("RETURN 1 AS n")
            optional_record = result.single_optional()
            record, warnings = optional_record.record, optional_record.warnings
            self.assertIsInstance(record, types.Record)
            self.assertEqual(record.values, [types.CypherInt(1)])
            self.assertEqual(warnings, [])

    def test_result_single_optional_with_2_records(self):
        def _test():
            with self._session("yield_2_records.script",
                               fetch_size=fetch_size) as session:
                result = session.run("RETURN 1 AS n")

                optional_record = result.single_optional()
                record = optional_record.record
                warnings = optional_record.warnings
                self.assertIsInstance(record, types.Record)
                self.assertEqual(record.values, [types.CypherInt(1)])
                self._assert_not_exactly_one_record_warning(warnings)

                # single_optional(), should always exhaust the full result
                # stream to prevent abusing it as another `next` method.
                for _ in range(2):
                    optional_record = result.single_optional()
                    self.assertIsNone(optional_record.record)
                    self.assertEqual(optional_record.warnings, [])

        for fetch_size in (1, 2):
            with self.subTest(fetch_size=fetch_size):
                _test()

    def test_result_single_optional_with_disconnect(self):
        with self._session("disconnect_on_pull.script") as session:
            result = session.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.single_optional()
            self._assert_connection_error(exc.exception)

    def test_result_single_optional_with_failure(self):
        err = "Neo.TransientError.Completely.MadeUp"

        with self._session("error_on_pull.script",
                           vars_={"#ERROR#": err}) as session:
            result = session.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.single_optional()
            self.assertEqual(err, exc.exception.code)

    def test_result_single_optional_with_failure_tx_run(self):
        err = "Neo.TransientError.Completely.MadeUp"

        with self._session("tx_error_on_pull.script",
                           vars_={"#ERROR#": err}) as session:
            tx = session.begin_transaction()
            result = tx.run("RETURN 1 AS n")
            with self.assertRaises(types.DriverError) as exc:
                result.single_optional()
            self.assertEqual(err, exc.exception.code)

    def test_result_single_optional_with_failure_tx_func_run(self):
        err = "Neo.TransientError.Completely.MadeUp"
        work_call_count = 0

        def work(tx):
            nonlocal work_call_count
            work_call_count += 1
            result = tx.run("RETURN 1 AS n")
            if work_call_count == 1:
                with self.assertRaises(types.DriverError) as exc:
                    result.single_optional()
                self.assertEqual(err, exc.exception.code)
                raise exc.exception
            else:
                return result.single_optional()

        with self._session("tx_error_on_pull.script",
                           vars_={"#ERROR#": err}) as session:
            optional_record = session.execute_read(work)
            record, warnings = optional_record.record, optional_record.warnings
            self.assertIsInstance(record, types.Record)
            self.assertEqual(record.values, [types.CypherInt(1)])
            self.assertEqual(warnings, [])
