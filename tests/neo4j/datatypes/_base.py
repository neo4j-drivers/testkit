from nutkit.frontend import ApplicationCodeError
import nutkit.protocol as types
from tests.neo4j.shared import get_driver
from tests.shared import TestkitTestCase

MIN_INT64 = -(2 ** 63)
MAX_INT64 = (2 ** 63) - 1


class _TestTypesBase(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._session = None
        self._driver = None

    def tearDown(self):
        if self._session:
            self._session.close()
        if self._driver:
            self._driver.close()
        super().tearDown()

    def _create_driver_and_session(self):
        if self._session is not None:
            self._session.close()
        if self._driver is not None:
            self._driver.close()
        self._driver = get_driver(self._backend)
        self._session = self._driver.session("w")

    def _verify_can_echo(self, val):
        def work(tx):
            result = tx.run("RETURN $x as y", params={"x": val})
            record_ = result.next()
            assert isinstance(result.next(), types.NullRecord)
            return record_

        record = self._session.execute_read(work)
        self.assertEqual(record, types.Record(values=[val]))

    def _read_query_values(self, query, params=None):
        def work(tx):
            result = tx.run(query, params=params)
            record_ = result.next()
            assert isinstance(result.next(), types.NullRecord)
            return record_.values

        return self._session.execute_read(work)

    def _write_query_values(self, query, params=None):
        values = []

        def work(tx):
            nonlocal values
            result = tx.run(query, params=params)
            record_ = result.next()
            assert isinstance(result.next(), types.NullRecord)
            values = record_.values
            # rollback
            raise ApplicationCodeError

        with self.assertRaises(types.FrontendError):
            self._session.execute_write(work)
        return values
