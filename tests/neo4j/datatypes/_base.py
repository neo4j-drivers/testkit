from __future__ import annotations

import nutkit.protocol as types
from nutkit.frontend import ApplicationCodeError
from tests.neo4j.shared import (
    get_auto_resolved_db,
    get_driver,
    has_tx_support,
    requires_tx_support,
    with_retries,
)
from tests.shared import TestkitTestCase


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
        self._session = self._driver.session(
            "w", database=get_auto_resolved_db()
        )

    def _verify_can_echo(self, val):
        def work(runner):
            result = runner.run("RETURN $x AS y", params={"x": val})
            record_ = result.next()
            assert isinstance(result.next(), types.NullRecord)
            return record_

        if has_tx_support(self):
            record = self._session.execute_read(work)
        else:
            record = with_retries(work, self._session)
        self.assertEqual(record, types.Record(values=[val]))

    def _send_value_as_param(self, val):
        def work(runner):
            result = runner.run("RETURN 1 AS n", params={"x": val})
            result.consume()

        if has_tx_support(self):
            self._session.execute_read(work)
        else:
            with_retries(work, self._session)

    def _read_query_values(self, query, params=None):
        def work(runner):
            result = runner.run(query, params=params)
            record_ = result.next()
            assert isinstance(result.next(), types.NullRecord)
            return record_.values

        if has_tx_support(self):
            return self._session.execute_read(work)
        else:
            return with_retries(work, self._session)

    @requires_tx_support
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
