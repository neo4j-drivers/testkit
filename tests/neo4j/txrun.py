import uuid

import nutkit.protocol as types
from tests.neo4j.shared import (
    get_driver,
)
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)


class TestTxRun(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._driver = get_driver(self._backend)
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        self._driver.close()
        super().tearDown()

    def test_updates_last_bookmark_on_commit(self):
        # Verifies that last bookmark is set on the session upon
        # succesful commit.
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run("CREATE (n:SessionNode) RETURN n")
        tx.commit()
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 1)
        self.assertGreater(len(bookmarks[0]), 3)

    def test_does_not_update_last_bookmark_on_rollback(self):
        # Verifies that last bookmark is set on the session upon
        # succesful commit.
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run("CREATE (n:SessionNode) RETURN n")
        tx.rollback()
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 0)

    def test_does_not_update_last_bookmark_on_failure(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN").next()
            tx.commit()
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 0)

    def test_should_be_able_to_rollback_a_failure(self):
        if get_driver_name() in ["go"]:
            self.skipTest('Could not rollback transaction')
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN").next()
        tx.rollback()

    def test_should_not_rollback_a_rollbacked_tx(self):
        if get_driver_name() in ["go"]:
            self.skipTest('Does not raise the exception')
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.rollback()
        with self.assertRaises(types.responses.DriverError):
            tx.rollback()

    def test_should_not_rollback_a_commited_tx(self):
        if get_driver_name() in ["go"]:
            self.skipTest('Does not raise the exception')
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            tx.rollback()

    def test_should_not_commit_a_commited_tx(self):
        if get_driver_name() in ["go"]:
            self.skipTest('Does not raise exception')
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            tx.commit()

    def test_should_not_run_valid_query_in_invalid_tx(self):
        if get_driver_name() in ["python"]:
            self.skipTest("executes the second RUN")
        if get_driver_name() in ["go"]:
            self.skipTest('Could not rollback transaction')

        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        with self.assertRaises(types.responses.DriverError):
            tx.run("NOT CYPHER").consume()

        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN 42").next()

        tx.rollback()

    def test_should_fail_run_in_a_commited_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN 42").next()

    def test_should_fail_run_in_a_rollbacked_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.rollback()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN 42").next()

    def test_should_fail_to_run_query_for_invalid_bookmark(self):
        self._session = self._driver.session("w")
        tx1 = self._session.beginTransaction()
        result = tx1.run('CREATE ()')
        result.consume()
        tx1.commit()
        last_bookmarks = self._session.lastBookmarks()
        assert len(last_bookmarks) == 1
        last_bookmark = last_bookmarks[0]
        invalid_bookmark = last_bookmark[:-1] + "-"
        self._session.close()
        self._session = self._driver.session("w", [invalid_bookmark])

        with self.assertRaises(types.responses.DriverError):
            tx2 = self._session.beginTransaction()
            tx2.run("CREATE ()").consume()

    def test_tx_configuration(self):
        self._session = self._driver.session("r")
        tx = self._session.beginTransaction(txMeta={"foo": "bar"}, timeout=3)
        result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
        values = []
        if self.driver_supports_features(types.Feature.TMP_RESULT_KEYS):
            self.assertEqual(result.keys(), ["x"])
        for record in result:
            values.append(record.values[0])
        tx.commit()
        self.assertEqual(values, list(map(types.CypherInt, range(1, 5))))

    def test_consume_after_commit(self):
        self._session = self._driver.session("w", fetchSize=2)
        tx = self._session.beginTransaction()
        result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
        if self.driver_supports_features(types.Feature.TMP_RESULT_KEYS):
            self.assertEqual(result.keys(), ["x"])
        values = []
        for _ in range(2):
            record = result.next()
            self.assertIsInstance(record, types.Record)
            values.append(record.values[0])
        tx.commit()

        # TODO: what should happen here?
        # options:
        #   - Don't fetch any further records but return what is buffered rn
        #   - throw exception
        #   - Commit should've buffered all records and return them now

    def test_parallel_queries(self):
        def _test():
            self._session = self._driver.session("w", fetchSize=2)
            tx = self._session.beginTransaction()
            result1 = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
            result2 = tx.run("UNWIND [5,6,7,8] AS x RETURN x")
            if self.driver_supports_features(types.Feature.TMP_RESULT_KEYS):
                self.assertEqual(result1.keys(), ["x"])
                self.assertEqual(result2.keys(), ["x"])
            if invert_fetching:
                values2 = list(map(lambda rec: rec.values[0], result2))
                values1 = list(map(lambda rec: rec.values[0], result1))
            else:
                values1 = list(map(lambda rec: rec.values[0], result1))
                values2 = list(map(lambda rec: rec.values[0], result2))
            tx.commit()
            self.assertEqual(values1, list(map(types.CypherInt, (1, 2, 3, 4))))
            self.assertEqual(values2, list(map(types.CypherInt, (5, 6, 7, 8))))
            self._session.close()
            self._session = None

        for invert_fetching in (True, False):
            with self.subTest("inverted" if invert_fetching else "in_order"):
                _test()

    def test_interwoven_queries(self):
        def _test():
            self._session = self._driver.session("w", fetchSize=2)
            tx = self._session.beginTransaction()
            result1 = tx.run("UNWIND [1,2,3,4] AS x RETURN x")

            if run_q2_before_q1_fetch:
                result2 = tx.run("UNWIND [5,6,7,8] AS y RETURN y")

            if self.driver_supports_features(types.Feature.TMP_RESULT_KEYS):
                self.assertEqual(result1.keys(), ["x"])
            values1 = [result1.next().values[0]]

            if not run_q2_before_q1_fetch:
                result2 = tx.run("UNWIND [5,6,7,8] AS y RETURN y")
            if self.driver_supports_features(types.Feature.TMP_RESULT_KEYS):
                self.assertEqual(result2.keys(), ["y"])
            values2 = list(map(lambda rec: rec.values[0], result2))

            self.assertEqual(values2, list(map(types.CypherInt, (5, 6, 7, 8))))
            tx.commit()
            self.assertIsInstance(result2.next(), types.NullRecord)
            self.assertEqual(values1, [types.CypherInt(1)])
            # TODO: what should result1.next() result in?
            # options:
            # result should have discarded all records.
            # self.assertEqual(list(map(lambda rec: rec.values[0], result1)),
            #                  [])

            # result should buffer records on tx closure
            # values1 += list(map(lambda rec: rec.values[0], result1))
            # self.assertEqual(values1, list(map(types.CypherInt,
            #                                    (1, 2, 3, 4))))

            # result should have been consumed and raises an exception
            # with self.assertRaises(types.DriverError) as exc:
            #     result1.next()
            # # TODO: check exc

            self._session.close()
            self._session = None

        for run_q2_before_q1_fetch in (True, False):
            with self.subTest("run_q2_before_q1_fetch-%s"
                              % run_q2_before_q1_fetch):
                _test()

    def test_unconsumed_result(self):
        def _test():
            uuid_ = str(uuid.uuid1())

            self._session = self._driver.session("w")
            tx = self._session.beginTransaction()
            tx.run("CREATE (a:Thing {uuid:$uuid})",
                   params={"uuid": types.CypherString(uuid_)})
            # do not consume the result or do anything with it
            if commit:
                tx.commit()
            else:
                tx.rollback()

            res = self._session.run("MATCH (a:Thing {uuid:$uuid}) RETURN a",
                                    params={"uuid": types.CypherString(uuid_)})
            self.assertEqual(len(list(res)), commit)

        for commit in (True, False):
            with self.subTest("commit" if commit else "rollback"):
                _test()
