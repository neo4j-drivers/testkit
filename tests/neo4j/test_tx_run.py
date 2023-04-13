import uuid

import nutkit.protocol as types
from tests.neo4j.shared import (
    cluster_unsafe_test,
    get_driver,
    get_server_info,
)
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)


class TestTxRun(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._driver = get_driver(self._backend)
        self._session1 = None
        self._session2 = None

    def tearDown(self):
        for session in (self._session1, self._session2):
            if session:
                session.close()
        self._driver.close()
        super().tearDown()

    @cluster_unsafe_test
    def test_simple_query(self):
        def _test():
            self._driver.close()
            self._driver = get_driver(self._backend, user_agent="test")
            self._session1 = self._driver.session("r", fetch_size=2)
            tx = self._session1.begin_transaction()
            result = tx.run("UNWIND [1, 2, 3, 4] AS x RETURN x")
            if consume:
                summary = result.consume()
                self.assertIsInstance(summary, types.Summary)
            else:
                self.assertEqual(list(result), [
                    types.Record([types.CypherInt(i)]) for i in range(1, 5)
                ])
            if rollback:
                tx.rollback()
            else:
                tx.commit()
            self._session1.close()
            self._session1 = None

        for consume in (True, False):
            for rollback in (True, False):
                with self.subTest(consume=consume, rollback=rollback):
                    _test()

    @cluster_unsafe_test
    def test_can_commit_transaction(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest(
                'Returns CypherNull as foo\'s value instead of "bar"'
            )
        self._session1 = self._driver.session("w")

        tx = self._session1.begin_transaction()

        # Create a node
        result = tx.run("CREATE (a) RETURN id(a)")
        record = result.next()
        self.assertIsInstance(record, types.Record)
        self.assertEqual(len(record.values), 1)

        node_id = record.values[0]
        self.assertIsInstance(node_id, types.CypherInt)

        # Update a property
        tx.run("MATCH (a) WHERE id(a) = $n SET a.foo = $foo",
               params={"n": node_id, "foo": types.CypherString("bar")})

        tx.commit()

        # Check the property value
        result = self._session1.run("MATCH (a) WHERE id(a) = $n RETURN a.foo",
                                    params={"n": node_id})
        record = result.next()
        self.assertIsInstance(record, types.Record)
        self.assertEqual(len(record.values), 1)
        self.assertEqual(record.values[0], types.CypherString("bar"))

    @cluster_unsafe_test
    def test_can_rollback_transaction(self):
        self._session1 = self._driver.session("w")

        tx = self._session1.begin_transaction()

        # Create a node
        result = tx.run("CREATE (a) RETURN id(a)")
        record = result.next()
        self.assertIsInstance(record, types.Record)
        self.assertEqual(len(record.values), 1)

        node_id = record.values[0]
        self.assertIsInstance(node_id, types.CypherInt)

        # Update a property
        tx.run("MATCH (a) WHERE id(a) = $n SET a.foo = $foo",
               params={"n": node_id, "foo": types.CypherString("bar")})

        tx.rollback()

        # Check the property value
        result = self._session1.run("MATCH (a) WHERE id(a) = $n RETURN a.foo",
                                    params={"n": node_id})
        record = result.next()
        self.assertIsInstance(record, types.NullRecord)

    @cluster_unsafe_test
    def test_updates_last_bookmark_on_commit(self):
        # Verifies that last bookmark is set on the session upon
        # successful commit.
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        tx.run("CREATE (n:SessionNode) RETURN n")
        tx.commit()
        bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(bookmarks), 1)
        self.assertGreater(len(bookmarks[0]), 3)

    @cluster_unsafe_test
    def test_does_not_update_last_bookmark_on_rollback(self):
        # Verifies that last bookmark is set on the session upon
        # succesful commit.
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        tx.run("CREATE (n:SessionNode) RETURN n")
        tx.rollback()
        bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(bookmarks), 0)

    @cluster_unsafe_test
    def test_does_not_update_last_bookmark_on_failure(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Raises syntax error on session.close in tearDown.")
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        with self.assertRaises(types.responses.DriverError):
            result = tx.run("RETURN")
            # TODO: remove this block once all languages work
            if get_driver_name() in ["javascript"]:
                result.next()
        bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(bookmarks), 0)

    @cluster_unsafe_test
    def test_should_be_able_to_rollback_a_failure(self):
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN").next()
        tx.rollback()

    @cluster_unsafe_test
    def test_should_not_commit_a_failure(self):
        self._session1 = self._driver.session("r")
        tx = self._session1.begin_transaction()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN").next()
        with self.assertRaises(types.responses.DriverError):
            tx.commit()

    @cluster_unsafe_test
    def test_should_not_rollback_a_rollbacked_tx(self):
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        tx.run("CREATE (:TXNode1)").consume()
        tx.rollback()
        with self.assertRaises(types.responses.DriverError):
            tx.rollback()

    @cluster_unsafe_test
    def test_should_not_rollback_a_commited_tx(self):
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        tx.run("CREATE (:TXNode1)").consume()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            tx.rollback()

    @cluster_unsafe_test
    def test_should_not_commit_a_commited_tx(self):
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        tx.run("CREATE (:TXNode1)").consume()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            tx.commit()

    @cluster_unsafe_test
    def test_should_not_allow_run_on_a_commited_tx(self):
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        tx.run("CREATE (:TXNode1)").consume()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            result = tx.run("RETURN 1")
            # TODO: remove this block once all languages work
            if get_driver_name() in ["javascript"]:
                result.next()

    @cluster_unsafe_test
    def test_should_not_allow_run_on_a_rollbacked_tx(self):
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        tx.run("CREATE (:TXNode1)").consume()
        tx.rollback()
        with self.assertRaises(types.responses.DriverError):
            result = tx.run("RETURN 1")
            # TODO: remove this block once all languages work
            if get_driver_name() in ["javascript"]:
                result.next()

    @cluster_unsafe_test
    def test_should_not_run_valid_query_in_invalid_tx(self):
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        with self.assertRaises(types.responses.DriverError):
            tx.run("NOT CYPHER").consume()

        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN 42").next()

    @cluster_unsafe_test
    def test_should_fail_run_in_a_commited_tx(self):
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN 42").next()

    @cluster_unsafe_test
    def test_should_fail_run_in_a_rollbacked_tx(self):
        self._session1 = self._driver.session("w")
        tx = self._session1.begin_transaction()
        tx.rollback()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN 42").next()

    @cluster_unsafe_test
    def test_should_fail_to_run_query_for_invalid_bookmark(self):
        self._session1 = self._driver.session("w")
        tx1 = self._session1.begin_transaction()
        result = tx1.run("CREATE ()")
        result.consume()
        tx1.commit()
        last_bookmarks = self._session1.last_bookmarks()
        assert len(last_bookmarks) == 1
        last_bookmark = last_bookmarks[0]
        invalid_bookmark = last_bookmark[:-1] + "-"
        self._session1.close()
        self._session1 = self._driver.session("w", [invalid_bookmark])

        with self.assertRaises(types.responses.DriverError):
            tx2 = self._session1.begin_transaction()
            tx2.run("CREATE ()").consume()

    @cluster_unsafe_test
    def test_broken_transaction_should_not_break_session(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Raises syntax error on session.close in tearDown.")
        self._session1 = self._driver.session("r")
        tx = self._session1.begin_transaction()
        with self.assertRaises(types.DriverError):
            result = tx.run("NOT CYPHER")
            # TODO: remove this block once all languages work
            if get_driver_name() in ["javascript"]:
                result.next()
        # TODO: remove this block once all languages work
        if get_driver_name() in ["java", "ruby"]:
            # requires explicit rollback on a failed transaction
            tx.rollback()
        tx = self._session1.begin_transaction()
        tx.run("RETURN 1")
        tx.commit()

    @cluster_unsafe_test
    def test_tx_configuration(self):
        metadata = {"foo": types.CypherFloat(1.5),
                    "bar": types.CypherString("baz")}
        self._session1 = self._driver.session("r")
        tx = self._session1.begin_transaction(
            tx_meta=metadata, timeout=3000
        )
        result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
        values = []
        self.assertEqual(result.keys(), ["x"])
        for record in result:
            values.append(record.values[0])
        if get_server_info().version >= "4":
            result = tx.run("CALL tx.getMetaData")
            record = result.next()
            self.assertIsInstance(record, types.Record)
            self.assertEqual(record.values, [types.CypherMap(metadata)])
        tx.commit()
        self.assertEqual(values, list(map(types.CypherInt, range(1, 5))))

    @cluster_unsafe_test
    def test_tx_timeout(self):
        self._session1 = self._driver.session("w")
        tx0 = self._session1.begin_transaction()
        tx0.run("MERGE (:Node)").consume()
        tx0.commit()
        self._session2 = self._driver.session(
            "w", bookmarks=self._session1.last_bookmarks()
        )
        tx1 = self._session1.begin_transaction()
        tx1.run("MATCH (a:Node) SET a.property = 1").consume()
        tx2 = self._session2.begin_transaction(timeout=250)
        with self.assertRaises(types.DriverError) as e:
            result = tx2.run("MATCH (a:Node) SET a.property = 2")
            result.consume()

        self.assertEqual(e.exception.code,
                         "Neo.ClientError.Transaction.LockClientStopped")
        if get_driver_name() in ["python"]:
            self.assertEqual(e.exception.errorType,
                             "<class 'neo4j.exceptions.ClientError'>")

    @cluster_unsafe_test
    def test_consume_after_commit(self):
        self._session1 = self._driver.session("w", fetch_size=2)
        tx = self._session1.begin_transaction()
        result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
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

    @cluster_unsafe_test
    def test_parallel_queries(self):
        def _test():
            self._session1 = self._driver.session("w", fetch_size=2)
            tx = self._session1.begin_transaction()
            result1 = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
            result2 = tx.run("UNWIND [5,6,7,8] AS x RETURN x")
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
            self._session1.close()
            self._session1 = None

        for invert_fetching in (True, False):
            with self.subTest(invert_fetching=invert_fetching):
                _test()

    @cluster_unsafe_test
    def test_interwoven_queries(self):
        def _test():
            self._session1 = self._driver.session("w", fetch_size=2)
            tx = self._session1.begin_transaction()
            result1 = tx.run("UNWIND [1,2,3,4] AS x RETURN x")

            if run_q2_before_q1_fetch:
                result2 = tx.run("UNWIND [5,6,7,8] AS y RETURN y")

            self.assertEqual(result1.keys(), ["x"])
            values1 = [result1.next().values[0]]

            if not run_q2_before_q1_fetch:
                result2 = tx.run("UNWIND [5,6,7,8] AS y RETURN y")
            self.assertEqual(result2.keys(), ["y"])
            values2 = list(map(lambda rec: rec.values[0], result2))

            self.assertEqual(values2, list(map(types.CypherInt, (5, 6, 7, 8))))
            self.assertIsInstance(result2.next(), types.NullRecord)
            tx.commit()
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

            self._session1.close()
            self._session1 = None

        for run_q2_before_q1_fetch in (True, False):
            with self.subTest(run_q2_before_q1_fetch=run_q2_before_q1_fetch):
                _test()

    @cluster_unsafe_test
    def test_unconsumed_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Backend seems to misinterpret query parameters")

        def _test():
            uuid_ = str(uuid.uuid1())

            self._session1 = self._driver.session("w")
            tx = self._session1.begin_transaction()
            tx.run("CREATE (a:Thing {uuid:$uuid})",
                   params={"uuid": types.CypherString(uuid_)})
            # do not consume the result or do anything with it
            if commit:
                tx.commit()
            else:
                tx.rollback()

            res = self._session1.run(
                "MATCH (a:Thing {uuid:$uuid}) RETURN a",
                params={"uuid": types.CypherString(uuid_)}
            )
            self.assertEqual(len(list(res)), commit)

        for commit in (True, False):
            with self.subTest(commit=commit):
                _test()
