import uuid

import nutkit.protocol as types
from tests.neo4j.shared import (
    get_auto_resolved_db,
    get_driver,
    get_server_info,
    requires_tx_metadata_support,
    requires_tx_timeout_support,
    with_retries,
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

    def _get_session(
        self, access_mode, bookmarks=None, database=None, fetch_size=None
    ):
        if database is None:
            database = get_auto_resolved_db()
        return self._driver.session(
            access_mode, bookmarks, database=database, fetch_size=fetch_size
        )

    def test_simple_query(self):
        def _test():
            def work():
                with self._session1.begin_transaction() as tx:
                    result = tx.run("UNWIND [1, 2, 3, 4] AS x RETURN x")
                    if consume:
                        summary = result.consume()
                        self.assertIsInstance(summary, types.Summary)
                    else:
                        self.assertEqual(list(result), [
                            types.Record([types.CypherInt(i)])
                            for i in range(1, 5)
                        ])
                    if rollback:
                        tx.rollback()
                    else:
                        tx.commit()

            self._driver.close()
            self._driver = get_driver(self._backend, user_agent="test")
            self._session1 = self._get_session("r", fetch_size=2)
            try:
                with_retries(work)
            finally:
                self._session1.close()
                self._session1 = None

        for consume in (True, False):
            for rollback in (True, False):
                with self.subTest(consume=consume, rollback=rollback):
                    _test()

    def test_can_commit_transaction(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest(
                'Returns CypherNull as foo\'s value instead of "bar"'
            )

        def create_and_update():
            with self._session1.begin_transaction() as tx:
                # Create a node
                result = tx.run("CREATE (a) RETURN id(a)")
                record = result.next()
                self.assertIsInstance(record, types.Record)
                self.assertEqual(len(record.values), 1)

                node_id_ = record.values[0]
                self.assertIsInstance(node_id_, types.CypherInt)

                # Update a property
                tx.run(
                    "MATCH (a) WHERE id(a) = $n SET a.foo = $foo",
                    params={"n": node_id_, "foo": types.CypherString("bar")},
                )

                tx.commit()
                return node_id_

        def read(node_id_):
            # Check the property value
            result = self._session1.run(
                "MATCH (a) WHERE id(a) = $n RETURN a.foo",
                params={"n": node_id_}
            )
            record = result.next()
            self.assertIsInstance(record, types.Record)
            self.assertEqual(len(record.values), 1)
            self.assertEqual(record.values[0], types.CypherString("bar"))

        self._session1 = self._get_session("w")
        node_id = with_retries(create_and_update)
        with_retries(read, node_id)

    def test_can_rollback_transaction(self):
        def create_and_update_rolled_back():
            with self._session1.begin_transaction() as tx:
                # Create a node
                result = tx.run("CREATE (a) RETURN id(a)")
                record = result.next()
                self.assertIsInstance(record, types.Record)
                self.assertEqual(len(record.values), 1)

                node_id_ = record.values[0]
                self.assertIsInstance(node_id_, types.CypherInt)

                # Update a property
                tx.run(
                    "MATCH (a) WHERE id(a) = $n SET a.foo = $foo",
                    params={"n": node_id_, "foo": types.CypherString("bar")},
                )

                tx.rollback()
                return node_id_

        def read(node_id_):
            # Check the property value
            result = self._session1.run(
                "MATCH (a) WHERE id(a) = $n RETURN a.foo",
                params={"n": node_id_}
            )
            record = result.next()
            self.assertIsInstance(record, types.NullRecord)

        self._session1 = self._get_session("w")
        node_id = with_retries(create_and_update_rolled_back)
        with_retries(read, node_id)

    def test_updates_last_bookmark_on_commit(self):
        # Verifies that last bookmark is set on the session upon
        # successful commit.
        def work():
            with self._session1.begin_transaction() as tx:
                tx.run("CREATE (n:SessionNode) RETURN n")
                tx.commit()

        self._session1 = self._get_session("w")
        with_retries(work)
        bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(bookmarks), 1)
        self.assertGreater(len(bookmarks[0]), 3)

    def test_does_not_update_last_bookmark_on_rollback(self):
        # Verifies that last bookmark is set on the session upon
        # successful commit.
        def work():
            with self._session1.begin_transaction() as tx:
                tx.run("CREATE (n:SessionNode) RETURN n")
                tx.rollback()

        self._session1 = self._get_session("w")
        with_retries(work)
        bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(bookmarks), 0)

    def test_does_not_update_last_bookmark_on_failure(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Raises syntax error on session.close in tearDown.")

        def work():
            with self._session1.begin_transaction() as tx:
                with self.assertRaises(types.responses.DriverError):
                    result = tx.run("RETURN")
                    # TODO: remove this block once all languages work
                    if get_driver_name() in ["javascript"]:
                        result.next()
                tx.close()

        self._session1 = self._get_session("w")
        with_retries(work)
        bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(bookmarks), 0)

    def test_should_be_able_to_rollback_a_failure(self):
        def work():
            with self._session1.begin_transaction() as tx:
                with self.assertRaises(types.responses.DriverError):
                    tx.run("RETURN").next()
                tx.rollback()

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_should_not_commit_a_failure(self):
        def work():
            with self._session1.begin_transaction() as tx:
                with self.assertRaises(types.responses.DriverError):
                    tx.run("RETURN").next()
                with self.assertRaises(types.responses.DriverError):
                    tx.commit()

        self._session1 = self._get_session("r")
        with_retries(work)

    def test_should_not_rollback_a_rollbacked_tx(self):
        def work():
            with self._session1.begin_transaction() as tx:
                tx.run("CREATE (:TXNode1)").consume()
                tx.rollback()
                with self.assertRaises(types.responses.DriverError):
                    tx.rollback()

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_should_not_rollback_a_commited_tx(self):
        def work():
            with self._session1.begin_transaction() as tx:
                tx.run("CREATE (:TXNode1)").consume()
                tx.commit()
                with self.assertRaises(types.responses.DriverError):
                    tx.rollback()

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_should_not_commit_a_commited_tx(self):
        def work():
            with self._session1.begin_transaction() as tx:
                tx.run("CREATE (:TXNode1)").consume()
                tx.commit()
                with self.assertRaises(types.responses.DriverError):
                    tx.commit()

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_should_not_allow_run_on_a_commited_tx(self):
        def work():
            with self._session1.begin_transaction() as tx:
                tx.run("CREATE (:TXNode1)").consume()
                tx.commit()
                with self.assertRaises(types.responses.DriverError):
                    result = tx.run("RETURN 1")
                    # TODO: remove this block once all languages work
                    if get_driver_name() in ["javascript"]:
                        result.next()

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_should_not_allow_run_on_a_rollbacked_tx(self):
        def work():
            with self._session1.begin_transaction() as tx:
                tx.run("CREATE (:TXNode1)").consume()
                tx.rollback()
                with self.assertRaises(types.responses.DriverError):
                    result = tx.run("RETURN 1")
                    # TODO: remove this block once all languages work
                    if get_driver_name() in ["javascript"]:
                        result.next()

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_should_not_run_valid_query_in_invalid_tx(self):
        def work():
            with self._session1.begin_transaction() as tx:
                with self.assertRaises(types.responses.DriverError):
                    tx.run("NOT CYPHER").consume()

                with self.assertRaises(types.responses.DriverError):
                    tx.run("RETURN 42").next()

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_should_fail_run_in_a_commited_tx(self):
        def work():
            with self._session1.begin_transaction() as tx:
                tx.commit()
                with self.assertRaises(types.responses.DriverError):
                    tx.run("RETURN 42").next()

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_should_fail_run_in_a_rollbacked_tx(self):
        def work():
            with self._session1.begin_transaction() as tx:
                tx.rollback()
                with self.assertRaises(types.responses.DriverError):
                    tx.run("RETURN 42").next()

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_should_fail_to_run_query_for_invalid_bookmark(self):
        def get_bookmarks():
            with self._session1.begin_transaction() as tx1:
                result = tx1.run("CREATE ()")
                result.consume()
                tx1.commit()
                return self._session1.last_bookmarks()

        def use_invalid_bookmark():
            with self.assertRaises(types.responses.DriverError):
                with self._session1.begin_transaction() as tx2:
                    tx2.run("CREATE ()").consume()

        self._session1 = self._get_session("w")
        last_bookmarks = with_retries(get_bookmarks)
        assert len(last_bookmarks) == 1
        last_bookmark = last_bookmarks[0]
        invalid_bookmark = last_bookmark[:-1] + "-"
        self._session1.close()

        self._session1 = self._get_session("w", [invalid_bookmark])
        with_retries(use_invalid_bookmark)

    def test_broken_transaction_should_not_break_session(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Raises syntax error on session.close in tearDown.")

        def test():
            with self._get_session("r") as session:
                tx = session.begin_transaction()
                with self.assertRaises(types.DriverError):
                    result = tx.run("NOT CYPHER")
                    # TODO: remove this block once all languages work
                    if get_driver_name() in ["javascript"]:
                        result.next()
                # TODO: remove this block once all languages work
                if get_driver_name() in ["java", "ruby"]:
                    # requires explicit rollback on a failed transaction
                    tx.rollback()
                tx = session.begin_transaction()
                tx.run("RETURN 1")
                tx.commit()

        with_retries(test)

    @requires_tx_metadata_support
    @requires_tx_timeout_support
    def test_tx_configuration(self):
        metadata = {"foo": types.CypherFloat(1.5),
                    "bar": types.CypherString("baz")}
        self._session1 = self._get_session("r")

        def work():
            with self._session1.begin_transaction(
                tx_meta=metadata, timeout=3000
            ) as tx:
                result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
                values = []
                self.assertEqual(result.keys(), ["x"])
                for record in result:
                    values.append(record.values[0])
                if get_server_info().parsed_version() >= (4, 0):
                    result = tx.run("CALL tx.getMetaData")
                    record = result.next()
                    self.assertIsInstance(record, types.Record)
                    self.assertEqual(
                        record.values, [types.CypherMap(metadata)]
                    )
                tx.commit()
                self.assertEqual(
                    values, list(map(types.CypherInt, range(1, 5)))
                )

        with_retries(work)

    @requires_tx_timeout_support
    def test_tx_timeout(self):
        def setup():
            tx0 = self._session1.begin_transaction()
            tx0.run("MERGE (:Node)").consume()
            tx0.commit()

        def work():
            with self._session1.begin_transaction() as tx1:
                with self._session2.begin_transaction(timeout=250) as tx2:
                    tx1.run("MATCH (a:Node) SET a.property = 1").consume()

                    with self.assertRaises(types.DriverError) as e:
                        result = tx2.run("MATCH (a:Node) SET a.property = 2")
                        result.consume()
                    return e

        self._session1 = self._get_session("w")
        with_retries(setup)

        self._session2 = self._get_session(
            "w", bookmarks=self._session1.last_bookmarks()
        )
        e = with_retries(work)
        self.assertEqual(e.exception.code,
                         "Neo.ClientError.Transaction.LockClientStopped")
        if get_driver_name() in ["python"]:
            self.assertEqual(e.exception.errorType,
                             "<class 'neo4j.exceptions.ClientError'>")

    def test_consume_after_commit(self):
        def work():
            with self._session1.begin_transaction() as tx:
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
                #   - Don't fetch any further records but return what is
                #     buffered rn
                #   - throw exception
                #   - Commit should've buffered all records and return them now

        self._session1 = self._get_session("w", fetch_size=2)
        with_retries(work)

    def test_parallel_queries(self):
        def _test():
            with self._get_session("w", fetch_size=2) as session:
                with session.begin_transaction() as tx:
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
                    self.assertEqual(
                        values1, list(map(types.CypherInt, (1, 2, 3, 4)))
                    )
                    self.assertEqual(
                        values2, list(map(types.CypherInt, (5, 6, 7, 8)))
                    )

        for invert_fetching in (True, False):
            with self.subTest(invert_fetching=invert_fetching):
                with_retries(_test)

    def test_interwoven_queries(self):
        def _test():
            with self._get_session("w", fetch_size=2) as session:
                with session.begin_transaction() as tx:
                    result1 = tx.run("UNWIND [1,2,3,4] AS x RETURN x")

                    if run_q2_before_q1_fetch:
                        result2 = tx.run("UNWIND [5,6,7,8] AS y RETURN y")

                    self.assertEqual(result1.keys(), ["x"])
                    values1 = [result1.next().values[0]]

                    if not run_q2_before_q1_fetch:
                        result2 = tx.run("UNWIND [5,6,7,8] AS y RETURN y")
                    self.assertEqual(result2.keys(), ["y"])
                    values2 = list(map(lambda rec: rec.values[0], result2))

                    self.assertEqual(
                        values2, list(map(types.CypherInt, (5, 6, 7, 8)))
                    )
                    self.assertIsInstance(result2.next(), types.NullRecord)
                    tx.commit()
                    self.assertEqual(values1, [types.CypherInt(1)])
                    # TODO: what should result1.next() result in?
                    # options:
                    # result should have discarded all records.
                    # self.assertEqual(
                    #     list(map(lambda rec: rec.values[0], result1)), []
                    # )

                    # result should buffer records on tx closure
                    # values1 += list(map(lambda rec: rec.values[0], result1))
                    # self.assertEqual(values1, list(map(types.CypherInt,
                    #                                    (1, 2, 3, 4))))

                    # result should have been consumed and raises an exception
                    # with self.assertRaises(types.DriverError) as exc:
                    #     result1.next()
                    # # TODO: check exc

        for run_q2_before_q1_fetch in (True, False):
            with self.subTest(run_q2_before_q1_fetch=run_q2_before_q1_fetch):
                with_retries(_test)

    def test_unconsumed_result(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Backend seems to misinterpret query parameters")

        def _test():
            uuid_ = str(uuid.uuid1())

            with self._get_session("w") as session:
                with session.begin_transaction() as tx:
                    tx.run("CREATE (a:Thing {uuid:$uuid})",
                           params={"uuid": types.CypherString(uuid_)})
                    # do not consume the result or do anything with it
                    if commit:
                        tx.commit()
                    else:
                        tx.rollback()

                    res = session.run(
                        "MATCH (a:Thing {uuid:$uuid}) RETURN a",
                        params={"uuid": types.CypherString(uuid_)}
                    )
                    self.assertEqual(len(list(res)), commit)

        for commit in (True, False):
            with self.subTest(commit=commit):
                with_retries(_test)
