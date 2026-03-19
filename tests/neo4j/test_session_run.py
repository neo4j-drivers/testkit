import nutkit.protocol as types
from tests.neo4j.shared import (
    get_auto_resolved_db,
    get_driver,
    requires_tx_metadata_support,
    requires_tx_timeout_support,
    with_retries,
)
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)


class TestSessionRun(TestkitTestCase):
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

    def _get_session(self, access_mode, fetch_size=None, bookmarks=None):
        return self._driver.session(
            access_mode,
            bookmarks=bookmarks,
            database=get_auto_resolved_db(),
            fetch_size=fetch_size,
        )

    def test_iteration_smaller_than_fetch_size(self):
        def work():
            # Verifies that correct number of records are retrieved
            # Retrieve one extra record after last one the make sure driver can
            # handle that.
            result = self._session1.run("UNWIND [1, 2, 3, 4, 5] AS x RETURN x")
            expects = [
                types.Record(values=[types.CypherInt(1)]),
                types.Record(values=[types.CypherInt(2)]),
                types.Record(values=[types.CypherInt(3)]),
                types.Record(values=[types.CypherInt(4)]),
                types.Record(values=[types.CypherInt(5)]),
                types.NullRecord(),
                types.NullRecord()
            ]
            self.assertEqual(result.keys(), ["x"])
            for exp in expects:
                rec = result.next()
                self.assertEqual(rec, exp)

        self._session1 = self._get_session("r", fetch_size=1000)
        with_retries(work)

    def test_can_return_node(self):
        def work():
            result = self._session1.run("CREATE (a:Person {name:'Alice'}) "
                                        "RETURN a")
            record = result.next()
            self.assertEqual(len(record.values), 1)
            value = record.values[0]
            # TODO: remove this when all backends encode this correctly
            self.assertIsInstance(value, types.CypherNode)
            if get_driver_name() in ["java"] and isinstance(value.id, int):
                value.id = types.CypherInt(value.id)
            self.assertIsInstance(value.id, types.CypherInt)
            self.assertEqual(value.labels,
                             types.CypherList([types.CypherString("Person")]))
            self.assertEqual(
                value.props,
                types.CypherMap({"name": types.CypherString("Alice")})
            )
            self.assertIsInstance(result.next(), types.NullRecord)
            self.assertIsInstance(result.next(), types.NullRecord)

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_can_return_relationship(self):
        def work():
            result = self._session1.run("CREATE ()-[r:KNOWS {since:1999}]->() "
                                        "RETURN r")
            record = result.next()
            self.assertEqual(len(record.values), 1)
            value = record.values[0]
            self.assertIsInstance(value, types.CypherRelationship)
            self.assertIsInstance(value.id, types.CypherInt)
            self.assertIsInstance(value.startNodeId, types.CypherInt)
            self.assertIsInstance(value.endNodeId, types.CypherInt)
            self.assertEqual(value.type, types.CypherString("KNOWS"))
            self.assertEqual(value.props,
                             types.CypherMap({"since": types.CypherInt(1999)}))
            self.assertIsInstance(result.next(), types.NullRecord)
            self.assertIsInstance(result.next(), types.NullRecord)

        self._session1 = self._get_session("w")
        with_retries(work)

    def test_can_return_path(self):
        def work():
            result = self._session1.run(
                "CREATE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'}) RETURN p"
            )
            record = result.next()
            self.assertEqual(len(record.values), 1)
            value = record.values[0]
            self.assertIsInstance(value, types.CypherPath)

            nodes = value.nodes
            self.assertIsInstance(nodes, types.CypherList)
            nodes = nodes.value
            self.assertEqual(len(nodes), 2)
            self.assertIsInstance(nodes[0], types.CypherNode)
            self.assertIsInstance(nodes[0].id, types.CypherInt)
            self.assertEqual(nodes[0].labels, types.CypherList([]))
            self.assertEqual(
                nodes[0].props,
                types.CypherMap({"name": types.CypherString("Alice")})
            )
            self.assertIsInstance(nodes[1], types.CypherNode)
            self.assertIsInstance(nodes[1].id, types.CypherInt)
            self.assertEqual(nodes[1].labels, types.CypherList([]))
            self.assertEqual(
                nodes[1].props,
                types.CypherMap({"name": types.CypherString("Bob")})
            )

            rels = value.relationships
            self.assertIsInstance(rels, types.CypherList)
            rels = rels.value
            self.assertEqual(len(rels), 1)
            self.assertIsInstance(rels[0], types.CypherRelationship)
            self.assertIsInstance(rels[0].id, types.CypherInt)
            self.assertEqual(rels[0].startNodeId, nodes[0].id)
            self.assertEqual(rels[0].endNodeId, nodes[1].id)
            self.assertEqual(rels[0].type, types.CypherString("KNOWS"))
            self.assertEqual(rels[0].props, types.CypherMap({}))

            self.assertIsInstance(result.next(), types.NullRecord)

        self._session1 = self._get_session("w")
        with_retries(work)

    @requires_tx_metadata_support
    def test_autocommit_transactions_should_support_metadata(self):
        metadata = {"foo": types.CypherFloat(1.5),
                    "bar": types.CypherString("baz")}

        def work():
            result = self._session1.run(
                "CALL tx.getMetaData",
                tx_meta=metadata
            )
            return result.next()

        self._session1 = self._get_session("r")
        record = with_retries(work)
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherMap(metadata)])

    @requires_tx_timeout_support
    def test_autocommit_transactions_should_support_timeout(self):
        def work():
            with self._get_session("w") as session1:
                session1.run("MERGE (:Node)").consume()
                with self._get_session(
                    "w", bookmarks=session1.last_bookmarks()
                ) as session2:
                    with session1.begin_transaction() as tx:
                        tx.run("MATCH (a:Node) SET a.property = 1").consume()
                        with self.assertRaises(types.DriverError) as e:
                            result = session2.run(
                                "MATCH (a:Node) SET a.property = 2",
                                timeout=250,
                            )
                            result.consume()
                    self.assertEqual(
                        e.exception.code,
                        "Neo.ClientError.Transaction.LockClientStopped",
                    )
                    if get_driver_name() in ["python"]:
                        self.assertEqual(
                            e.exception.errorType,
                            "<class 'neo4j.exceptions.ClientError'>",
                        )

        with_retries(work)

    def test_regex_in_parameter(self):
        def work():
            result = self._session1.run(
                "UNWIND "
                "['A', 'B', 'C', 'A B', 'B C', 'A B C', 'A BC', 'AB C'] "
                "AS t WITH t WHERE t =~ $re RETURN t",
                params={"re": types.CypherString(r".*\bB\b.*")}
            )
            self.assertEqual(list(map(lambda r: r.values, result)), [
                [types.CypherString("B")],
                [types.CypherString("A B")],
                [types.CypherString("B C")],
                [types.CypherString("A B C")],
            ])

        self._session1 = self._get_session("r")
        with_retries(work)

    def test_regex_inline(self):
        def work():
            result = self._session1.run(
                "UNWIND "
                "['A', 'B', 'C', 'A B', 'B C', 'A B C', 'A BC', 'AB C'] "
                r"AS t WITH t WHERE t =~ '.*\\bB\\b.*' RETURN t"
            )
            self.assertEqual(list(map(lambda r: r.values, result)), [
                [types.CypherString("B")],
                [types.CypherString("A B")],
                [types.CypherString("B C")],
                [types.CypherString("A B C")],
            ])

        self._session1 = self._get_session("r")
        with_retries(work)

    def test_iteration_larger_than_fetch_size(self):
        # Verifies that correct number of records are retrieved and that the
        # parameter is respected. Uses parameter to generate a long list of
        # records.  Typical fetch size is 1000, selected value should be a bit
        # larger than fetch size, if driver allows this as a parameter we
        # should set it to a known value.

        fetch_size = 1000

        def work():
            result = self._session1.run(
                "UNWIND RANGE(0, $n) AS x RETURN x",
                params={"n": types.CypherInt(fetch_size + 7)}
            )
            for x in range(0, fetch_size + 7):
                exp = types.Record(values=[types.CypherInt(x)])
                rec = result.next()
                self.assertEqual(rec, exp)

        self._session1 = self._get_session("r", fetch_size=fetch_size)
        with_retries(work)

    def test_partial_iteration(self):
        def work():
            # Verifies that not consuming all records works
            with self._get_session("r", fetch_size=2) as session:
                result = session.run(
                    "UNWIND RANGE(0, 1000) AS x RETURN x"
                )
                for x in range(0, 4):
                    exp = types.Record(values=[types.CypherInt(x)])
                    rec = result.next()
                    self.assertEqual(rec, exp)

            # not consumed all records & starting a new session
            with self._get_session("r", fetch_size=2) as session:
                result = session.run(
                    "UNWIND RANGE(2000, 3000) AS x RETURN x"
                )
                for x in range(2000, 2004):
                    exp = types.Record(values=[types.CypherInt(x)])
                    rec = result.next()
                    self.assertEqual(rec, exp)

                # not consumed all records & reusing the previous session
                result = session.run(
                    "UNWIND RANGE(4000, 5000) AS x RETURN x"
                )
                for x in range(4000, 4004):
                    exp = types.Record(values=[types.CypherInt(x)])
                    rec = result.next()
                    self.assertEqual(rec, exp)

        with_retries(work)

    def test_simple_query(self):
        def _test():
            def work(session_):
                result = session_.run(
                    "UNWIND [1, 2, 3, 4] AS x RETURN x"
                )
                if consume:
                    summary = result.consume()
                    self.assertIsInstance(summary, types.Summary)
                else:
                    self.assertEqual(list(result), [
                        types.Record([types.CypherInt(i)]) for i in range(1, 5)
                    ])

            self._driver.close()
            self._driver = get_driver(self._backend, user_agent="test")
            with self._get_session("r", fetch_size=2) as session:
                with_retries(work, session)

        for consume in (True, False):
            with self.subTest(consume=consume):
                _test()

    def test_session_reuse(self):
        def _test():
            def work(session_):
                result = session_.run(
                    "UNWIND [1, 2, 3, 4] AS x RETURN x"
                )
                if consume:
                    result.consume()
                result = session_.run("UNWIND [5,6,7,8] AS x RETURN x")
                records = list(map(lambda record: record.values, result))
                self.assertEqual(
                    records,
                    [[i] for i in map(types.CypherInt, range(5, 9))],
                )
                summary = result.consume()
                self.assertIsInstance(summary, types.Summary)

            with self._get_session("r", fetch_size=2) as session:
                with_retries(work, session)

        for consume in (True, False):
            with self.subTest(consume=consume):
                _test()

    def test_iteration_nested(self):
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Nested results not working in 4.2 and earlier. "
                          "FIX AND ENABLE in 4.3")
        # Verifies that it is possible to nest results with small fetch sizes.
        # Auto-commit results does not (as of 4.x) support multiple results on
        # the same connection but that isn't visible when testing at
        # this level.

        def work():
            def run(session_, i, n):
                return session_.run(
                    "UNWIND RANGE ($i, $n) AS x RETURN x",
                    params={"i": types.CypherInt(i), "n": types.CypherInt(n)}
                )

            with self._get_session("r", fetch_size=2) as session:
                i0 = 0
                n0 = 6
                res0 = run(session, i0, n0)
                for r0 in range(i0, n0 + 1):
                    rec = res0.next()
                    self.assertEqual(
                        rec,
                        types.Record(values=[types.CypherInt(r0)]),
                    )
                    i1 = 7
                    n1 = 11
                    res1 = run(session, i1, n1)
                    for r1 in range(i1, n1 + 1):
                        rec = res1.next()
                        self.assertEqual(
                            rec,
                            types.Record(values=[types.CypherInt(r1)]),
                        )
                        i2 = 999
                        n2 = 1001
                        res2 = run(session, i2, n2)
                        for r2 in range(i2, n2 + 1):
                            rec = res2.next()
                            self.assertEqual(
                                rec,
                                types.Record(values=[types.CypherInt(r2)]),
                            )
                        self.assertEqual(res2.next(), types.NullRecord())
                    self.assertEqual(res1.next(), types.NullRecord())
                self.assertEqual(res0.next(), types.NullRecord())

        with_retries(work)

    def test_recover_from_invalid_query(self):
        # Verifies that an error is returned on an invalid query and that
        # the session can function with a valid query afterwards.
        def work():
            with self._get_session("r") as session:
                with self.assertRaises(types.DriverError):
                    # DEVIATION
                    # Go   - error trigger upon run
                    # Java - error trigger upon iteration
                    result = session.run("INVALID QUERY")
                    result.next()
                    # TODO: Further inspection of the type of error?
                    # Should be a client error

                # This one should function properly
                result = session.run("RETURN 1 AS n")
                self.assertEqual(
                    result.next(), types.Record(values=[types.CypherInt(1)])
                )

        with_retries(work)

    def test_recover_from_fail_on_streaming(self):
        def work():
            with self._get_session("r") as session:
                result = session.run(
                    "UNWIND [1, 0, 2] AS x RETURN 10 / x"
                )
                self.assertEqual(
                    result.next(), types.Record(values=[types.CypherInt(10)])
                )
                with self.assertRaises(types.DriverError):
                    result.next()
                # TODO: Further inspection of the type of error?
                # Should be a database error

                # This one should function properly
                result = session.run("RETURN 1 as n")
                self.assertEqual(
                    result.next(), types.Record(values=[types.CypherInt(1)])
                )

        with_retries(work)

    def test_updates_last_bookmark(self):
        def work():
            with self._get_session("w") as session:
                result = session.run("CREATE (n:SessionNode) RETURN n")
                result.consume()
                bookmarks = session.last_bookmarks()
                self.assertEqual(len(bookmarks), 1)
                self.assertGreater(len(bookmarks[0]), 3)

            with self._get_session("w", bookmarks=bookmarks) as session:
                result = session.run("CREATE (n:SessionNode) RETURN n")
                result.consume()
                new_bookmarks = session.last_bookmarks()
                self.assertEqual(len(new_bookmarks), 1)
                self.assertNotIn(new_bookmarks[0], bookmarks)

        with_retries(work)

    def test_fails_on_bad_syntax(self):
        self._session1 = self._get_session("w")
        with self.assertRaises(types.DriverError) as e:
            with_retries(lambda: self._session1.run("X").consume())
        self.assertEqual(e.exception.code,
                         "Neo.ClientError.Statement.SyntaxError")

    def test_fails_on_missing_parameter(self):
        self._session1 = self._get_session("w")
        with self.assertRaises(types.DriverError) as e:
            with_retries(lambda: self._session1.run("RETURN $x").consume())
        self.assertEqual(e.exception.code,
                         "Neo.ClientError.Statement.ParameterMissing")

    def test_long_string(self):
        string = "A" * 2 ** 20
        query = "RETURN '{}'".format(string)
        for _ in range(6):
            with self._get_session("r") as session:
                records = with_retries(lambda s: list(s.run(query)), session)
            self.assertEqual(
                list(map(lambda r: r.values, records)),
                [[types.CypherString(string)]]
            )
