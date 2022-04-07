import nutkit.protocol as types
from tests.neo4j.shared import (
    cluster_unsafe_test,
    get_driver,
)
from tests.shared import (
    driver_feature,
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

    @cluster_unsafe_test
    def test_iteration_smaller_than_fetch_size(self):
        # Verifies that correct number of records are retrieved
        # Retrieve one extra record after last one the make sure driver can
        # handle that.
        self._session1 = self._driver.session("r", fetch_size=1000)
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
        if self.driver_supports_features(types.Feature.TMP_RESULT_KEYS):
            assert result.keys() == ["x"]
        for exp in expects:
            rec = result.next()
            self.assertEqual(rec, exp)

    @cluster_unsafe_test
    def test_can_return_node(self):
        self._session1 = self._driver.session("w")
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
        isinstance(result.next(), types.NullRecord)

    @driver_feature(types.Feature.TMP_CYPHER_PATH_AND_RELATIONSHIP)
    @cluster_unsafe_test
    def test_can_return_relationship(self):
        self._session1 = self._driver.session("w")
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
        isinstance(result.next(), types.NullRecord)

    @driver_feature(types.Feature.TMP_CYPHER_PATH_AND_RELATIONSHIP)
    @cluster_unsafe_test
    def test_can_return_path(self):
        self._session1 = self._driver.session("w")
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
        self.assertEqual(nodes[1].props,
                         types.CypherMap({"name": types.CypherString("Bob")}))

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

        isinstance(result.next(), types.NullRecord)

    @cluster_unsafe_test
    def test_autocommit_transactions_should_support_metadata(self):
        metadata = {"foo": types.CypherFloat(1.5),
                    "bar": types.CypherString("baz")}
        self._session1 = self._driver.session("r")
        result = self._session1.run(
            "CALL tx.getMetaData",
            tx_meta={k: v.value for k, v in metadata.items()}
        )
        record = result.next()
        self.assertIsInstance(record, types.Record)
        self.assertEqual(record.values, [types.CypherMap(metadata)])

    @cluster_unsafe_test
    def test_autocommit_transactions_should_support_timeout(self):
        self._session1 = self._driver.session("w")
        self._session1.run("MERGE (:Node)").consume()
        self._session2 = self._driver.session(
            "w", bookmarks=self._session1.last_bookmarks()
        )
        tx1 = self._session1.begin_transaction()
        tx1.run("MATCH (a:Node) SET a.property = 1").consume()
        with self.assertRaises(types.DriverError) as e:
            result = self._session2.run("MATCH (a:Node) SET a.property = 2",
                                        timeout=250)
            result.consume()
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            # requires explicit termination of transactions
            tx1.rollback()
        self.assertEqual(e.exception.code,
                         "Neo.TransientError.Transaction.LockClientStopped")
        if get_driver_name() in ["python"]:
            self.assertEqual(e.exception.errorType,
                             "<class 'neo4j.exceptions.TransientError'>")

    @cluster_unsafe_test
    def test_regex_in_parameter(self):
        self._session1 = self._driver.session("r")
        result = self._session1.run(
            "UNWIND ['A', 'B', 'C', 'A B', 'B C', 'A B C', 'A BC', 'AB C'] "
            "AS t WITH t WHERE t =~ $re RETURN t",
            params={"re": types.CypherString(r".*\bB\b.*")}
        )
        self.assertEqual(list(map(lambda r: r.values, result)), [
            [types.CypherString("B")],
            [types.CypherString("A B")],
            [types.CypherString("B C")],
            [types.CypherString("A B C")],
        ])

    @cluster_unsafe_test
    def test_regex_inline(self):
        self._session1 = self._driver.session("r")
        result = self._session1.run(
            "UNWIND ['A', 'B', 'C', 'A B', 'B C', 'A B C', 'A BC', 'AB C'] "
            r"AS t WITH t WHERE t =~ '.*\\bB\\b.*' RETURN t"
        )
        self.assertEqual(list(map(lambda r: r.values, result)), [
            [types.CypherString("B")],
            [types.CypherString("A B")],
            [types.CypherString("B C")],
            [types.CypherString("A B C")],
        ])

    @cluster_unsafe_test
    def test_iteration_larger_than_fetch_size(self):
        # Verifies that correct number of records are retrieved and that the
        # parameter is respected. Uses parameter to generate a long list of
        # records.  Typical fetch size is 1000, selected value should be a bit
        # larger than fetch size, if driver allows this as a parameter we
        # should set it to a known value.
        n = 1000
        self._session1 = self._driver.session("r", fetch_size=n)
        n = n + 7
        result = self._session1.run(
            "UNWIND RANGE(0, $n) AS x RETURN x",
            params={"n": types.CypherInt(n)})
        for x in range(0, n):
            exp = types.Record(values=[types.CypherInt(x)])
            rec = result.next()
            self.assertEqual(rec, exp)

    @cluster_unsafe_test
    def test_partial_iteration(self):
        # Verifies that not consuming all records works
        self._session1 = self._driver.session("r", fetch_size=2)
        result = self._session1.run("UNWIND RANGE(0, 1000) AS x RETURN x")
        for x in range(0, 4):
            exp = types.Record(values=[types.CypherInt(x)])
            rec = result.next()
            self.assertEqual(rec, exp)
        self._session1.close()
        self._session1 = None

        # not consumed all records & starting a new session
        self._session1 = self._driver.session("r", fetch_size=2)
        result = self._session1.run("UNWIND RANGE(2000, 3000) AS x RETURN x")
        for x in range(2000, 2004):
            exp = types.Record(values=[types.CypherInt(x)])
            rec = result.next()
            self.assertEqual(rec, exp)

        # not consumed all records & reusing the previous session
        result = self._session1.run("UNWIND RANGE(4000, 5000) AS x RETURN x")
        for x in range(4000, 4004):
            exp = types.Record(values=[types.CypherInt(x)])
            rec = result.next()
            self.assertEqual(rec, exp)

        self._session1.close()
        self._session1 = None

    @cluster_unsafe_test
    def test_simple_query(self):
        def _test():
            self._driver.close()
            self._driver = get_driver(self._backend, user_agent="test")
            self._session1 = self._driver.session("r", fetch_size=2)
            result = self._session1.run("UNWIND [1, 2, 3, 4] AS x RETURN x")
            if consume:
                summary = result.consume()
                self.assertIsInstance(summary, types.Summary)
            else:
                self.assertEqual(list(result), [
                    types.Record([types.CypherInt(i)]) for i in range(1, 5)
                ])
            self._session1.close()
            self._session1 = None

        for consume in (True, False):
            with self.subTest(consume=consume):
                _test()

    @cluster_unsafe_test
    def test_session_reuse(self):
        def _test():
            self._session1 = self._driver.session("r", fetch_size=2)
            result = self._session1.run("UNWIND [1, 2, 3, 4] AS x RETURN x")
            if consume:
                result.consume()
            result = self._session1.run("UNWIND [5,6,7,8] AS x RETURN x")
            records = list(map(lambda record: record.values, result))
            self.assertEqual(records,
                             [[i] for i in map(types.CypherInt, range(5, 9))])
            summary = result.consume()
            self.assertIsInstance(summary, types.Summary)
            self._session1.close()
            self._session1 = None

        for consume in (True, False):
            with self.subTest(consume=consume):
                _test()

    @cluster_unsafe_test
    def test_iteration_nested(self):
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Nested results not working in 4.2 and earlier. "
                          "FIX AND ENABLE in 4.3")
        # Verifies that it is possible to nest results with small fetch sizes.
        # Auto-commit results does not (as of 4.x) support multiple results on
        # the same connection but that isn't visible when testing at
        # this level.
        self._session1 = self._driver.session("r", fetch_size=2)

        def run(i, n):
            return self._session1.run(
                "UNWIND RANGE ($i, $n) AS x RETURN x",
                {"i": types.CypherInt(i), "n": types.CypherInt(n)})
        i0 = 0
        n0 = 6
        res0 = run(i0, n0)
        for r0 in range(i0, n0 + 1):
            rec = res0.next()
            self.assertEqual(rec, types.Record(values=[types.CypherInt(r0)]))
            i1 = 7
            n1 = 11
            res1 = run(i1, n1)
            for r1 in range(i1, n1 + 1):
                rec = res1.next()
                self.assertEqual(
                    rec, types.Record(values=[types.CypherInt(r1)]))
                i2 = 999
                n2 = 1001
                res2 = run(i2, n2)
                for r2 in range(i2, n2 + 1):
                    rec = res2.next()
                    self.assertEqual(
                        rec, types.Record(values=[types.CypherInt(r2)]))
                self.assertEqual(res2.next(), types.NullRecord())
            self.assertEqual(res1.next(), types.NullRecord())
        self.assertEqual(res0.next(), types.NullRecord())

    @cluster_unsafe_test
    def test_recover_from_invalid_query(self):
        # Verifies that an error is returned on an invalid query and that
        # the session can function with a valid query afterwards.
        self._session1 = self._driver.session("r")
        with self.assertRaises(types.DriverError):
            # DEVIATION
            # Go   - error trigger upon run
            # Java - error trigger upon iteration
            result = self._session1.run("INVALID QUERY")
            result.next()
            # TODO: Further inspection of the type of error?
            # Should be a client error

        # This one should function properly
        result = self._session1.run("RETURN 1 AS n")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(1)]))

    @cluster_unsafe_test
    def test_recover_from_fail_on_streaming(self):
        self._session1 = self._driver.session("r")
        result = self._session1.run("UNWIND [1, 0, 2] AS x RETURN 10 / x")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(10)]))
        with self.assertRaises(types.DriverError):
            result.next()
        # TODO: Further inspection of the type of error?
        # Should be a database error

        # This one should function properly
        result = self._session1.run("RETURN 1 as n")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(1)]))

    @cluster_unsafe_test
    def test_updates_last_bookmark(self):
        self._session1 = self._driver.session("w")
        result = self._session1.run("CREATE (n:SessionNode) RETURN n")
        result.consume()
        bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(bookmarks), 1)
        self.assertGreater(len(bookmarks[0]), 3)

        self._session1.close()
        self._session1 = self._driver.session("w", bookmarks=bookmarks)
        result = self._session1.run("CREATE (n:SessionNode) RETURN n")
        result.consume()
        new_bookmarks = self._session1.last_bookmarks()
        self.assertEqual(len(new_bookmarks), 1)
        self.assertNotIn(new_bookmarks[0], bookmarks)

    @cluster_unsafe_test
    def test_fails_on_bad_syntax(self):
        self._session1 = self._driver.session("w")
        with self.assertRaises(types.DriverError) as e:
            self._session1.run("X").consume()
        self.assertEqual(e.exception.code,
                         "Neo.ClientError.Statement.SyntaxError")

    @cluster_unsafe_test
    def test_fails_on_missing_parameter(self):
        self._session1 = self._driver.session("w")
        with self.assertRaises(types.DriverError) as e:
            self._session1.run("RETURN $x").consume()
        self.assertEqual(e.exception.code,
                         "Neo.ClientError.Statement.ParameterMissing")

    @cluster_unsafe_test
    def test_long_string(self):
        string = "A" * 2 ** 20
        query = "RETURN '{}'".format(string)
        for _ in range(6):
            session = self._driver.session("r")
            try:
                records = list(session.run(query))
            finally:
                session.close()
            self.assertEqual(
                list(map(lambda r: r.values, records)),
                [[types.CypherString(string)]]
            )
