from nutkit import protocol as types
from tests.neo4j.shared import (
    bolt_only_test,
    cluster_unsafe_test,
    get_auto_resolved_db,
    get_default_db,
    get_driver,
    get_neo4j_host_and_port,
    get_neo4j_host_and_wrong_port,
    get_neo4j_scheme,
    has_summary_query_type_support,
    QueryBuilder,
    requires_multi_db_support,
    requires_tx_support,
    unencrypted_only_test,
    with_retries,
)
from tests.shared import (
    dns_resolve_single,
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)


class TestDirectDriver(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._driver = None
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        if self._driver:
            self._driver.close()
        super().tearDown()

    def _get_session(self, access_mode, database=None, bookmarks=None):
        if database is None:
            database = get_auto_resolved_db()
        return self._driver.session(
            access_mode, bookmarks=bookmarks, database=database
        )

    @cluster_unsafe_test
    @bolt_only_test  # custom resolver is ignored via HTTP
    @unencrypted_only_test
    def test_custom_resolver(self):
        # TODO unify this
        if get_driver_name() in ["javascript", "dotnet"]:
            self.skipTest("resolver not implemented in backend")
        if get_driver_name() in ["go", "java"]:
            self.skipTest("Does not call resolver for direct connections")

        host, port = get_neo4j_host_and_port()
        host = dns_resolve_single(host)
        resolved_addresses = []

        def my_resolver(socket_address):
            resolved_addresses.append(socket_address)
            return (
                # should be rejected as unable to connect
                "127.100.200.42:%d" % port,
                "%s:%d" % (host, port),  # should succeed
            )

        # Fixing to `bolt` (as opposed to `get_neo4j_scheme()`) to avoid the
        # resolver being called twice (once when fetching the routing table,
        # once when establishing a connection for running the query).
        uri = "bolt://*"
        self._driver = get_driver(self._backend, uri=uri,
                                  resolver_fn=my_resolver,
                                  connection_timeout_ms=200)
        self._session = self._get_session("r")
        result = self._session.run("RETURN 1")
        summary = result.consume()

        self.assertEqual(summary.server_info.address, "%s:%d" % (host, port))
        self.assertEqual(resolved_addresses, ["*:7687"])

    @driver_feature(types.Feature.API_DRIVER_VERIFY_CONNECTIVITY)
    def test_fail_nicely_when_using_wrong_port(self):
        # TODO add support and remove this block
        if get_driver_name() in ["go"]:
            self.skipTest("verifyConnectivity not implemented in backend")
        scheme = get_neo4j_scheme()
        host, port = get_neo4j_host_and_wrong_port()
        uri = "%s://%s:%d" % (scheme, host, port)
        self._driver = get_driver(self._backend, uri=uri,
                                  connection_timeout_ms=500)
        with self.assertRaises(types.DriverError) as e:
            self._driver.verify_connectivity()
        if get_driver_name() in ["python"]:
            self.assertEqual(e.exception.errorType,
                             "<class 'neo4j.exceptions.ServiceUnavailable'>")

    @requires_tx_support
    def test_supports_multi_db(self):
        def work(tx):
            return tx.run("RETURN 1 as n").consume()

        self._driver = get_driver(self._backend)
        self._session = self._get_session("w")
        summary = self._session.execute_read(work)
        result = self._driver.supports_multi_db()

        self.assertTrue(result)
        # This is the default database name if not set explicitly on the
        # Neo4j Server
        self.assertEqual(summary.database, get_default_db())

        if has_summary_query_type_support(self):
            self.assertEqual(summary.query_type, "r")

    @requires_multi_db_support
    def test_multi_db_non_existing(self):
        self._driver = get_driver(self._backend)
        self._session = self._get_session("w", database="system")
        self._session.run(QueryBuilder.drop_db("test-database")).consume()
        self._session.close()

        self._session = self._get_session("r", database="test-database")
        with self.assertRaises(types.DriverError) as e:
            result = self._session.run("RETURN 1")
            result.next()
        exc = e.exception
        self.assertEqual(exc.code,
                         "Neo.ClientError.Database.DatabaseNotFound")
        self.assertIn("test-database", exc.msg)
        self.assertIn("exist", exc.msg)
        if get_driver_name() in ["python"]:
            self.assertEqual(exc.errorType,
                             "<class 'neo4j.exceptions.ClientError'>")

    @requires_multi_db_support
    def test_multi_db(self):
        self._driver = get_driver(self._backend)
        create_db_query = QueryBuilder.create_db("test-database")
        drop_db_query = QueryBuilder.drop_db("test-database")

        self._session = self._get_session("w", database="system")

        with_retries(lambda: self._session.run(drop_db_query).consume())
        with_retries(lambda: self._session.run(create_db_query).consume())
        self._session.close()

        self._session = self._get_session("r", database="test-database")

        def get_db_name(session):
            result = session.run("RETURN 1")
            # server bug on 4.4-: does not report db on DISCARD before PULL
            result.next()
            return result.consume().database

        db_name = with_retries(lambda: get_db_name(self._session))
        self.assertEqual(db_name, "test-database")

        self._session.close()
        self._session = self._get_session("w", database="system")
        with_retries(lambda: self._session.run(drop_db_query).consume())

    @requires_multi_db_support
    def test_multi_db_various_databases(self):
        def get_people_names(sessions):
            return get_names(sessions.run("MATCH (p:Person) RETURN p"))

        def get_db_names(sessions):
            return get_names(sessions.run("SHOW DATABASES"), node=False)

        def get_names(result_, node=True):
            names = set()
            for record in result_:
                if node:
                    self.assertEqual(len(record.values), 1)
                    self.assertEqual(result_.keys(), ["p"])
                    p = record.values[0]
                    self.assertIsInstance(p, types.CypherNode)
                    self.assertIsInstance(p.props, types.CypherMap)
                    name = p.props.value.get("name")
                else:
                    keys = result_.keys()
                    self.assertIn("name", keys)
                    idx = keys.index("name")
                    name = record.values[idx]
                self.assertIsInstance(name, types.CypherString)
                names.add(name.value)
            return names

        def run_consume(session, query):
            with_retries(lambda: session.run(query).consume())

        create_db_testa_query = QueryBuilder.create_db("testa")
        create_db_testb_query = QueryBuilder.create_db("testb")
        drop_db_testa_query = QueryBuilder.drop_db("testa")
        drop_db_testb_query = QueryBuilder.drop_db("testb")
        wipe_all_query = "MATCH (n) DETACH DELETE n"

        self._driver = get_driver(self._backend)

        self._session = self._get_session("w")
        # Test that default database is empty
        run_consume(self._session, wipe_all_query)
        names = with_retries(lambda: get_people_names(self._session))
        self.assertEqual(names, set())
        self._session.close()

        self._session = self._get_session("w", database="system")
        run_consume(self._session, drop_db_testa_query)
        run_consume(self._session, drop_db_testb_query)
        bookmarks = self._session.last_bookmarks()
        self._session.close()
        self._session = self._get_session("w", database="system",
                                          bookmarks=bookmarks)
        names = with_retries(lambda: get_db_names(self._session))
        self.assertEqual(names, {"system", get_default_db()})

        run_consume(self._session, create_db_testa_query)
        run_consume(self._session, create_db_testb_query)
        bookmarks = self._session.last_bookmarks()
        self._session.close()

        self._session = self._get_session("w", database="testa",
                                          bookmarks=bookmarks)
        run_consume(self._session, 'CREATE (p:Person {name: "ALICE"})')
        self._session.close()

        self._session = self._get_session("w", database="testb")
        run_consume(self._session, 'CREATE (p:Person {name: "BOB"})')
        self._session.close()

        self._session = self._get_session("w")
        # Test that default database is still empty
        names = with_retries(lambda: get_people_names(self._session))
        self.assertEqual(names, set())
        self._session.close()

        self._session = self._get_session("w", database="testa")
        names = with_retries(lambda: get_people_names(self._session))
        self.assertEqual(names, {"ALICE"})
        self._session.close()

        self._session = self._get_session("w", database="testb")
        names = with_retries(lambda: get_people_names(self._session))
        self.assertEqual(names, {"BOB"})
        self._session.close()

        self._session = self._get_session("w", database="system")
        run_consume(self._session, drop_db_testa_query)
        self._session.close()

        self._session = self._get_session("w", database="system")
        run_consume(self._session, drop_db_testb_query)
        self._session.close()

        self._session = self._get_session("w")
        run_consume(self._session, wipe_all_query)
