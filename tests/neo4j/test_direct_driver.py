from nutkit import protocol as types
from nutkit.protocol.error_type import ErrorType

from ..shared import (
    dns_resolve_single,
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from .shared import (
    cluster_unsafe_test,
    get_driver,
    get_neo4j_host_and_http_port,
    get_neo4j_host_and_port,
    get_neo4j_scheme,
    get_server_info,
    requires_multi_db_support,
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

    @cluster_unsafe_test
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

        self._driver = get_driver(self._backend, uri="bolt://*",
                                  resolver_fn=my_resolver,
                                  connection_timeout_ms=200)
        self._session = self._driver.session("r")
        result = self._session.run("RETURN 1")
        summary = result.consume()

        self.assertEqual(summary.server_info.address, "%s:%d" % (host, port))
        self.assertEqual(resolved_addresses, ["*:7687"])

    @driver_feature(types.Feature.API_DRIVER_VERIFY_CONNECTIVITY)
    def test_fail_nicely_when_using_http_port(self):
        # TODO add support and remove this block
        if get_driver_name() in ["go"]:
            self.skipTest("verifyConnectivity not implemented in backend")
        scheme = get_neo4j_scheme()
        host, port = get_neo4j_host_and_http_port()
        uri = "%s://%s:%d" % (scheme, host, port)
        self._driver = get_driver(self._backend, uri=uri,
                                  connection_timeout_ms=500)
        with self.assertRaises(types.DriverError) as e:
            self._driver.verify_connectivity()
        if get_driver_name() in ["python"]:
            self.assertEqual(e.exception.errorType,
                             "<class 'neo4j.exceptions.ServiceUnavailable'>")
        elif get_driver_name() in ["java"]:
            self.assertEqual(ErrorType.SERVICE_UNAVAILABLE_ERROR.value,
                             e.exception.errorType)

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_supports_multi_db(self):
        def work(tx):
            return tx.run("RETURN 1 as n").consume()

        self._driver = get_driver(self._backend)
        self._session = self._driver.session("w")
        summary = self._session.read_transaction(work)
        result = self._driver.supports_multi_db()

        self.assertTrue(result)
        # This is the default database name if not set explicitly on the
        # Neo4j Server
        self.assertEqual(summary.database, "neo4j")

        self.assertEqual(summary.query_type, "r")

    def test_multi_db_non_existing(self):
        if not get_server_info().supports_multi_db:
            self.skipTest("Needs multi DB support")
        self._driver = get_driver(self._backend)
        self._session = self._driver.session("r", database="test-database")
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
        elif get_driver_name() in ["java"]:
            self.assertEqual(ErrorType.FATAL_DISCOVERY_ERROR.value,
                             exc.errorType)

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    @requires_multi_db_support
    @cluster_unsafe_test
    def test_multi_db(self):
        self._driver = get_driver(self._backend)
        server_info = get_server_info()
        if server_info.max_protocol_version >= "4":
            self._session = self._driver.session("w", database="system")

            self._session.run("DROP DATABASE `test-database` IF EXISTS")\
                .consume()
            self._session.run("CREATE DATABASE `test-database`").consume()
            self._session.close()

            self._session = self._driver.session("r", database="test-database")
            result = self._session.run("RETURN 1")
            summary = result.consume()
            self.assertEqual(summary.database, "test-database")

            self._session.close()
            self._session = self._driver.session("w", database="system")
            self._session.run("DROP DATABASE `test-database` IF EXISTS").\
                consume()
        else:
            self._session = self._driver.session("w", database="neo4j")
            with self.assertRaises(types.DriverError) as e:
                self._session.run("RETURN 1").consume()
            if get_driver_name() in ["python"]:
                self.assertEqual(
                    "<class 'neo4j.exceptions.ConfigurationError'>",
                    e.exception.errorType
                )
                self.assertIn(
                    "database is not supported in Bolt Protocol Version(3, 0)",
                    e.exception.msg
                )
            elif get_driver_name() in ["java"]:
                self.assertEqual(ErrorType.CLIENT_ERROR.value,
                                 e.exception.errorType)

    @requires_multi_db_support
    @cluster_unsafe_test
    def test_multi_db_various_databases(self):
        def get_names(result_, node=True):
            names = set()
            for record in result_:
                if node:
                    self.assertEqual(len(record.values), 1)
                    if self.driver_supports_features(
                            types.Feature.TMP_RESULT_KEYS):
                        self.assertEqual(result_.keys(), ["p"])
                    p = record.values[0]
                    self.assertIsInstance(p, types.CypherNode)
                    self.assertIsInstance(p.props, types.CypherMap)
                    name = p.props.value.get("name")
                else:
                    idx = 0
                    if self.driver_supports_features(
                            types.Feature.TMP_RESULT_KEYS):
                        keys = result_.keys()
                        self.assertIn("name", keys)
                        idx = keys.index("name")
                    name = record.values[idx]
                self.assertIsInstance(name, types.CypherString)
                names.add(name.value)
            return names

        self._driver = get_driver(self._backend)

        self._session = self._driver.session("w")
        # Test that default database is empty
        self._session.run("MATCH (n) DETACH DELETE n").consume()
        result = self._session.run("MATCH (p:Person) RETURN p")
        self.assertIsInstance(result.next(), types.NullRecord)
        self._session.close()
        self._session = self._driver.session("w", database="system")
        self._session.run("DROP DATABASE testa IF EXISTS").consume()
        self._session.run("DROP DATABASE testb IF EXISTS").consume()
        bookmarks = self._session.last_bookmarks()
        self._session.close()
        self._session = self._driver.session("w", database="system",
                                             bookmarks=bookmarks)
        result = self._session.run("SHOW DATABASES")
        self.assertEqual(get_names(result, node=False), {"system", "neo4j"})

        result = self._session.run("CREATE DATABASE testa")
        result.consume()
        result = self._session.run("CREATE DATABASE testb")
        result.consume()
        bookmarks = self._session.last_bookmarks()
        self._session.close()

        self._session = self._driver.session("w", database="testa",
                                             bookmarks=bookmarks)
        result = self._session.run('CREATE (p:Person {name: "ALICE"})')
        result.consume()
        self._session.close()

        self._session = self._driver.session("w", database="testb")
        result = self._session.run('CREATE (p:Person {name: "BOB"})')
        result.consume()
        self._session.close()

        self._session = self._driver.session("w")
        # Test that default database is still empty
        result = self._session.run("MATCH (p:Person) RETURN p")
        self.assertIsInstance(result.next(), types.NullRecord)
        self._session.close()

        self._session = self._driver.session("w", database="testa")
        result = self._session.run("MATCH (p:Person) RETURN p")
        self.assertEqual(get_names(result), {"ALICE"})
        self._session.close()

        self._session = self._driver.session("w", database="testb")
        result = self._session.run("MATCH (p:Person) RETURN p")
        self.assertEqual(get_names(result), {"BOB"})
        self._session.close()

        self._session = self._driver.session("w", database="system")
        self._session.run("DROP DATABASE testa IF EXISTS").consume()
        self._session.close()

        self._session = self._driver.session("w", database="system")
        self._session.run("DROP DATABASE testb IF EXISTS").consume()
        self._session.close()

        self._session = self._driver.session("w")
        self._session.run("MATCH (n) DETACH DELETE n").consume()
