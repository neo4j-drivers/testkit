from nutkit import protocol as types
from nutkit.frontend import Driver
from ..shared import (
    driver_feature,
    dns_resolve_single,
    get_driver_name,
    TestkitTestCase,
)
from .shared import (
    get_authorization,
    get_driver,
    get_neo4j_host_and_http_port,
    get_neo4j_host_and_port,
    get_neo4j_scheme,
    get_server_info,
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
                                  resolverFn=my_resolver,
                                  connectionTimeoutMs=200)
        self._session = self._driver.session("r")
        result = self._session.run("RETURN 1")
        summary = result.consume()

        self.assertEqual(summary.server_info.address, "%s:%d" % (host, port))
        self.assertEqual(resolved_addresses, ["*:7687"])

    def test_fail_nicely_when_using_http_port(self):
        # TODO add support and remove this block
        if get_driver_name() in ['go']:
            self.skipTest("verifyConnectivity not implemented in backend")
        scheme = get_neo4j_scheme()
        host, port = get_neo4j_host_and_http_port()
        uri = "%s://%s:%d" % (scheme, host, port)
        self._driver = get_driver(self._backend, uri=uri)
        with self.assertRaises(types.DriverError) as e:
            self._driver.verifyConnectivity()
        if get_driver_name() in ["python"]:
            self.assertEqual(e.exception.errorType,
                             "<class 'neo4j.exceptions.ServiceUnavailable'>")

    def test_should_fail_on_incorrect_password(self):
        uri = "%s://%s:%d" % (get_neo4j_scheme(), *get_neo4j_host_and_port())
        auth = get_authorization()
        auth.credentials = auth.credentials + "-but-wrong!"
        self._driver = Driver(self._backend, uri, auth)
        self._session = self._driver.session("w")

        with self.assertRaises(types.DriverError) as e:
            self._session.run("RETURN 1").consume()

        if get_driver_name() in ["python"]:
            self.assertEqual(e.exception.errorType,
                             "<class 'neo4j.exceptions.AuthError'>")

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_supports_multi_db(self):
        self._driver = get_driver(self._backend)
        self._session = self._driver.session("w")
        summary = self._session.run("RETURN 1 as n").consume()
        result = self._driver.supportsMultiDB()
        server_version = tuple(map(int, get_server_info().version.split(".")))

        if server_version in ((4, 0), (4, 1), (4, 2), (4, 3)):
            self.assertTrue(result)
            # This is the default database name if not set explicitly on the
            # Neo4j Server
            self.assertEqual(summary.database, "neo4j")
        elif server_version == (3, 5):
            self.assertFalse(result)
            self.assertIsNone(summary.database)
        else:
            self.fail("Unexpected server version %s" % get_server_info())

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
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            # does not set exception code or message
            return
        self.assertEqual(exc.code,
                         "Neo.ClientError.Database.DatabaseNotFound")
        # TODO remove this block once all languages work
        if get_driver_name() in ["java"]:
            # does not set exception message
            return
        self.assertIn("test-database", exc.msg)
        self.assertIn("exist", exc.msg)
        if get_driver_name() in ["python"]:
            self.assertEqual(exc.errorType,
                             "<class 'neo4j.exceptions.ClientError'>")

    @driver_feature(types.Feature.TMP_FULL_SUMMARY)
    def test_multi_db(self):
        self._driver = get_driver(self._backend)
        if not get_server_info().supports_multi_db:
            self.skipTest("Needs multi DB support")
        self._session = self._driver.session("w", database="system")

        self._session.run("DROP DATABASE `test-database` IF EXISTS").consume()
        self._session.run("CREATE DATABASE `test-database`").consume()
        self._session.close()

        self._session = self._driver.session("r", database="test-database")
        result = self._session.run("RETURN 1")
        summary = result.consume()
        self.assertEqual(summary.database, "test-database")

        self._session.close()
        self._session = self._driver.session("w", database="system")
        self._session.run("DROP DATABASE `test-database` IF EXISTS").consume()
