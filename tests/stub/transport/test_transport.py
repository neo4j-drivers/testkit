import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


# Low-level network transport tests
class TestTransport(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._driver_name = get_driver_name()
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth)
        self._session = self._driver.session("w")

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    @driver_feature(types.Feature.BOLT_4_4)
    def test_noop(self):
        # Verifies that no op messages sent on bolt chunking layer are ignored.
        # The no op messages are sent from server as a way to notify that the
        # connection is still up.
        # Was introduced with Bolt 4.1, however, that version is legacy and
        # no longer supported by drivers. Therefore, we test with Bolt 4.4.
        bolt_version = "4.4"
        self._server.start(path=self.script_path("reader_with_noops.script"),
                           vars_={"#BOLT_VERSION#": bolt_version})
        result = self._session.run("RETURN 1 AS n")
        record = result.next()
        null_record = result.next()
        self._driver.close()
        self._server.done()

        # Verify the result
        # Indirectly verifies that we got a record
        self.assertEqual(record.values[0].value, 1)
        self.assertIsInstance(null_record, types.NullRecord)

    @driver_feature(types.Feature.BOLT_5_7)
    def test_driver_ignores_feature_flags_handshake_v2(self):
        self._server.start(
            path=self.script_path("handshake_v2_features.script")
        )
        with self.assertRaises(types.DriverError):
            self._session.run("RETURN 1 AN n").consume()
        self._server.done()

    @driver_feature(types.Feature.BOLT_5_7)
    def test_driver_can_negotiate_5_7_with_handshake_v1(self):
        self._server.start(
            path=self.script_path("handshake_v1_bolt_5_7.script")
        )
        with self.assertRaises(types.DriverError):
            self._session.run("RETURN 1 AN n").consume()
        self._server.done()
