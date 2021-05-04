from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer

# Low-level network transport tests
class TestTransport(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()
        auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri, auth)
        self._session = self._driver.session("w")

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def test_noop(self):
        # Verifies that no op messages sent on bolt chunking layer are ignored.
        # The no op messages are sent from server as a way to notify that the
        # connection is still up.
        # Bolt 4.1 >
        bolt_version = "4.1"
        self._server.start(path=self.script_path("reader_with_noops.script"),
                           vars={"#BOLT_VERSION#": bolt_version})
        result = self._session.run("RETURN 1 as n")
        record = result.next()
        null_record = result.next()
        self._driver.close()
        self._server.done()

        # Verify the result
        # Indirectly verifies that we got a record
        self.assertEqual(record.values[0].value, 1)
        self.assertIsInstance(null_record, types.NullRecord)

