from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer

script = """
!: BOLT $bolt_version
!: AUTO RESET
!: AUTO HELLO
!: AUTO GOODBYE

C: RUN "RETURN 1 as n" {} {}
   PULL { "n": 1000}
S: SUCCESS {"fields": ["n"]}
   <NOOP>
   <NOOP>
   RECORD [1]
   <NOOP>
   <NOOP>
   <NOOP>
   SUCCESS {"type": "w"}
"""


# Low-level network transport tests
class Transport(TestkitTestCase):
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
        # Verifies that no op messages sent on bolt chunking layer are ignored. The no op messages
        # are sent from server as a way to notify that the connection is still up.
        # Bolt 4.1 >
        bolt_version = "4.1"
        self._server.start(script=script, vars = {"$bolt_version": bolt_version})
        result = self._session.run("RETURN 1 as n")
        record = result.next()
        nilrec = result.next()
        self._driver.close()
        self._server.done()

        # Verify the result
        self.assertEqual(record.values[0].value, 1) # Indirectly verifies that we got a record
        self.assertIsInstance(nilrec, types.NullRecord)

