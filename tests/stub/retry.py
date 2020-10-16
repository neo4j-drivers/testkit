import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


script_read = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET
!: AUTO GOODBYE

C: BEGIN {"mode": "r"}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
"""

script_retry = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE

C: BEGIN {"mode": "r"}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
C: COMMIT
S: FAILURE {"code": "Neo.TransientError.Database.DatabaseUnavailable", "message": "<whatever>"}
C: RESET
S: SUCCESS {}
$extra_reset_1
C: BEGIN {"mode": "r"}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
$extra_reset_2
"""

script_commit_disconnect = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET

C: BEGIN {}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "w"}
C: COMMIT
"""

class TestRetry(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()

    def tearDown(self):
        self._backend.close()
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analys.
        self._server.reset()

    def test_read(self):
        self._server.start(script=script_read)
        num_retries = 0
        def once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
        driver = Driver(self._backend, "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.readTransaction(once)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 1)

        session.close()
        driver.close()
        self._server.done()

    def test_read_twice(self):
        # We could probably use AUTO RESET in the script but this makes the diffs more
        # obvious.
        vars = {
            "$extra_reset_1": "",
            "$extra_reset_2": "",
        }
        if self._driverName not in ["go"]:
            vars["$extra_reset_2"] = "C: RESET\nS: SUCCESS {}"
        if self._driverName in ["java", "javascript"]:
            vars["$extra_reset_1"] = "C: RESET\nS: SUCCESS {}"

        self._server.start(script=script_retry, vars=vars)
        num_retries = 0
        def twice(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
        driver = Driver(self._backend, "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.readTransaction(twice)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 2)

        session.close()
        driver.close()
        self._server.done()

    def test_disconnect_on_commit(self):
        # Should NOT retry when connection is lost on unconfirmed commit.
        # The rule could be relaxed on read transactions therefore we test on writeTransaction.
        # An error should be raised to indicate the failure
        if not self._driverName in ["go"]:
            self.skipTest("Backend missing support for SessionWriteTransaction")
        self._server.start(script=script_commit_disconnect)
        num_retries = 0
        def once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
        auth = AuthorizationToken(scheme="basic")
        driver = Driver(self._backend, "bolt://%s" % self._server.address, auth)
        session = driver.session("w")

        with self.assertRaises(types.DriverError) as e: # Check further...
            session.writeTransaction(once)

        self.assertEqual(num_retries, 1)
        session.close()
        driver.close()
        self._server.done()

