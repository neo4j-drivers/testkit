import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


class TestRetry(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)
        self._driver = get_driver_name()

    def tearDown(self):
        self._backend.close()
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analys.
        self._server.reset()

    def test_read(self):
        if self._driver in ["dotnet"]:
            self.skipTest("Transactional functions not implemented in backend")

        script = "retry_read.script"
        if self._driver in ["go"]:
            # Until Go is updated to use PULL with n
            script = "retry_read_v3.script"
        self._server.start(os.path.join(scripts_path, script))

        num_retries = 0
        def retry_once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
        driver = Driver(self._backend, "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.readTransaction(retry_once)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 1)

        session.close()
        driver.close()
        self._server.done()

    def test_read_twice(self):
        if self._driver in ["dotnet"]:
            self.skipTest("Transactional functions not implemented in backend")

        script = "retry_read_twice.script"
        if self._driver in ["go"]:
            # Until Go is updated to use PULL with n
            # Lean version has fewer resets
            script = "retry_read_twice_lean_v3.script"
        self._server.start(os.path.join(scripts_path, script))

        num_retries = 0
        def retry_twice(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
        driver = Driver(self._backend, "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.readTransaction(retry_twice)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 2)

        session.close()
        driver.close()
        self._server.done()

