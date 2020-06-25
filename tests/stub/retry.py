import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


class TestRetry(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)

    def tearDown(self):
        self._backend.close()
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analys.
        self._server.reset()

    def test_read(self):
        self._server.start(os.path.join(scripts_path, "retry_read.script"))

        def retry_once(tx):
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
        driver = Driver(self._backend, "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.readTransaction(retry_once)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)

        session.close()
        driver.close()
        self._server.done()


