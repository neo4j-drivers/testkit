import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


class TestRetry(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()

    def tearDown(self):
        self._backend.close()
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analys.
        stub_9001.reset()

    def test_read(self):
        stub_9001.start(os.path.join(scripts_path, "retry_read.script"))

        def retry_once(tx):
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
        driver = Driver(self._backend, "bolt://%s" % stub_9001.address, auth)
        session = driver.session("r")
        x = session.readTransaction(retry_once)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)

        session.close()
        driver.close()
        stub_9001.done()


