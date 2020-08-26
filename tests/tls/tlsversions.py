import unittest
from tests.shared import *
from tests.tls.shared import *


class TestTlsVersions(unittest.TestCase):

    def setUp(self):
        self._backend = new_backend()
        self._server = None
        self._driver = get_driver_name()

    def tearDown(self):
        if self._server:
            # If test raised an exception this will make sure that the stub server
            # is killed and it's output is dumped for analys.
            self._server.reset()
            self._server = None
        self._backend.close()

    def test_1_1(self):
        self.skipTest("Test not implemented")

    def test_1_2(self):
        self.skipTest("Test not implemented")

