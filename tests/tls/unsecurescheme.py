import unittest
from tests.shared import *
from tests.tls.shared import *


class TestUnsecureScheme(unittest.TestCase):
    """ Tests URL scheme neo4j/bolt where TLS is not used. The fact that driver can not connect
    to a TLS server with this configuration is less interesting than the error handling when
    this happens, the driver backend should "survive" (without special hacks in it).
    """
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

    def test_secure_server(self):
        self._server = TlsServer("trustedRoot_thehost")
        self.assertFalse(try_connect(self._backend, self._server, "neo4j", "thehost"))

