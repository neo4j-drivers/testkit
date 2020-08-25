
import unittest
from tests.shared import *
from tests.tls.shared import *


class TestCASigned(unittest.TestCase):
    """ Tests URL scheme neo4j+s where server is assumed to present a server certificate
    signed by a certificate authority recognized by the driver.
    """
    def setUp(self):
        self._backend = new_backend()
        self._server = None
        self._driver = get_driver_name()
        # Doesn't really matter
        #self._auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
        #self._scheme = "neo4j+s://%s:%d" % ("thehost", 6666)


    def tearDown(self):
        if self._server:
            # If test raised an exception this will make sure that the stub server
            # is killed and it's output is dumped for analys.
            self._server.reset()
            self._server = None
        self._backend.close()

    """ Happy path, the server has a valid server certificate signed by a trusted
    certificate authority.
    """
    def test_connect_trusted_ca(self):
        if self._driver in ["dotnet"]:
            self.skipTest("No support for installing CAs in docker image")

        self._server = TlsServer("trustedRoot_thehost")
        self.assertTrue(try_connect(self._backend, self._server, "neo4j+s", "thehost"))

    """ Verifies that driver rejects connect if hostnames doesn't match
    """
    def test_connect_wrong_hostname(self):
        if self._driver in ["dotnet"]:
            self.skipTest("No support for installing CAs in docker image")

        # TLS server is setup to serve under the name 'thehost' but driver will connect
        # to this server using 'thehostbutwrong'. Note that the docker container must
        # map this hostname to same IP as 'thehost', if this hasn't been done we won't
        # connect (expected) but get a timeout instead since the TLS server hasn't received
        # any connect attempt at all.
        self._server = TlsServer("trustedRoot_thehost")
        self.assertFalse(try_connect(self._backend, self._server, "neo4j+s", "thehostbutwrong"))

