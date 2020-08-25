import unittest
from tests.shared import *
from tests.tls.shared import *


class TestSelfSigned(unittest.TestCase):
    """ Tests URL scheme neo4j+ssc where server is assumed to present a signed server certificate
    but not necessarily signed by an authority recognized by the driver.
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

    """ A server certificate signed by a trusted CA should be accepted even when configured
    for self signed.
    """
    def test_trusted_ca_correct_hostname(self):
        if self._driver in ["dotnet"]:
            self.skipTest("No support for installing CAs in docker image")

        self._server = TlsServer("trustedRoot_thehost")
        self.assertTrue(try_connect(self._backend, self._server, "neo4j+ssc", "thehost"))

    """ should not connect """
    def test_trusted_ca_expired_server_correct_hostname(self):
        self.skipTest("Test not implemented")

    """ A server certificate signed by a trusted CA but with wrong hostname will still be
    accepted.
    """
    def test_trusted_ca_wrong_hostname(self):
        if self._driver in ["dotnet"]:
            self.skipTest("No support for installing CAs in docker image")

        # TLS server is setup to serve under the name 'thehost' but driver will connect
        # to this server using 'thehostbutwrong'. Note that the docker container must
        # map this hostname to same IP as 'thehost', if this hasn't been done we won't
        # connect (expected) but get a timeout instead since the TLS server hasn't received
        # any connect attempt at all.
        self._server = TlsServer("trustedRoot_thehost")
        self.assertTrue(try_connect(self._backend, self._server, "neo4j+ssc", "thehostbutwrong"))

    """ Should connect """
    def test_untrusted_ca_correct_hostname(self):
        self.skipTest("Test not implemented")

    """ Should connect """
    def test_untrusted_ca_wrong_hostname(self):
        self.skipTest("Test not implemented")


