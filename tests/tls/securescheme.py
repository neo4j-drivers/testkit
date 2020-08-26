import unittest
from tests.shared import *
from tests.tls.shared import *

schemes = ["neo4j+s", "bolt+s"]

class TestSecureScheme(unittest.TestCase):
    """ Tests URL scheme neo4j+s/bolt+s where server is assumed to present a server certificate
    signed by a certificate authority recognized by the driver.
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

    """ Happy path, the server has a valid server certificate signed by a trusted
    certificate authority.
    """
    def test_trusted_ca_correct_hostname(self):
        if self._driver in ["dotnet"]:
            self.skipTest("No support for installing CAs in docker image")

        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("trustedRoot_thehost")
                self.assertTrue(try_connect(self._backend, self._server, scheme, "thehost"))

    """ The certificate authority is ok, hostname is ok but the server certificate has expired.
    Should not connect on expired certificate.
    """
    def test_trusted_ca_expired_server_correct_hostname(self):
        if self._driver in ["dotnet"]:
            self.skipTest("No support for installing CAs in docker image")

        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("trustedRoot_thehost_expired")
                self.assertFalse(try_connect(self._backend, self._server, scheme, "thehost"))

    """ Verifies that driver rejects connect if hostnames doesn't match
    """
    def test_trusted_ca_wrong_hostname(self):
        if self._driver in ["dotnet"]:
            self.skipTest("No support for installing CAs in docker image")

        # TLS server is setup to serve under the name 'thehost' but driver will connect
        # to this server using 'thehostbutwrong'. Note that the docker container must
        # map this hostname to same IP as 'thehost', if this hasn't been done we won't
        # connect (expected) but get a timeout instead since the TLS server hasn't received
        # any connect attempt at all.
        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("trustedRoot_thehost")
                self.assertFalse(try_connect(self._backend, self._server, scheme, "thehostbutwrong"))

    """ should not connect """
    def test_untrusted_ca_correct_hostname(self):
        if self._driver in ["dotnet"]:
            self.skipTest("No support for installing CAs in docker image")

        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("untrustedRoot_thehost_expired")
                self.assertFalse(try_connect(self._backend, self._server, scheme, "thehost"))

    """ Should not connect """
    def test_unencrypted(self):
        self.skipTest("Test not implemented")

