from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.tls.shared import (
    TlsServer,
    try_connect,
)


schemes = ["neo4j+ssc", "bolt+ssc"]


class TestSelfSignedScheme(TestkitTestCase):
    """ Tests URL scheme neo4j+ssc/bolt+ssc where server is assumed to present a signed server certificate
    but not necessarily signed by an authority recognized by the driver.
    """
    def setUp(self):
        super().setUp()
        self._server = None
        self._driver = get_driver_name()

    def tearDown(self):
        if self._server:
            # If test raised an exception this will make sure that the stub
            # server is killed and it's output is dumped for analysis.
            self._server.reset()
            self._server = None
        super().tearDown()

    """ A server certificate signed by a trusted CA should be accepted even when configured
    for self signed.
    """
    def test_trusted_ca_correct_hostname(self):
        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("trustedRoot_thehost")
                self.assertTrue(try_connect(self._backend, self._server, scheme, "thehost"))

    """ A server certificate signed by a trusted CA but the certificate has expired.
    Go driver happily connects when InsecureSkipVerify is enabled, same for all drivers ?
    """
    def test_trusted_ca_expired_server_correct_hostname(self):

        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("trustedRoot_thehost_expired")
                self.assertTrue(try_connect(self._backend, self._server, scheme, "thehost"))

    """ A server certificate signed by a trusted CA but with wrong hostname will still be
    accepted.
    """
    def test_trusted_ca_wrong_hostname(self):
        # TLS server is setup to serve under the name 'thehost' but driver will connect
        # to this server using 'thehostbutwrong'. Note that the docker container must
        # map this hostname to same IP as 'thehost', if this hasn't been done we won't
        # connect (expected) but get a timeout instead since the TLS server hasn't received
        # any connect attempt at all.
        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("trustedRoot_thehost")
                self.assertTrue(try_connect(self._backend, self._server, scheme, "thehostbutwrong"))

    """ Should connect """
    def test_untrusted_ca_correct_hostname(self):

        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("untrustedRoot_thehost")
                self.assertTrue(try_connect(self._backend, self._server, scheme, "thehost"))

    """ Should connect """
    def test_untrusted_ca_wrong_hostname(self):
        for scheme in schemes:
            with self.subTest(scheme):
                self._server = TlsServer("untrustedRoot_thehost")
                self.assertTrue(try_connect(self._backend, self._server, scheme, "thehostbutwrong"))

    """ Verifies that driver doesn't connect when it has been configured for TLS connections
    but the server doesn't speak TLS
    """
    def test_unencrypted(self):
        for scheme in schemes:
            with self.subTest(scheme):
                # The server cert doesn't really matter but set it to the one that would work
                # if TLS happens to be on.
                self._server = TlsServer("untrustedRoot_thehost", disableTls=True)
                self.assertFalse(try_connect(self._backend, self._server, scheme, "thehost"))

