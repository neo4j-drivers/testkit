import nutkit.protocol as types
from tests.shared import get_driver_name
from tests.tls.shared import (
    TestkitTlsTestCase,
    TlsServer,
)


class TestSelfSignedScheme(TestkitTlsTestCase):
    # Tests URL scheme neo4j+ssc/bolt+ssc where server is assumed to present a
    # signed server certificate but not necessarily signed by an authority
    # recognized by the driver.
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

    schemes = "neo4j+ssc", "bolt+ssc"
    required_features = types.Feature.API_SSL_SCHEMES,
    extra_driver_configs = {},

    def test_driver_is_encrypted(self):
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._test_reports_encrypted(True, scheme, **driver_config)

    def test_trusted_ca_correct_hostname(self):
        # A server certificate signed by a trusted CA should be accepted even
        # when configured for self signed.
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._server = TlsServer("trustedRoot_thehost")
                    with self._make_driver(
                        scheme, "thehost", **driver_config
                    ) as driver:
                        self.assertTrue(self._try_connect(
                            self._server, driver
                        ))
                if self._server is not None:
                    self._server.reset()

    def test_trusted_ca_expired_server_correct_hostname(self):
        # A server certificate signed by a trusted CA but the certificate has
        # expired. Go driver happily connects when InsecureSkipVerify is
        # enabled, same for all drivers ?
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._server = TlsServer("trustedRoot_thehost_expired")
                    with self._make_driver(
                        scheme, "thehost", **driver_config
                    ) as driver:
                        self.assertTrue(self._try_connect(
                            self._server, driver
                        ))
                if self._server is not None:
                    self._server.reset()

    def test_trusted_ca_wrong_hostname(self):
        # A server certificate signed by a trusted CA but with wrong hostname
        # will still be accepted.
        # TLS server is setup to serve under the name 'thehost' but driver will
        # connect to this server using 'thehostbutwrong'. Note that the docker
        # container must map this hostname to same IP as 'thehost', if this
        # hasn't been done we won't connect (expected) but get a timeout
        # instead since the TLS server hasn't received any connect attempt at
        # all.
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._server = TlsServer("trustedRoot_thehost")
                    with self._make_driver(
                        scheme, "thehostbutwrong", **driver_config
                    ) as driver:
                        self.assertTrue(self._try_connect(
                            self._server, driver
                        ))
                if self._server is not None:
                    self._server.reset()

    def test_untrusted_ca_correct_hostname(self):
        # Should connect
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._server = TlsServer("untrustedRoot_thehost")
                    with self._make_driver(
                        scheme, "thehost", **driver_config
                    ) as driver:
                        self.assertTrue(self._try_connect(
                            self._server, driver
                        ))
                if self._server is not None:
                    self._server.reset()

    def test_untrusted_ca_wrong_hostname(self):
        # Should connect
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._server = TlsServer("untrustedRoot_thehost")
                    with self._make_driver(
                        scheme, "thehostbutwrong", **driver_config
                    ) as driver:
                        self.assertTrue(self._try_connect(
                            self._server, driver
                        ))
                if self._server is not None:
                    self._server.reset()

    def test_unencrypted(self):
        # Verifies that driver doesn't connect when it has been configured for
        # TLS connections but the server doesn't speak TLS
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    # The server cert doesn't really matter but set it to the
                    # one that would work if TLS happens to be on.
                    self._server = TlsServer("untrustedRoot_thehost",
                                             disable_tls=True)
                    with self._make_driver(
                        scheme, "thehost", **driver_config
                    ) as driver:
                        self.assertFalse(self._try_connect(
                            self._server, driver
                        ))
                if self._server is not None:
                    self._server.reset()


class TestTrustAllCertsConfig(TestSelfSignedScheme):
    schemes = "neo4j", "bolt"
    required_features = types.Feature.API_SSL_CONFIG,
    extra_driver_configs = {"encrypted": True, "trusted_certificates": []},

    def test_driver_is_encrypted(self):
        super().test_driver_is_encrypted()

    def test_trusted_ca_correct_hostname(self):
        super().test_trusted_ca_correct_hostname()

    def test_trusted_ca_expired_server_correct_hostname(self):
        super().test_trusted_ca_expired_server_correct_hostname()

    def test_trusted_ca_wrong_hostname(self):
        super().test_trusted_ca_wrong_hostname()

    def test_untrusted_ca_correct_hostname(self):
        super().test_untrusted_ca_correct_hostname()

    def test_untrusted_ca_wrong_hostname(self):
        super().test_untrusted_ca_wrong_hostname()

    def test_unencrypted(self):
        super().test_unencrypted()
