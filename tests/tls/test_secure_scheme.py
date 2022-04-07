import nutkit.protocol as types
from tests.shared import get_driver_name
from tests.tls.shared import (
    TestkitTlsTestCase,
    TlsServer,
)


class TestSecureScheme(TestkitTlsTestCase):
    # Tests URL scheme neo4j+s/bolt+s where server is assumed to present a
    # server certificate signed by a certificate authority recognized by the
    # driver.

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

    schemes = "neo4j+s", "bolt+s"
    required_features = types.Feature.API_SSL_SCHEMES,
    extra_driver_configs = {},
    cert_prefix = "trustedRoot_"

    def _start_server(self, cert_suffix, **kwargs):
        if "Root_" not in cert_suffix:
            cert = self.cert_prefix + cert_suffix
        else:
            cert = cert_suffix
        self._server = TlsServer(cert, **kwargs)

    def test_driver_is_encrypted(self):
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._test_reports_encrypted(True, scheme, **driver_config)

    def test_trusted_ca_correct_hostname(self):
        # Happy path, the server has a valid server certificate signed by a
        # trusted certificate authority.
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._start_server("thehost")
                    self.assertTrue(self._try_connect(
                        self._server, scheme, "thehost", **driver_config
                    ))
                self._server.reset()

    def test_trusted_ca_expired_server_correct_hostname(self):
        # The certificate authority is ok, hostname is ok but the server
        # certificate has expired. Should not connect on expired certificate.
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._start_server("thehost_expired")
                    self.assertFalse(self._try_connect(
                        self._server, scheme, "thehost", **driver_config
                    ))
                self._server.reset()

    def test_trusted_ca_wrong_hostname(self):
        # Verifies that driver rejects connection if host name doesn't match
        # TLS server is setup to serve under the name 'thehost' but driver will
        # connect to this server using 'thehostbutwrong'. Note that the docker
        # container must map this hostname to same IP as 'thehost', if this
        # hasn't been done we won't connect (expected) but get a timeout
        # instead since the TLS server hasn't received any connect attempt at
        # all.
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._start_server("thehost")
                    self.assertFalse(self._try_connect(
                        self._server, scheme, "thehostbutwrong",
                        **driver_config
                    ))
                self._server.reset()

    def test_untrusted_ca_correct_hostname(self):
        # Verifies that driver rejects connection if host name doesn't match
        # trusted
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._server = TlsServer("untrustedRoot_thehost")
                    self.assertFalse(self._try_connect(
                        self._server, scheme, "thehost", **driver_config
                    ))
                self._server.reset()

    def test_unencrypted(self):
        # Verifies that driver doesn't connect when it has been configured for
        # TLS connections but the server doesn't speak TLS
        for driver_config in self.extra_driver_configs:
            for scheme in self.schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    # The server cert doesn't really matter but set it to the
                    # one that would work if TLS happens to be on.
                    self._start_server("thehost", disable_tls=True)
                    self.assertFalse(self._try_connect(
                        self._server, scheme, "thehost", **driver_config
                    ))
                self._server.reset()


class TestTrustSystemCertsConfig(TestSecureScheme):
    schemes = "neo4j", "bolt"
    required_features = types.Feature.API_SSL_CONFIG,
    extra_driver_configs = (
        {"encrypted": True, "trusted_certificates": "None"},
        {"encrypted": True},
    )

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

    def test_unencrypted(self):
        super().test_unencrypted()


class TestTrustCustomCertsConfig(TestTrustSystemCertsConfig):
    extra_driver_configs = (
        {"encrypted": True, "trusted_certificates": ["customRoot.crt"]},
        {"encrypted": True,
         "trusted_certificates": ["customRoot2.crt", "customRoot.crt"]},
        {"encrypted": True,
         "trusted_certificates": ["customRoot.crt", "customRoot2.crt"]},
    )
    cert_prefix = "customRoot_"

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

    def test_unencrypted(self):
        super().test_unencrypted()
