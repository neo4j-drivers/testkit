import os

import nutkit.protocol as types
from tests.shared import get_driver_name
from tests.tls.shared import (
    TestkitTlsTestCase,
    TlsServer,
)

THIS_PATH = os.path.dirname(os.path.abspath(__file__))


class TestClientCertificate(TestkitTlsTestCase):
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

    client_cert_on_server = os.path.join(THIS_PATH,
                                         "certs", "server", "bolt", "trusted",
                                         "client.pem")

    client_certificate_cert = os.path.join(THIS_PATH,
                                           "certs", "driver",
                                           "certificate.pem")
    client_certificate_key = os.path.join(THIS_PATH,
                                          "certs", "driver",
                                          "privatekey.pem")

    client_certificate_key_with_pwd = \
        os.path.join(THIS_PATH,
                     "certs", "driver",
                     "privatekey_with_thepassword.pem")

    client_certificate_password = "thepassword"

    required_features = types.Feature.API_SSL_SCHEMES, \
        types.Feature.API_SSL_CLIENT_CERTIFICATE

    def test_s_and_client_certificate_present(self):
        schemes = "neo4j+s", "bolt+s"
        client_certificates = (self._get_client_certificate(),
                               self._get_client_certificate_with_password())
        for client_certificate in client_certificates:
            for scheme in schemes:
                with self.subTest(scheme=scheme,
                                  client_certificate=client_certificate):
                    self._start_server("trustedRoot_thehost",
                                       client_cert=self.client_cert_on_server)
                    self.assertTrue(self._try_connect(
                        self._server, scheme, "thehost",
                        client_certificate=client_certificate,
                    ))
                self._server.reset()

    def test_s_and_certificate_not_present(self):
        schemes = "neo4j+s", "bolt+s"
        for scheme in schemes:
            with self.subTest(scheme=scheme):
                self._start_server("trustedRoot_thehost",
                                   client_cert=self.client_cert_on_server)
                self.assertFalse(self._try_connect(
                    self._server, scheme, "thehost"
                ))
            self._server.reset()

    def test_ssc_and_client_certificate_present(self):
        schemes = "neo4j+ssc", "bolt+ssc"
        client_certificates = (self._get_client_certificate(),
                               self._get_client_certificate_with_password())
        for client_certificate in client_certificates:
            for scheme in schemes:
                with self.subTest(scheme=scheme,
                                  client_certificate=client_certificate):
                    self._start_server("trustedRoot_thehost",
                                       client_cert=self.client_cert_on_server)
                    self.assertTrue(self._try_connect(
                        self._server, scheme, "thehost",
                        client_certificate=client_certificate,
                    ))
                self._server.reset()

    def test_scc_and_certificate_not_present(self):
        schemes = "neo4j+ssc", "bolt+ssc"
        for scheme in schemes:
            with self.subTest(scheme=scheme):
                self._start_server("trustedRoot_thehost",
                                   client_cert=self.client_cert_on_server)
                self.assertFalse(self._try_connect(
                    self._server, scheme, "thehost"
                ))
            self._server.reset()

    def _start_server(self, cert, **kwargs):
        self._server = TlsServer(cert, **kwargs)

    def _get_client_certificate(self):
        return types.ClientCertificate(self.client_certificate_cert,
                                       self.client_certificate_key)

    def _get_client_certificate_with_password(self):
        return types.ClientCertificate(self.client_certificate_cert,
                                       self.client_certificate_key_with_pwd,
                                       self.client_certificate_password)
