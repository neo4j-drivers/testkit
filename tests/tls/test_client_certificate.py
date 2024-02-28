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

    required_features = types.Feature.API_SSL_SCHEMES,
    extra_driver_configs = {
    },

    def test_ssc_and_client_certificate_present(self):
        schemes = "neo4j+ssc", "bolt+ssc"
        for driver_config in self.extra_driver_configs:
            for scheme in schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._start_server("trustedRoot_thehost",
                                       client_cert=self.client_cert_on_server)
                    self.assertTrue(self._try_connect(
                        self._server, scheme, "thehost",
                        client_certificate=self._get_client_certificate(),
                        **driver_config
                    ))
                self._server.reset()

    def test_scc_and_certificate_not_present(self):
        schemes = "neo4j+ssc", "bolt+ssc"
        for driver_config in self.extra_driver_configs:
            for scheme in schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._start_server("trustedRoot_thehost",
                                       client_cert=self.client_cert_on_server)
                    self.assertFalse(self._try_connect(
                        self._server, scheme, "thehost", **driver_config
                    ))
                self._server.reset()

    def _start_server(self, cert, **kwargs):
        self._server = TlsServer(cert, **kwargs)

    def _get_client_certificate(self):
        return (self.client_certificate_cert, self.client_certificate_key)
