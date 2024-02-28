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
        "client_certificate": (client_certificate_cert, client_certificate_key)
    },

    def test_driver_is_encrypted_with_ssc(self):
        schemes = "neo4j+ssc", "bolt+ssc"
        self._server = TlsServer("trustedRoot_thehost",
                                 client_cert=self.client_cert_on_server)
        for driver_config in self.extra_driver_configs:
            for scheme in schemes:
                with self.subTest(scheme=scheme, driver_config=driver_config):
                    self._test_reports_encrypted(True, scheme, **driver_config)
