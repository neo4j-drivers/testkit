import os

import nutkit.protocol as types
from nutkit.frontend import (
    ClientCertificateHolder,
    ClientCertificateProvider,
)
from tests.shared import get_driver_name
from tests.tls.shared import (
    TestkitTlsTestCase,
    TlsServer,
)

THIS_PATH = os.path.dirname(os.path.abspath(__file__))


class _TestClientCertificateBase(TestkitTlsTestCase):
    required_features = (types.Feature.API_SSL_SCHEMES,
                         types.Feature.API_SSL_CLIENT_CERTIFICATE)

    def setUp(self):
        super().setUp()
        self._server = None
        self._driver = get_driver_name()

    def tearDown(self):
        if self._server:
            # If test raised an exception this will make sure that the stub
            # server is killed, and its output is dumped for analysis.
            self._server.reset()
            self._server = None
        super().tearDown()

    def _start_server(self, cert, **kwargs):
        self._server = TlsServer(cert, **kwargs)

    @classmethod
    def _client_cert_on_server(cls, i=1):
        return os.path.join(
            THIS_PATH, "certs", "server", "bolt", "trusted", f"client{i}.pem"
        )

    @classmethod
    def _client_certificate_cert(cls, i=1):
        return os.path.join(
            THIS_PATH, "certs", "driver", f"certificate{i}.pem"
        )

    @classmethod
    def _client_certificate_key(cls, i=1):
        return os.path.join(
            THIS_PATH, "certs", "driver", f"privatekey{i}.pem"
        )

    @classmethod
    def _client_certificate_key_with_pwd(cls, i=1):
        return os.path.join(
            THIS_PATH, "certs", "driver",
            f"privatekey{i}_with_thepassword{i}.pem"
        )

    @classmethod
    def _client_certificate_password(cls, i=1):
        return f"thepassword{i}"

    @classmethod
    def _get_client_certificate(cls, i=1):
        return types.ClientCertificate(
            cls._client_certificate_cert(i),
            cls._client_certificate_key(i)
        )

    @classmethod
    def _get_client_certificate_with_password(cls, i=1):
        return types.ClientCertificate(
            cls._client_certificate_cert(i),
            cls._client_certificate_key_with_pwd(i),
            cls._client_certificate_password(i)
        )


class TestClientCertificate(_TestClientCertificateBase):
    def test_s_and_client_certificate_present(self):
        schemes = ("neo4j+s", "bolt+s")
        client_certificates = (self._get_client_certificate(),
                               self._get_client_certificate_with_password())
        for client_certificate in client_certificates:
            for scheme in schemes:
                with self.subTest(scheme=scheme,
                                  client_certificate=client_certificate):
                    self._start_server(
                        "trustedRoot_thehost",
                        client_cert=self._client_cert_on_server()
                    )
                    with self._make_driver(
                        scheme, "thehost",
                        client_certificate=client_certificate
                    ) as driver:
                        self.assertTrue(
                            self._try_connect(self._server, driver)
                        )
                if self._server:
                    self._server.reset()

    def test_s_and_certificate_not_present(self):
        schemes = ("neo4j+s", "bolt+s")
        for scheme in schemes:
            with self.subTest(scheme=scheme):
                self._start_server(
                    "trustedRoot_thehost",
                    client_cert=self._client_cert_on_server()
                )
                with self._make_driver(scheme, "thehost") as driver:
                    self.assertFalse(self._try_connect(
                        self._server, driver
                    ))
            if self._server:
                self._server.reset()

    def test_ssc_and_client_certificate_present(self):
        schemes = ("neo4j+ssc", "bolt+ssc")
        client_certificates = (self._get_client_certificate(),
                               self._get_client_certificate_with_password())
        for client_certificate in client_certificates:
            for scheme in schemes:
                with self.subTest(scheme=scheme,
                                  client_certificate=client_certificate):
                    self._start_server(
                        "trustedRoot_thehost",
                        client_cert=self._client_cert_on_server()
                    )
                    with self._make_driver(
                        scheme, "thehost",
                        client_certificate=client_certificate
                    ) as driver:
                        self.assertTrue(
                            self._try_connect(self._server, driver)
                        )
                if self._server:
                    self._server.reset()

    def test_scc_and_certificate_not_present(self):
        schemes = ("neo4j+ssc", "bolt+ssc")
        for scheme in schemes:
            with self.subTest(scheme=scheme):
                self._start_server(
                    "trustedRoot_thehost",
                    client_cert=self._client_cert_on_server()
                )
                with self._make_driver(scheme, "thehost") as driver:
                    self.assertFalse(
                        self._try_connect(self._server, driver)
                    )
            if self._server:
                self._server.reset()


class TestClientCertificateRotation(_TestClientCertificateBase):
    required_features = (
        *_TestClientCertificateBase.required_features,
        types.Feature.API_LIVENESS_CHECK,
    )

    def test_client_rotation(self):
        cert_calls = 0
        cert_holder = ClientCertificateHolder(self._get_client_certificate(1))

        def get_cert() -> ClientCertificateHolder:
            nonlocal cert_calls, cert_holder
            cert_calls += 1
            has_update = cert_holder.has_update
            cert_holder.has_update = False
            return ClientCertificateHolder(cert_holder.cert, has_update)

        cert_provider = ClientCertificateProvider(self._backend, get_cert)

        with self._make_driver(
            "bolt+s", "thehost", client_certificate=cert_provider
        ) as driver:
            for i in (1, 2):
                for _ in (1, 2):
                    self._start_server(
                        "trustedRoot_thehost",
                        client_cert=self._client_cert_on_server(i=i)
                    )
                    self.assertTrue(self._try_connect(
                        self._server, driver
                    ))
                    self._server.reset()
                    self.assertEqual(1, cert_calls)
                    cert_calls = 0

                cert_holder = ClientCertificateHolder(
                    self._get_client_certificate(i + 1)
                )
