import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.tls.shared import TlsServer


schemes = ("bolt", "neo4j")


class TestExplicitSslOptions(TestkitTestCase):
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

    @driver_feature(types.Feature.API_SSL_SCHEMES, types.Feature.API_SSL_CONFIG)
    def test_explicit_config_and_scheme_config(self):
        def _test():
            url = "%s://%s:%d" % (scheme, "thehost", 6666)
            auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                            credentials="pass")
            with self.assertRaises(types.DriverError) as exc:
                Driver(self._backend, url, auth, encrypted=encrypted,
                       trustedCertificates=certs)
            if get_driver_name() in ["javascript"]:
                self.assertIs("encryption", exc.exception.msg.lower())
                self.assertIs("trust", exc.exception.msg.lower())
            else:
                self.fail("Add expected error type for driver.")

        self._server = TlsServer("trustedRoot_thehost")
        for scheme in ("neo4j+s", "neo4j+ssc", "bolt+s", "bolt+ssc"):
            for encrypted in (True, False):
                for certs in ("None", [], ["customRoot.crt"]):
                    with self.subTest("%s-%s-%s" % (scheme, encrypted, certs)):
                        _test()

