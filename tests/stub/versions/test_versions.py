from nutkit.frontend import Driver
from nutkit.protocol import AuthorizationToken
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestProtocolVersions(TestkitTestCase):
    """ Verifies that the driver can connect to a server that speaks a specific
    bolt protocol version.
    """

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)

    def tearDown(self):
        self._server.done()
        super().tearDown()

    def _run(self, version, pull='PULL {"n": 1000}'):
        script_path = self.script_path(
            "v{}_return_1.script".format(version)
        )
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))
        self._server.start(path=script_path,
                           vars={"#VERSION#": version, "#PULL#": pull})
        session = driver.session("w", fetchSize=1000)
        result = session.run("RETURN 1 AS n")
        # Otherwise the script will not fail when the protocol is not present
        # (on backends where run is lazily evaluated)
        result.next()
        session.close()
        driver.close()

    def test_supports_bolt_4x0(self):
        self._run("4x0")

    def test_supports_bolt_4x1(self):
        self._run("4x1")

    def test_supports_bolt_4x2(self):
        self._run("4x2")

    def test_supports_bolt_4x3(self):
        if get_driver_name() in ['java']:
            self.skipTest("4.3 protocol not implemented")
        self._run("4x3")

    def test_supports_bolt_3x0(self):
        self._run("3")
