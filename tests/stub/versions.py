import unittest
from tests.shared import new_backend, get_driver_name
from tests.stub.shared import StubServer
from nutkit.frontend import Driver, AuthorizationToken


class ProtocolVersions(unittest.TestCase):
    """ Verifies that the driver can connect to a server that speaks a specific
    bolt protocol version.
    """

    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)

    def tearDown(self):
        self._backend.close()
        self._server.reset()

    def _run(self, version, pull='PULL {"n": 1000}'):
        script = """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: RUN "RETURN 1 AS n" {} {}
           #PULL#
        S: SUCCESS {"fields": ["n.name"]}
           SUCCESS {"type": "w"}
        """
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))
        self._server.start(script=script,
                           vars={"#VERSION#": version, "#PULL#": pull})
        session = driver.session("w", fetchSize=1000)
        session.run("RETURN 1 AS n")
        session.close()
        driver.close()

    def test_supports_bolt_4x0(self):
        self._run("4.0")

    def test_supports_bolt_4x1(self):
        self._run("4.1")

    def test_supports_bolt_4x3(self):
        if get_driver_name() in ['java']:
            self.skipTest("4.3 protocol not implemented")
        self._run("4.3")

    def test_supports_bolt_3x0(self):
        self._run("3", pull="PULL_ALL")
