from contextlib import contextmanager

from nutkit.frontend import Driver
from nutkit.protocol import (
    AuthorizationToken,
    DriverError,
)
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

    @contextmanager
    def _get_session(self, script_path, vars_=None):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))
        self._server.start(path=script_path, vars=vars_)
        session = driver.session("w", fetchSize=1000)
        try:
            yield session
        finally:
            session.close()
            driver.close()
            self._server.reset()

    def _run(self, version, server_agent=None, check_version=False,
             rejected_agent=False, check_server_address=False):
        """
            :param version: e.g. "4x3" or "3"
            :type version: string
            :param server_agent: Set a specific server agent and check that the
                                 summary list the correct one.
            :type server_agent: str, optional
            :param check_version: Check if the summary contains the correct bolt
                                  version
            :type check_version: bool
            :param rejected_agent:
                True: Expect the driver to fail by rejecting the server agent
                False: Expect the driver not to fail
            :type rejected_agent: bool
            :param check_server_address: Check if the summary contains the right
                                         server-address
            :type check_server_address: bool
        """
        expected_version = version.replace("x", ".")
        if "." not in expected_version:
            expected_version += ".0"
        vars_ = {}
        if server_agent is None:
            vars_["#SERVER_AGENT#"] = "Neo4j/" + expected_version + ".0"
        else:
            vars_["#SERVER_AGENT#"] = server_agent
        script_path = self.script_path("v{}_return_1.script".format(version))
        with self._get_session(script_path, vars_=vars_) as session:
            try:
                result = session.run("RETURN 1 AS n")
                if server_agent or check_version or check_server_address:
                    summary = result.consume()
                    if check_version:
                        self.assertEqual(summary.server_info.protocol_version,
                                         expected_version)
                    if server_agent is not None:
                        self.assertEqual(summary.server_info.agent,
                                         vars_["#SERVER_AGENT#"])
                    if check_server_address:
                        self.assertEqual(summary.server_info.address,
                                         self._server.address)
                else:
                    # Otherwise the script will not fail when the protocol is
                    # not present (on backends where run is lazily evaluated)
                    result.next()
            except DriverError:
                if not rejected_agent:
                    raise
                # TODO: check exception further
            else:
                if rejected_agent:
                    self.fail("Driver should have rejected the server agent")
        self._server.done()

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

    def test_server_version(self):
        for version in ("4x3", "4x2", "4x1", "4x0", "3"):
            with self.subTest(version):
                self._run(version, check_version=True)

    def test_server_agent(self):
        for version in ("4x3", "4x2", "4x1", "4x0", "3"):
            for agent, reject in (
                ("Neo4j/4.3.0", False),
                ("Neo4j/4.1.0", False),
                ("neo4j/4.1.0", True),
                ("Neo4j/Funky!", False),
                ("Neo4j4.3.0", True),
                ("FooBar/4.3.0", True),
            ):
                # TODO: remove these blocks, once all drivers work
                if get_driver_name() in ["dotnet"]:
                    # skip subtest: Doesn't reject server's agent sting,
                    #               compiles server agent from bolt version
                    #               potentially more differences
                    continue
                if reject and get_driver_name() in ["javascript", "go", "java"]:
                    # skip subtest: Does not reject server's agent string
                    continue
                if agent == "Neo4j/Funky!" and get_driver_name() in ["java"]:
                    # skip subtest: Tries to parse the server agent
                    continue
                with self.subTest(version + "-" + agent.replace(".", "x")):
                    self._run(version, server_agent=agent,
                              rejected_agent=reject)

    def test_server_address_in_summary(self):
        # TODO: remove block when all drivers support the address field
        if get_driver_name() in ["python", "java", "javascript", "go",
                                 "dotnet"]:
            self.skipTest("Backend doesn't support server address in summary")
        for version in ("4x3", "4x2", "4x1", "4x0", "3"):
            with self.subTest(version):
                self._run(version, check_server_address=True)

    def test_obtain_summary_twice(self):
        # TODO: remove block when all drivers support the address field
        if get_driver_name() in ["python", "java", "javascript", "go",
                                 "dotnet"]:
            self.skipTest("Backend doesn't support server address in summary")
        with self._get_session(
            self.script_path("v4x3_return_1.script"),
            vars_={"#SERVER_AGENT#": "Neo4j/4.3.0"}
        ) as session:
            result = session.run("RETURN 1 AS n")
            summary = result.consume()
            self.assertEqual(summary.server_info.address, self._server.address)
        # result should cache summary and still be valid after the session's and
        # the driver's life-time
        summary = result.consume()
        self.assertEqual(summary.server_info.address, self._server.address)
