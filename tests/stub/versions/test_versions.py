from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    get_dns_resolved_server_address,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestProtocolVersions(TestkitTestCase):
    """
    Test bolt versions.

    Verifies that the driver can connect to a server that speaks a specific
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
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=script_path, vars_=vars_)
        session = driver.session("w", fetch_size=1000)
        try:
            yield session
        finally:
            session.close()
            driver.close()
            self._server.reset()

    def _run(self, version, server_agent=None, check_version=False,
             rejected_agent=False, check_server_address=False):
        """Run the common part of the tests.

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
        expected_server_version = version.replace("x", ".")
        if "." not in expected_server_version:
            expected_server_version += ".0"
        expected_server_version += ".0"
        vars_ = {}
        if server_agent is None:
            vars_["#SERVER_AGENT#"] = "Neo4j/" + expected_server_version
        else:
            vars_["#SERVER_AGENT#"] = server_agent
        script_path = self.script_path("v{}_return_1.script".format(version))
        with self._get_session(script_path, vars_=vars_) as session:
            try:
                result = session.run("RETURN 1 AS n")
                if server_agent or check_version or check_server_address:
                    summary = result.consume()
                    if check_version:
                        expected_protocol_version = \
                            self._server.get_negotiated_bolt_version()
                        expected_protocol_version = ".".join(
                            map(str, expected_protocol_version)
                        )
                        if "." not in expected_protocol_version:
                            expected_protocol_version += ".0"
                        self.assertEqual(summary.server_info.protocol_version,
                                         expected_protocol_version)
                    if server_agent is not None:
                        self.assertEqual(summary.server_info.agent,
                                         vars_["#SERVER_AGENT#"])
                    if check_server_address:
                        self.assertEqual(
                            summary.server_info.address,
                            get_dns_resolved_server_address(self._server)
                        )
                else:
                    # Otherwise the script will not fail when the protocol is
                    # not present (on backends where run is lazily evaluated)
                    result.next()
            except types.DriverError:
                if not rejected_agent:
                    raise
                # TODO: check exception further
            else:
                if rejected_agent:
                    self.fail("Driver should have rejected the server agent")
            self._server.done()

    @driver_feature(types.Feature.BOLT_3_0)
    def test_supports_bolt_3x0(self):
        self._run("3")

    @driver_feature(types.Feature.BOLT_4_0)
    def test_supports_bolt_4x0(self):
        self._run("4x0")

    @driver_feature(types.Feature.BOLT_4_1)
    def test_supports_bolt_4x1(self):
        self._run("4x1")

    @driver_feature(types.Feature.BOLT_4_2)
    def test_supports_bolt_4x2(self):
        self._run("4x2")

    @driver_feature(types.Feature.BOLT_4_3)
    def test_supports_bolt_4x3(self):
        self._run("4x3")

    @driver_feature(types.Feature.BOLT_4_4)
    def test_supports_bolt4x4(self):
        self._run("4x4")

    def test_server_version(self):
        for version in ("4x4", "4x3", "4x2", "4x1", "4x0", "3"):
            if not self.driver_supports_bolt(version):
                continue
            with self.subTest(version):
                self._run(version, check_version=True)

    def test_server_agent(self):
        for version in ("4x4", "4x3", "4x2", "4x1", "4x0", "3"):
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
                if reject and get_driver_name() in ["javascript", "go",
                                                    "java"]:
                    # skip subtest: Does not reject server's agent string
                    continue
                if agent == "Neo4j/Funky!" and get_driver_name() in ["java"]:
                    # skip subtest: Tries to parse the server agent
                    continue
                if not self.driver_supports_bolt(version):
                    continue
                with self.subTest(version + "-" + agent.replace(".", "x")):
                    self._run(version, server_agent=agent,
                              rejected_agent=reject)

    def test_server_address_in_summary(self):
        # TODO: remove block when all drivers support the address field
        if get_driver_name() in ["java", "javascript", "go", "dotnet"]:
            self.skipTest("Backend doesn't support server address in summary")
        for version in ("4x3", "4x2", "4x1", "4x0", "3"):
            if not self.driver_supports_bolt(version):
                continue
            with self.subTest(version):
                self._run(version, check_server_address=True)

    def test_obtain_summary_twice(self):
        # TODO: remove block when all drivers support the address field
        if get_driver_name() in ["java", "javascript", "go", "dotnet"]:
            self.skipTest("Backend doesn't support server address in summary")
        with self._get_session(
            self.script_path("v4x4_return_1.script"),
            vars_={"#SERVER_AGENT#": "Neo4j/4.4.0"}
        ) as session:
            result = session.run("RETURN 1 AS n")
            summary = result.consume()
            self.assertEqual(summary.server_info.address,
                             get_dns_resolved_server_address(self._server))
        # result should cache summary and still be valid after the session's
        # and the driver's life-time
        summary = result.consume()
        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._server))

    @driver_feature(types.Feature.BOLT_3_0)
    def test_should_reject_server_using_verify_connectivity_bolt_3x0(self):
        # TODO remove this block once fixed
        if get_driver_name() in ["dotnet", "go", "javascript"]:
            self.skipTest("Skipped because it needs investigation")
        self._test_should_reject_server_using_verify_connectivity(version="3")

    @driver_feature(types.Feature.BOLT_4_0)
    def test_should_reject_server_using_verify_connectivity_bolt_4x0(self):
        # TODO remove this block once fixed
        if get_driver_name() in ["java", "dotnet", "go", "javascript"]:
            self.skipTest("Skipped because it needs investigation")
        self._test_should_reject_server_using_verify_connectivity(
            version="4.0"
        )

    @driver_feature(types.Feature.BOLT_4_1)
    def test_should_reject_server_using_verify_connectivity_bolt_4x1(self):
        # TODO remove this block once fixed
        if get_driver_name() in ["java", "dotnet", "go", "javascript"]:
            self.skipTest("Skipped because it needs investigation")
        self._test_should_reject_server_using_verify_connectivity(
            version="4.1"
        )

    @driver_feature(types.Feature.BOLT_4_2)
    def test_should_reject_server_using_verify_connectivity_bolt_4x2(self):
        # TODO remove this block once fixed
        if get_driver_name() in ["java", "dotnet", "go", "javascript"]:
            self.skipTest("Skipped because it needs investigation")
        self._test_should_reject_server_using_verify_connectivity(
            version="4.2"
        )

    @driver_feature(types.Feature.BOLT_4_3)
    def test_should_reject_server_using_verify_connectivity_bolt_4x3(self):
        # TODO remove this block once fixed
        if get_driver_name() in ["java", "dotnet", "go", "javascript"]:
            self.skipTest("Skipped because it needs investigation")
        self._test_should_reject_server_using_verify_connectivity(
            version="4.3"
        )

    @driver_feature(types.Feature.BOLT_4_4)
    def test_should_reject_server_using_verify_connectivity_bolt_4x4(self):
        # TODO remove this block once fixed
        if get_driver_name() in ["java", "dotnet", "go", "javascript"]:
            self.skipTest("Skipped because it needs investigation")
        self._test_should_reject_server_using_verify_connectivity(
            version="4.4"
        )

    def _test_should_reject_server_using_verify_connectivity(self, version):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        script_path = self.script_path("optional_hello.script")
        variables = {
            "#VERSION#": version,
            "#SERVER_AGENT#": "AgentSmith/0.0.1"
        }
        self._server.start(path=script_path, vars_=variables)

        with self.assertRaises(types.DriverError) as e:
            driver.verify_connectivity()

        self._assert_is_untrusted_server_exception(e.exception)
        self._server.done()
        driver.close()

    def _assert_is_untrusted_server_exception(self, e):
        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.UntrustedServerException",
                e.errorType)
