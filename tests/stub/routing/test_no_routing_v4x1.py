import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_dns_resolved_server_address,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer
from ._routing import get_extra_hello_props


class NoRoutingV4x1(TestkitTestCase):
    bolt_version = "4.1"
    version_dir = "v4x1_no_routing"
    server_agent = "Neo4j/4.1.0"
    adb = "adb"

    def setUp(self):
        super().setUp()
        self._server = StubServer(9000)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def get_vars(self):
        # TODO: "#ROUTING#": "" is the correct way to go
        #       (minimal data transmission)
        routing = ""
        if get_driver_name() in ['dotnet']:
            routing = ', "routing": null'
        return {
            "#VERSION#": self.bolt_version,
            "#SERVER_AGENT#": self.server_agent,
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
            "#USER_AGENT#": '007',
            "#ROUTING#": routing
        }

    # Checks that routing is disabled when URI is bolt, no routing in HELLO and
    # no call to retrieve routing table. From bolt >= 4.1 the routing context
    # is used to disable/enable server side routing.
    def test_should_read_successfully_using_read_session_run(self):
        # Driver is configured to talk to "routing" stub server
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(self.version_dir, "reader.script"),
            vars=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken(scheme="basic", principal="p",
                                                 credentials="c"),
                        userAgent="007")

        session = driver.session('r', database=self.adb)
        res = session.run("RETURN 1 as n")
        summary = res.consume()
        session.close()
        driver.close()

        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._server))
        self._server.done()

    def test_should_read_successfully_using_write_session_run(self):
        # Driver is configured to talk to "routing" stub server
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path(self.version_dir, "reader.script"),
            vars=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken(scheme="basic", principal="p",
                                                 credentials="c"),
                        userAgent="007")

        session = driver.session('w', database=self.adb)
        res = session.run("RETURN 5 as n")
        summary = res.consume()
        session.close()
        driver.close()

        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._server))
        self._server.done()

    def test_should_exclude_routing_context(self):
        # TODO remove this block once implemented
        if get_driver_name() in ['dotnet']:
            self.skipTest('does not exclude routing context')
        uri = "bolt://%s" % self._server.address
        no_routing_context_vars = self.get_vars()
        no_routing_context_vars.update({
            "#ROUTING#": ""
        })
        self._server.start(
            path=self.script_path(self.version_dir, "reader.script"),
            vars=no_routing_context_vars
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken(scheme="basic", principal="p",
                                                 credentials="c"),
                        userAgent="007")

        session = driver.session('w', database=self.adb)
        res = session.run("RETURN 5 as n")
        summary = res.consume()
        session.close()
        driver.close()

        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._server))
        self._server.done()

    def test_should_send_custom_user_agent_using_write_session_run(self):
        uri = "bolt://%s" % self._server.address
        custom_agent = "custom"
        custom_agent_context_vars = self.get_vars()
        custom_agent_context_vars.update({
            "#USER_AGENT#": custom_agent
        })
        self._server.start(
            path=self.script_path(self.version_dir, "reader.script"),
            vars=custom_agent_context_vars
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken(scheme="basic", principal="p",
                                                 credentials="c"),
                        userAgent=custom_agent)

        session = driver.session('w', database=self.adb)
        res = session.run("RETURN 5 as n")
        summary = res.consume()
        session.close()
        driver.close()

        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._server))
        self._server.done()
