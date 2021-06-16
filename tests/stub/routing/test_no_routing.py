from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_dns_resolved_server_address,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import (
    get_dns_resolved_server_address,
    StubServer,
)
from ._routing import get_extra_hello_props


class NoRouting(TestkitTestCase):
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
        if get_driver_name() in ['java', 'dotnet', 'go']:
            routing = ', "routing": null'
        return {
            "#VERSION#": "4.1",
            "#SERVER_AGENT#": "Neo4j/4.1.0",
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
            "#ROUTING#": routing
        }

    # Checks that routing is disabled when URI is bolt, no routing in HELLO and
    # no call to retrieve routing table. From bolt >= 4.1 the routing context
    # is used to disable/enable server side routing.
    def test_should_read_successfully_using_session_run(self):
        # Driver is configured to talk to "routing" stub server
        uri = "bolt://%s" % self._server.address
        self._server.start(
            path=self.script_path("v4x1_no_routing", "reader.script"),
            vars=self.get_vars()
        )
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken(scheme="basic", principal="p",
                                                 credentials="c"),
                        userAgent="007")

        session = driver.session('r', database="adb")
        res = session.run("RETURN 1 as n")
        summary = res.consume()
        session.close()
        driver.close()

        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._server))
        self._server.done()
