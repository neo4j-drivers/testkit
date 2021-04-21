from nutkit.frontend import Driver
from nutkit.protocol import AuthorizationToken
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


direct_connection_without_routing_ssr_script = """
!: BOLT #VERSION#
!: AUTO RESET
!: AUTO GOODBYE

{{
    C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "realm": "", "ticket": "" }
----
    C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "realm": "" }
----
    C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" }
}}

S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}

C: RUN "RETURN 1 AS n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n.name"]}
   SUCCESS {"type": "w"}
"""


class ServerSideRouting(TestkitTestCase):
    """ Verifies that the driver behaves as expected  when
    in Server Side Routing scenarios
    """

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._auth = types.AuthorizationToken(
            scheme="basic", principal="p", credentials="c")
        self._userAgent = "007"

    def tearDown(self):
        self._server.stop()
        super().tearDown()

    def test_direct_connection_without_url_params(self):
        """ When a direct driver is created without params, it should not send
        any information about the routing context in the HELLO message
        to not enable Server Side Routing
        """
        uri = "bolt://%s" % self._server.address
        self._server.start(script=direct_connection_without_routing_ssr_script,
                           vars={"#VERSION#": "4.1"})

        
        driver = Driver(self._backend, uri, self._auth, self._userAgent)
        session = driver.session("w", fetchSize=1000)
        result = session.run("RETURN 1 AS n")
        # Otherwise the script will not fail when the protocol is not present
        # (on backends where run is lazily evaluated)
        result.next()
        session.close()
        driver.close()
        self._server.done()

    def test_direct_connection_with_url_params(self):
        """ When a direct driver is created without params,
        it should throw an exception
        """
        params = "region=china&policy=my_policy"
        uri = "bolt://%s?%s" % (self._server.address, params)
        self._server.start(script=direct_connection_without_routing_ssr_script,
                           vars={
                               "#VERSION#": "4.1"
                           })
        try:
            driver = Driver(self._backend, uri, self._auth, self._userAgent)
        except Exception:
            pass
        else:
            driver.close()
            self.fail('Should not create the driver')


