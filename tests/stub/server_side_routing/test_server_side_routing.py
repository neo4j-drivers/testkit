from nutkit.frontend import Driver
import nutkit.protocol as types
from nutkit.protocol.error_type import ErrorType
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestServerSideRouting(TestkitTestCase):
    """Test driver-behavior in Server Side Routing scenarios."""

    required_features = types.Feature.BOLT_4_1,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._auth = types.AuthorizationToken(
            "basic", principal="p", credentials="c")
        self._userAgent = "007"

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def _start_server(self):
        path = self.script_path("direct_connection_without_routing_ssr.script")
        self._server.start(path=path, vars_={"#VERSION#": "4.1"})

    # When a direct driver is created without params, it should not send
    # any information about the routing context in the HELLO message
    # to not enable Server Side Routing
    def test_direct_connection_without_url_params(self):
        uri = "bolt://%s" % self._server.address
        self._start_server()

        driver = Driver(self._backend, uri, self._auth, self._userAgent)
        session = driver.session("w", fetch_size=1000)
        result = session.run("RETURN 1 AS n")
        # Otherwise the script will not fail when the protocol is not present
        # (on backends where run is lazily evaluated)
        result.next()
        session.close()
        driver.close()
        self._server.done()

    # When a direct driver is created without params,
    # it should throw an exception or work normaly without
    # sending routing info to the server
    def test_direct_connection_with_url_params(self):
        params = "region=china&policy=my_policy"
        uri = "bolt://%s?%s" % (self._server.address, params)
        self._start_server()
        with self.assertRaises(types.DriverError) as exc:
            driver = Driver(self._backend, uri, self._auth, self._userAgent)
        if get_driver_name() in ["java", "python", "javascript"]:
            self.assertEqual(ErrorType.ILLEGAL_ARGUMENT_ERROR.value,
                             exc.exception.errorType)
            self.assertIn(uri, exc.exception.msg)
        elif get_driver_name() in ["ruby"]:
            self.assertEqual("ArgumentError", exc.exception.errorType)
        elif get_driver_name() in ["javascript"]:
            self.assertIn(uri, exc.exception.msg)
        elif get_driver_name() in ["go"]:
            self.assertIn("bolt", exc.exception.msg.lower())
            self.assertIn("routing", exc.exception.msg.lower())
        elif get_driver_name() in ["dotnet"]:
            self.assertEqual("ArgumentError", exc.exception.errorType)
            # not asserting on the whole URI because the backend normalizes it.
            self.assertIn(params, exc.exception.msg)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
