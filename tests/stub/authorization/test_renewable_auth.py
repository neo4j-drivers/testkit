from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.stub.authorization.test_authorization import AuthorizationBase
from tests.stub.shared import StubServer


class TestRenewableAuth5x1(AuthorizationBase):

    # TODO:
    #  * Test Bolt 5.0 (should work by purging pool)
    #  * Test Bolt 5.1 (should work using re-auth)
    #  * Test token expiring by deadline
    #  * Test token expiring by server error

    required_features = types.Feature.BOLT_5_1,

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader = StubServer(9010)
        self._writer = StubServer(9011)
        self._uri = "bolt://%s:%d" % (self._reader.host,
                                      self._reader.port)
        self._driver = None

    def tearDown(self):
        self._router.reset()
        self._reader.reset()
        self._writer.reset()
        if self._driver:
            self._driver.close()
        super().tearDown()

    def get_vars(self):
        return {
            # "#HOST#": self._router.host,
            "#VERSION#": "5.1"
        }

    def test_test(self):
        def provider():
            return types.RenewableAuthToken(
                types.AuthorizationToken(
                    scheme="basic",
                    principal="neo4j",
                    credentials="pass"
                )
            )

        self.start_server(self._reader, "scheme_basic.script")
        self._driver = Driver(self._backend, self._uri, provider)
        session = self._driver.session("r")
        list(session.run("RETURN 1 AS n"))

        self._reader.done()
