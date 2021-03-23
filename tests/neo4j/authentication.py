import os

from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types
from tests.neo4j.shared import (
        get_neo4j_host_and_port, env_neo4j_user, env_neo4j_pass)
from tests.shared import new_backend, TestkitTestCase


class TestAuthenticationBasic(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._host, self._port = get_neo4j_host_and_port()
        self._scheme = "bolt://%s:%d" % (self._host, self._port)
        self._driver = None
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        if self._driver:
            self._driver.close()
        super().tearDown()

    def createDriverAndSession(self, token):
        self._driver = Driver(self._backend, self._scheme, token)
        self._session = self._driver.session("r")

    def verifyConnectivity(self, auth_token):
        self.createDriverAndSession(auth_token)
        result = self._session.run("RETURN 2 as Number")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(2)]))

    def testErrorOnIncorrectCredentials(self):
        auth_token = AuthorizationToken(scheme="basic",
                                        principal="fake",
                                        credentials="fake")
        # TODO: Expand this to check errorType is AuthenticationError
        with self.assertRaises(types.DriverError):
            self.verifyConnectivity(auth_token)

    # Tests both basic with realm specified and also custom auth token. All
    def testSuccessOnProvideRealmWithBasicToken(self):
        auth_token = AuthorizationToken(
            scheme="basic",
            realm="native",
            principal=os.environ.get(env_neo4j_user, "neo4j"),
            credentials=os.environ.get(env_neo4j_pass, "pass"))
        self.verifyConnectivity(auth_token)

    def testSuccessOnBasicToken(self):
        auth_token = AuthorizationToken(
            scheme="basic",
            principal=os.environ.get(env_neo4j_user, "neo4j"),
            credentials=os.environ.get(env_neo4j_pass, "pass"))
        self.verifyConnectivity(auth_token)
