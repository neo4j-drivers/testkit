import os

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.neo4j.shared import (
    cluster_unsafe_test,
    env_neo4j_user,
    env_neo4j_pass,
    get_driver,
)
from tests.shared import TestkitTestCase


class TestAuthenticationBasic(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._driver = None
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        if self._driver:
            self._driver.close()
        super().tearDown()

    def createDriverAndSession(self, token):
        self._driver = get_driver(self._backend, auth=token)
        self._session = self._driver.session("r")

    @cluster_unsafe_test
    def verifyConnectivity(self, auth_token):
        self.createDriverAndSession(auth_token)
        result = self._session.run("RETURN 2 as Number")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(2)]))

    @cluster_unsafe_test
    def testErrorOnIncorrectCredentials(self):
        auth_token = types.AuthorizationToken(scheme="basic",
                                              principal="fake",
                                              credentials="fake")
        # TODO: Expand this to check errorType is AuthenticationError
        with self.assertRaises(types.DriverError):
            self.verifyConnectivity(auth_token)

    # Tests both basic with realm specified and also custom auth token. All
    @cluster_unsafe_test
    def testSuccessOnProvideRealmWithBasicToken(self):
        auth_token = types.AuthorizationToken(
            scheme="basic",
            realm="native",
            principal=os.environ.get(env_neo4j_user, "neo4j"),
            credentials=os.environ.get(env_neo4j_pass, "pass"))
        self.verifyConnectivity(auth_token)

    @cluster_unsafe_test
    def testSuccessOnBasicToken(self):
        auth_token = types.AuthorizationToken(
            scheme="basic",
            principal=os.environ.get(env_neo4j_user, "neo4j"),
            credentials=os.environ.get(env_neo4j_pass, "pass"))
        self.verifyConnectivity(auth_token)
