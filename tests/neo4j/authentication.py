import unittest

from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken, NullRecord
import nutkit.protocol as types
from tests.neo4j.shared import *
from tests.shared import *


class TestAuthenticationBasic(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._host, self._port = get_neo4j_host_and_port()
        self._scheme = "bolt://%s:%d" % (self._host, self._port)

    def tearDown(self):
        self._session.close()
        self._driver.close()
        self._backend.close()

    def createDriverAndSession(self, token):
        self._driver = Driver(self._backend, self._scheme, token)
        self._session = self._driver.session("r")

    def verifyConnectivity(self, auth_token):
        self.createDriverAndSession(auth_token)
        result = self._session.run("RETURN 2 as Number")
        self.assertEqual(result.next(), types.Record(values=[2]))

    def testErrorOnIncorrectCredentials(self):
        auth_token = AuthorizationToken(scheme="basic",
                                        principal="fake",
                                        credentials="fake")
        with self.assertRaises(types.DriverError) as e:   # TODO: Expand this to check errorType is AuthenticationError
            self.verifyConnectivity(auth_token)

    # Tests both basic with realm specified and also custom auth token. All
    def testSuccessOnProvideRealmWithBasicToken(self):
        auth_token = AuthorizationToken(scheme="basic",
                                        realm="native",
                                        principal=os.environ.get(env_neo4j_user, "neo4j"),
                                        credentials=os.environ.get(env_neo4j_pass, "pass"))
        self.verifyConnectivity(auth_token)

    def testSuccessOnBasicToken(self):
        auth_token = AuthorizationToken(scheme="basic",
                                        principal=os.environ.get(env_neo4j_user, "neo4j"),
                                        credentials=os.environ.get(env_neo4j_pass, "pass"))
        self.verifyConnectivity(auth_token)




