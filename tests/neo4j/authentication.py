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

    def test_error_on_incorrect_credentials(self):
        auth_token = AuthorizationToken(scheme="basic", principal="fake", credentials="fake")
        self.createDriverAndSession(auth_token)
        with self.assertRaises(types.DriverError) as e:     # TODO: We will want to expand this to check errorType is AuthenticationError.
            self._session.run("RETURN 1")

    # Tests both basic with realm specified and also custom auth token. All
    def test_success_on_provide_realm_with_basic_token(self):
        auth_token = AuthorizationToken(scheme="basic", realm="native", principal=os.environ.get(env_neo4j_user, "neo4j"), credentials=os.environ.get(env_neo4j_pass, "pass"))
        self.verifyConnectivity(auth_token)

    # Work in progress.
    # def test_succes_on_custom_auth_with_parameters(self):
    # params = types.CypherMap([[types.CypherString("Key1"), types.CypherInt(1)], [types.CypherString("Key2"), types.CypherInt(2)]])
    # auth_token = AuthorizationToken(scheme="basic", realm="native", principal=os.environ.get(env_neo4j_user, "neo4j"), credentials=os.environ.get(env_neo4j_pass, "pass"), ticket=params)
    # self.verifyConnectivity(auth_token)



