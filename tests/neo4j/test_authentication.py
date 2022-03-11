import os

import nutkit.protocol as types
from tests.neo4j.shared import (
    cluster_unsafe_test,
    env_neo4j_pass,
    env_neo4j_user,
    get_authorization,
    get_driver,
    TestkitNeo4jTestCase,
)
from tests.shared import get_driver_name


class TestAuthenticationBasic(TestkitNeo4jTestCase):
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

    def create_driver_and_session(self, token):
        self._driver = get_driver(self._backend, auth=token)
        self._session = self._driver.session("r")

    def verify_connectivity(self, auth_token, use_tx=False):
        def dummy_query(tx_or_session):
            return tx_or_session.run("RETURN 2 as Number").next()

        self.create_driver_and_session(auth_token)
        if use_tx:
            result = self._session.read_transaction(dummy_query)
        else:
            result = self._session.run("RETURN 2 as Number")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(2)])
        )

    @cluster_unsafe_test
    def test_error_on_incorrect_credentials(self):
        auth = get_authorization()
        auth.credentials = auth.credentials + "-but-wrong!"
        # TODO: Expand this to check errorType is AuthenticationError
        with self.assertRaises(types.DriverError) as e:
            self.verify_connectivity(auth)
        self.assertEqual(e.exception.code,
                         "Neo.ClientError.Security.Unauthorized")
        if get_driver_name() in ["python"]:
            self.assertEqual(e.exception.errorType,
                             "<class 'neo4j.exceptions.AuthError'>")

    @cluster_unsafe_test
    def test_error_on_incorrect_credentials_tx(self):
        auth = get_authorization()
        auth.credentials = auth.credentials + "-but-wrong!"
        # TODO: Expand this to check errorType is AuthenticationError
        with self.assertRaises(types.DriverError) as e:
            self.verify_connectivity(auth, use_tx=True)
        self.assertEqual(e.exception.code,
                         "Neo.ClientError.Security.Unauthorized")
        if get_driver_name() in ["python"]:
            self.assertEqual(e.exception.errorType,
                             "<class 'neo4j.exceptions.AuthError'>")

    # Tests both basic with realm specified and also custom auth token. All
    @cluster_unsafe_test
    def test_success_on_provide_realm_with_basic_token(self):
        auth_token = types.AuthorizationToken(
            "basic",
            realm="native",
            principal=os.environ.get(env_neo4j_user, "neo4j"),
            credentials=os.environ.get(env_neo4j_pass, "pass"))
        self.verify_connectivity(auth_token)

    @cluster_unsafe_test
    def test_success_on_basic_token(self):
        auth_token = types.AuthorizationToken(
            "basic",
            principal=os.environ.get(env_neo4j_user, "neo4j"),
            credentials=os.environ.get(env_neo4j_pass, "pass"))
        self.verify_connectivity(auth_token)
