import os

import nutkit.protocol as types
from tests.neo4j.shared import (
    cluster_unsafe_test,
    env_neo4j_pass,
    env_neo4j_user,
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

    def create_driver_and_session(self, token):
        self._driver = get_driver(self._backend, auth=token)
        self._session = self._driver.session("r")

    @cluster_unsafe_test
    def verify_connectivity(self, auth_token):
        self.create_driver_and_session(auth_token)
        result = self._session.run("RETURN 2 as Number")
        self.assertEqual(
            result.next(), types.Record(values=[types.CypherInt(2)]))

    @cluster_unsafe_test
    def test_error_on_incorrect_credentials(self):
        auth_token = types.AuthorizationToken("basic",
                                              principal="fake",
                                              credentials="fake")
        # TODO: Expand this to check errorType is AuthenticationError
        with self.assertRaises(types.DriverError):
            self.verify_connectivity(auth_token)

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
