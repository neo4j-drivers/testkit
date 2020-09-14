import unittest

from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken, NullRecord
import nutkit.protocol as types
from tests.neo4j.shared import *
from tests.shared import *


class TestBoltTypes(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._host, self._port = get_neo4j_host_and_port()
        self._scheme = "bolt://%s:%d" % (self._host, self._port)

    def tearDown(self):
        self._session.close()
        self._driver.close()
        self._backend.close()

    def createDriverAndSession(self):
        auth_token = AuthorizationToken(scheme="basic",
                                        principal=os.environ.get(env_neo4j_user, "neo4j"),
                                        credentials=os.environ.get(env_neo4j_pass, "pass"))
        self._driver = Driver(self._backend, self._scheme, auth_token)
        self._session = self._driver.session("r")

    def testShouldEchoBack(self):
        test_map = {True:                   types.CypherBool,
                    False:                  types.CypherBool,
                    None:                   types.CypherNull,
                    1:                      types.CypherInt,
                    -7:                     types.CypherInt,
                    -129:                   types.CypherInt,
                    129:                    types.CypherInt,
                    2147483647:             types.CypherInt,
                    -2147483647:            types.CypherInt,
                    9223372036854775807:    types.CypherFloat,
                    -9223372036854775807:   types.CypherFloat,
                    1.7976931348623157E+308: types.CypherFloat,
                    2.2250738585072014e-308: types.CypherFloat,
                    4.9E-324:               types.CypherFloat,
                    0.0:                    types.CypherFloat,
                    1.1:                    types.CypherFloat,
                    "1":                    types.CypherString,
                    "-17∂ßå®":              types.CypherString,
                    "String":               types.CypherString,
                    "":                     types.CypherString}

        self.createDriverAndSession()

        for key, value in test_map.items():
            self.verifyCanEcho(key, value)

    def verifyCanEcho(self, key, value):
        result = self._session.run("RETURN $x as y", params={"x": value(key)})
        self.assertEqual(result.next(), types.Record(values=[key]))
