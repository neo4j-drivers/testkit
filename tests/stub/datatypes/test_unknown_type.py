import json
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestVectorTypes(TestkitTestCase):

    required_features = (
        types.Feature.API_TYPE_UNKNOWNTYPE,
        types.Feature.BOLT_6_0,
    )
    bolt_version = "6.0"

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    @contextmanager
    def _started_server(self, server, script, vars_=None):
        version_folder = "v{}".format(self.bolt_version.replace(".", "x"))
        server.start(
            path=self.script_path(version_folder, script),
            vars_=vars_,
        )
        try:
            yield
        finally:
            server.reset()

    @contextmanager
    def _driver(self, server):
        uri = "bolt://%s" % server.address
        auth = types.AuthorizationToken("basic", principal="", credentials="")
        driver = Driver(self._backend, uri, auth)
        try:
            yield driver
        finally:
            driver.close()

    @contextmanager
    def _session(self, driver):
        session = driver.session("r")
        try:
            yield session
        finally:
            session.close()

    def test_unknown_type(self):
        script = "echo_unknown.script"
        for (name, min_bolt, message) in (
            ("encrypted_value", "6.10", "This is an encrypted value"),
        ):
            with self.subTest(name=name, min_bolt=min_bolt, message=message):
                with self._started_server(
                    self._server,
                    script,
                    vars_={
                        "#UNKNOWN#":
                            f'{{"W": [{json.dumps(name)}, '
                            f"{json.dumps(min_bolt)}, "
                            f'{{"message": {json.dumps(message)}}}]}}',
                    },
                ):
                    with self._driver(self._server) as driver:
                        with self._session(driver) as session:
                            vec = types.CypherUnknownType(
                                name,
                                min_bolt,
                                message
                            )
                            result = session.run(
                                "RETURN 1 as one",
                            )
                            records = list(result)
                            self._server.done()
                            self.assertEqual(len(records), 1)
                            self.assertEqual(len(records[0].values), 1)
                            print(records[0].values[0])
                            self.assertEqual(vec, records[0].values[0])
