import json
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestUnknownTypes(TestkitTestCase):

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

    def test_unknown_type_with_message(self):
        script = "echo_unknown.script"
        name = "encrypted_value"
        min_bolt = "6.10"
        message = "This is an encrypted value"
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
                    unknown = types.CypherUnknownType(
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
                    self.assertEqual(unknown, records[0].values[0])

    def test_unknown_type_without_message(self):
        script = "echo_unknown.script"
        name = "encrypted_value"
        min_bolt = "6.10"
        with self._started_server(
            self._server,
            script,
            vars_={
                "#UNKNOWN#":
                    f'{{"W": [{json.dumps(name)}, '
                    f"{json.dumps(min_bolt)}, "
                    f"{{}}]}}"
            },
        ):
            with self._driver(self._server) as driver:
                with self._session(driver) as session:
                    unknown = types.CypherUnknownType(
                        name,
                        min_bolt
                    )
                    result = session.run(
                        "RETURN 1 as one",
                    )
                    records = list(result)
                    self._server.done()
                    self.assertEqual(len(records), 1)
                    self.assertEqual(len(records[0].values), 1)
                    print(records[0].values[0])
                    self.assertEqual(unknown, records[0].values[0])

    def test_unknown_type_with_junk_value_in_dict(self):
        script = "echo_unknown.script"
        name = "encrypted_value"
        min_bolt = "6.10"
        message = "This is an encrypted value"
        junk_value = "Should be ignored"
        with self._started_server(
            self._server,
            script,
            vars_={
                "#UNKNOWN#":
                    f'{{"W": [{json.dumps(name)}, '
                    f"{json.dumps(min_bolt)}, "
                    f'{{"message": {json.dumps(message)}, '
                    f'"junk": {json.dumps(junk_value)}}}]}}',
            },
        ):
            with self._driver(self._server) as driver:
                with self._session(driver) as session:
                    unknown = types.CypherUnknownType(
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
                    self.assertEqual(unknown, records[0].values[0])
