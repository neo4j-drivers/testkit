import json
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestUnsupportedTypes(TestkitTestCase):

    required_features = (
        types.Feature.API_TYPE_UNSUPPORTED_TYPE,
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

    def test_unsupported_type_subtests(self):

        script = "echo_unsupported.script"
        for (
            name, minimum_protocol_major, minimum_protocol_minor, message
        ) in (
            ("encrypted_value", 6, 10, None),
            ("encrypted_value", 6, 10, "test message"),
        ):
            with self.subTest(
                name=name, minimum_protocol_major=minimum_protocol_major,
                minimum_protocol_minor=minimum_protocol_minor, message=message
            ):
                if message is not None:
                    message_string = ', "' + message + '"'
                else:
                    message_string = ""
                with self._started_server(
                    self._server,
                    script,
                    vars_={
                        "#UNSUPPORTED#":
                            f'{{"UT": [{json.dumps(name)}, '
                            f"{json.dumps(minimum_protocol_major)}, "
                            f"{json.dumps(minimum_protocol_minor)}"
                            f"{message_string}]}}"
                    },
                ):
                    with self._driver(self._server) as driver:
                        with self._session(driver) as session:
                            unsupported = types.CypherUnsupportedType(
                                name,
                                minimum_protocol_major,
                                minimum_protocol_minor,
                                message
                            )
                            result = session.run(
                                "RETURN 1 AS one",
                            )
                            records = list(result)
                            self._server.done()
                            self.assertEqual(len(records), 1)
                            self.assertEqual(len(records[0].values), 1)
                            self.assertEqual(unsupported, records[0].values[0])

    def test_unsupported_type_in_list(self):
        script = "echo_unsupported.script"
        for (
            name, minimum_protocol_major, minimum_protocol_minor, message
        ) in (
            ("encrypted_value", 6, 10, None),
            ("encrypted_value", 6, 10, "test message"),
        ):
            with self.subTest(
                name=name, minimum_protocol_major=minimum_protocol_major,
                minimum_protocol_minor=minimum_protocol_minor, message=message
            ):
                if message is not None:
                    message_string = ', "' + message + '"'
                else:
                    message_string = ""
                with self._started_server(
                    self._server,
                    script,
                    vars_={
                        "#UNSUPPORTED#":
                            f'[1, 2, {{"UT": [{json.dumps(name)}, '
                            f"{json.dumps(minimum_protocol_major)}, "
                            f"{json.dumps(minimum_protocol_minor)}"
                            f"{message_string}]}}]"
                    },
                ):
                    with self._driver(self._server) as driver:
                        with self._session(driver) as session:
                            unsupported = types.CypherUnsupportedType(
                                name,
                                minimum_protocol_major,
                                minimum_protocol_minor,
                                message
                            )
                            result = session.run(
                                "RETURN 1 AS one",
                            )
                            records = list(result)
                            self._server.done()
                            self.assertEqual(len(records), 1)
                            self.assertEqual(len(records[0].values), 1)
                            self.assertEqual(
                                types.CypherInt(1),
                                records[0].values[0].value[0]
                            )
                            self.assertEqual(
                                unsupported,
                                records[0].values[0].value[2]
                            )
