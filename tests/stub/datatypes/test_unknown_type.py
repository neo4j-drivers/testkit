import json
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestUnknownTypes(TestkitTestCase):

    required_features = (
        types.Feature.API_TYPE_UNKNOWN_TYPE,
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

    def test_unknown_type_subtests(self):

        script = "echo_unknown.script"
        for (name, minimum_protocol_major, minimum_protocol_minor, extra) in (
            ("encrypted_value", 6, 10, None),
            ("encrypted_value", 6, 10, {"message": "test message"}),
            (
                "encrypted_value", 6, 10,
                {"message": "test message", "junk data": "junk"}
            ),
        ):
            with self.subTest(
                name=name, minimum_protocol_major=minimum_protocol_major,
                minimum_protocol_minor=minimum_protocol_minor, extra=extra
            ):
                if extra is not None:
                    extra_string = ", {"
                    for key in extra.keys():
                        if extra_string != ", {":
                            extra_string += ", "
                        extra_string += f'"{key}": {json.dumps(extra[key])}'
                    extra_string += "}"
                else:
                    extra_string = ", {}"
                with self._started_server(
                    self._server,
                    script,
                    vars_={
                        "#UNKNOWN#":
                            f'{{"W": [{json.dumps(name)}, '
                            f"{json.dumps(minimum_protocol_major)}, "
                            f"{json.dumps(minimum_protocol_minor)}"
                            f"{extra_string}]}}"
                    },
                ):
                    if extra is None:
                        message = None
                    else:
                        message = extra["message"]
                    with self._driver(self._server) as driver:
                        with self._session(driver) as session:
                            unknown = types.CypherUnknownType(
                                name,
                                minimum_protocol_major,
                                minimum_protocol_minor,
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

    def test_unknown_type_in_list(self):
        script = "echo_unknown.script"
        for (name, minimum_protocol_major, minimum_protocol_minor, extra) in (
            ("encrypted_value", 6, 10, None),
            ("encrypted_value", 6, 10, {"message": "test message"}),
            (
                "encrypted_value", 6, 10,
                {"message": "test message", "junk data": "junk"}
            ),
        ):
            with self.subTest(
                name=name, minimum_protocol_major=minimum_protocol_major,
                minimum_protocol_minor=minimum_protocol_minor, extra=extra
            ):
                if extra is not None:
                    extra_string = ", {"
                    for key in extra.keys():
                        if extra_string != ", {":
                            extra_string += ", "
                        extra_string += f'"{key}": {json.dumps(extra[key])}'
                    extra_string += "}"
                else:
                    extra_string = ", {}"
                with self._started_server(
                    self._server,
                    script,
                    vars_={
                        "#UNKNOWN#":
                            f'[1, 2, {{"W": [{json.dumps(name)}, '
                            f"{json.dumps(minimum_protocol_major)}, "
                            f"{json.dumps(minimum_protocol_minor)}"
                            f"{extra_string}]}}]"
                    },
                ):
                    if extra is None:
                        message = None
                    else:
                        message = extra["message"]
                    with self._driver(self._server) as driver:
                        with self._session(driver) as session:
                            unknown = types.CypherUnknownType(
                                name,
                                minimum_protocol_major,
                                minimum_protocol_minor,
                                message
                            )
                            result = session.run(
                                "RETURN 1 as one",
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
                                unknown,
                                records[0].values[0].value[2]
                            )
