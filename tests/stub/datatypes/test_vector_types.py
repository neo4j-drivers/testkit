import json
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestVectorTypes(TestkitTestCase):

    required_features = (
        types.Feature.API_TYPE_TEMPORAL,
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

    def test_vector(self):
        script = "echo_vector.script"
        for (dtype, data) in (
            ("i8", ""),
            ("i8", "01"),
            ("i8", "01 ff"),
            ("i16", "00 01"),
            ("i32", "00 00 00 01"),
            ("i64", "00 00 00 00 00 00 00 01"),
            ("f32", "3f 80 00 00"),
            ("f64", "3f f0 00 00 00 00 00 00"),
        ):
            with self.subTest(dtype=dtype, data=data):
                with self._started_server(
                    self._server,
                    script,
                    vars_={
                        "#VECTOR#":
                            f'{{"V": [{json.dumps(dtype)}, '
                            f"{json.dumps(data)}]}}",
                    },
                ):
                    with self._driver(self._server) as driver:
                        with self._session(driver) as session:
                            vec = types.CypherVector(dtype, data)
                            result = session.run(
                                "RETURN $vec AS vec",
                                params={"vec": vec},
                            )
                            records = list(result)
                            self._server.done()
                            self.assertEqual(len(records), 1)
                            self.assertEqual(len(records[0].values), 1)
                            self.assertEqual(vec, records[0].values[0])
