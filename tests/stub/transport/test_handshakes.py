from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestHandshakeManifest(TestkitTestCase):
    """
    Test bolt versions.

    Verifies that the driver can connect to a server that speaks a specific
    bolt protocol version.
    """

    @contextmanager
    def _server(self, script_path, vars_=None, port=9001):
        server = StubServer(port)
        server.start(path=script_path, vars_=vars_)
        try:
            yield server
        finally:
            server.reset()

    @contextmanager
    def _get_session(self, server):
        uri = "bolt://%s" % server.address
        auth = types.AuthorizationToken("basic", principal="", credentials="")
        driver = Driver(self._backend, uri, auth)
        try:
            session = driver.session("w", fetch_size=1000)
            try:
                yield session
            finally:
                session.close()
        finally:
            driver.close()

    def _run(self, handshake, handshake_response):
        script_path = self.script_path(
            "test_parameterized_handshake_manifest.script"
        )
        vars_ = {
            "#SERVER_AGENT#": "Neo4j/5.26.0"
        }
        if handshake is not None:
            vars_["#HANDSHAKE#"] = handshake
        if handshake_response is not None:
            vars_["#HANDSHAKE_RESPONSE#"] = handshake_response
        with self._server(script_path, vars_) as server:
            with self._get_session(server) as session:
                session.run("RETURN 1 AS n").consume()
            server.done()
            self.assertEqual(
                server.count_requests("RUN"),
                1,
                "Closed after handshake, driver choose an unexpected version",
            )

    @driver_feature(
        types.Feature.BOLT_5_7,
        types.Feature.BOLT_HANDSHAKE_MANIFEST_V1,
    )
    def test_handshake_manifest_server_responses(self):
        for server_response, expected_response in [
            (
                "00 00 01 FF 03 00 07 07 05 00 04 04 04 00 00 00 03 00",
                "00 00 07 05 00",
            ),
            (
                "00 00 01 FF 03 00 00 00 03 00 07 07 05 00 04 04 04 00",
                "00 00 07 05 00",
            ),
            (
                "00 00 01 FF 03 00 00 00 03 00 04 04 04 00 07 07 05 00",
                "00 00 07 05 00",
            ),
            (
                "00 00 01 FF 02 00 07 07 F2 00 07 07 05 00",
                "00 00 07 05 00",
            ),

        ]:
            with self.subTest(
                server_response=server_response,
                expected_response=expected_response
            ):
                self._run(server_response, expected_response)
