import re
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

    def _run(self, handshake, handshake_response, bolt_version="5.7"):
        script_path = self.script_path(
            "test_parameterized_handshake_manifest.script"
        )
        vars_ = {
            "#SERVER_AGENT#": "Neo4j/5.26.0",
            "#BOLT_VERSION#": bolt_version,
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

    def _get_newest_bolt_supported_by_driver(self, skip=0, min_version=(5, 0)):
        # skip: number of newest versions to skip (return n-th newest)
        # min_version: minimum bolt version to consider
        all_bolt_versions = [
            (f, tuple(map(int, f.value.split(":")[-1].split("."))))
            for f in types.Feature
            if re.match(r"^BOLT_(\d+)_(\d+)$", f.name)
        ]
        all_bolt_versions_ge_5_7 = sorted(
            [
                (f, v) for f, v in all_bolt_versions
                if v[0] == 5 and v >= min_version
            ],
            reverse=True,
            key=lambda x: x[1]
        )
        for feature, version in all_bolt_versions_ge_5_7:
            if self.driver_supports_features(feature):
                if not skip:
                    return version
                skip -= 1
        self.skipTest("No appropriate bolt version supported by driver")

    @driver_feature(types.Feature.BOLT_HANDSHAKE_MANIFEST_V1)
    def test_handshake_manifest_range_response_newer_server(self):
        used_version = self._get_newest_bolt_supported_by_driver()
        server_response, expected_response = (
            "00 00 01 FF 02 00 00 04 04 00 FF FF 05 00",
            f"00 00 {used_version[1]:02X} {used_version[0]:02X} 00",
        )
        self._run(
            server_response,
            expected_response,
            bolt_version=".".join(map(str, used_version)),
        )

    @driver_feature(types.Feature.BOLT_HANDSHAKE_MANIFEST_V1)
    def test_handshake_manifest_range_response_older_server(self):
        used_version = self._get_newest_bolt_supported_by_driver(skip=1)
        minor_major_hex = f"{used_version[1]:02X} {used_version[0]:02X}"
        used_version_range_hex = f"00 {used_version[1]:02X} {minor_major_hex}"
        used_version_hex = f"00 00 {minor_major_hex}"
        server_response, expected_response = (
            f"00 00 01 FF 02 00 00 04 04 {used_version_range_hex} 00",
            f"{used_version_hex} 00",
        )
        self._run(
            server_response,
            expected_response,
            bolt_version=".".join(map(str, used_version)),
        )
