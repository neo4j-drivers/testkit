import os
import subprocess
import sys
import time
from contextlib import contextmanager

from nutkit.frontend import Driver
from nutkit.protocol import (
    AuthorizationToken,
    DriverError,
    Feature,
)
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)

# Retrieve path to the repository containing this script.
# Use this path as base for locating a whole bunch of other stuff.
THIS_PATH = os.path.dirname(os.path.abspath(__file__))


class TlsServer:
    def __init__(self, server_cert, min_tls="0", max_tls="2",
                 disable_tls=False, client_cert=None):
        # Name of server certificate, corresponds to a .pem and .key file.
        server_path = os.path.join(THIS_PATH, "..", "..", "tlsserver",
                                   "tlsserver")
        cert_path = os.path.join(THIS_PATH, "certs", "server",
                                 "%s.pem" % server_cert)
        key_path = os.path.join(THIS_PATH, "certs", "server",
                                "%s.key" % server_cert)
        params = [
            server_path,
            "-bind", "0.0.0.0:6666",
            "-cert", cert_path,
            "-key", key_path,
            "-minTls", min_tls,
            "-maxTls", max_tls
        ]
        if disable_tls:
            params.append("--disableTls")
        if client_cert is not None:
            params.append("--clientCert")
            params.append(client_cert)
        self._process = subprocess.Popen(
            params,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            encoding="utf-8"
        )
        # Wait until something is written to know it started
        line = self._process.stdout.readline()
        print(line, end="")

    def _close_pipes(self):
        self._process.stdout.close()
        self._process.stderr.close()

    def connected(self):
        """Check if the server stopped w/o error and if a driver connected."""
        polls = 100
        while polls:
            self._process.poll()
            if self._process.returncode is None:
                time.sleep(0.1)
                polls -= 1
            else:
                connected = self._process.returncode == 0
                self._close_pipes()
                self._process = None
                return connected
        self._kill()
        raise Exception("Timeout")

    def _dump(self):
        sys.stdout.flush()
        print(">>>> Captured TLS server stdout")
        for line in self._process.stdout:
            print(line, end="")
        print("<<<< Captured TLS server stdout")

        print(">>>> Captured TLS server stderr")
        for line in self._process.stderr:
            print(line, end="")
        print("<<<< Captured TLS server stderr")

        self._close_pipes()
        sys.stdout.flush()

    def _kill(self):
        self._process.kill()
        self._process.wait()
        self._dump()
        self._process = None

    def reset(self):
        if self._process:
            self._kill()


class TestkitTlsTestCase(TestkitTestCase):
    @contextmanager
    def _make_driver(self, scheme, host, **driver_config):
        url = "%s://%s:6666" % (scheme, host)
        # Doesn't really matter
        auth = AuthorizationToken("basic", principal="neo4j",
                                  credentials="pass")
        driver = Driver(self._backend, url, auth, **driver_config)
        try:
            yield driver
        finally:
            driver.close()

    @contextmanager
    def _make_session(self, driver, mode, **session_config):
        session = driver.session(mode, **session_config)
        try:
            yield session
        finally:
            session.close()

    def _try_connect(self, server, driver):
        with self._make_session(driver, "r") as session:
            try:
                session.run("RETURN 1 AS n")
            except DriverError:
                pass
        return server.connected()

    @driver_feature(Feature.API_DRIVER_IS_ENCRYPTED)
    def _test_reports_encrypted(self, expected, scheme, **driver_config):
        supports_when_closed = self.driver_supports_features(
            Feature.DETAIL_CLOSED_DRIVER_IS_ENCRYPTED
        )
        url = "%s://example.com:6666" % scheme
        auth = AuthorizationToken("basic", principal="neo4j",
                                  credentials="pass")
        driver = Driver(self._backend, url, auth, **driver_config)
        self.assertEqual(driver.is_encrypted(), expected)
        driver.close()
        if supports_when_closed:
            self.assertEqual(driver.is_encrypted(), expected)
