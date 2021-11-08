import os
import subprocess
import sys
import time

from nutkit.frontend import Driver
from nutkit.protocol import (
    AuthorizationToken,
    DriverError,
)

# Retrieve path to the repository containing this script.
# Use this path as base for locating a whole bunch of other stuff.
THIS_PATH = os.path.dirname(os.path.abspath(__file__))


class TlsServer:
    def __init__(self, server_cert, min_tls="0", max_tls="2",
                 disable_tls=False):
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
        self._process = subprocess.Popen(
            params,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            encoding="utf-8"
        )
        # Wait until something is written to know it started
        line = self._process.stdout.readline()
        print(line)

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


def try_connect(backend, server, scheme, host):
    url = "%s://%s:%d" % (scheme, host, 6666)
    # Doesn't really matter
    auth = AuthorizationToken("basic", principal="neo4j", credentials="pass")
    driver = Driver(backend, url, auth)
    session = driver.session("r")
    try:
        session.run("RETURN 1 as n")
    except DriverError:
        pass
    session.close()
    driver.close()
    return server.connected()
