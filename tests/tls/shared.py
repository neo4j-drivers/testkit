
import subprocess, platform, os, time, sys
import unittest
from tests.shared import *
from nutkit.frontend import Driver, AuthorizationToken

# Retrieve path to the repository containing this script.
# Use this path as base for locating a whole bunch of other stuff.
thisPath = os.path.dirname(os.path.abspath(__file__))


class TlsServer:
    def __init__(self):
        pass

    def start(self):
        print("Starting TLS server")

        pycmd = "python3"
        if platform.system() is "Windows":
            pycmd = "python"
        serverPath = os.path.join(thisPath, "tlsserver.py")
        certPath = os.path.join(thisPath, "certs", "server", "trustedRoot_server1.pem")
        keyPath = os.path.join(thisPath, "certs", "server", "trustedRoot_server1.key")
        self._process = subprocess.Popen([pycmd, "-u", serverPath, certPath, keyPath],
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         close_fds=True,
                                         encoding='utf-8')
        # Wait until something is written to know it started
        print("Waiting for TLS server to start")
        self._process.stdout.readline()
        print("TLS server started")

    def _close_pipes(self):
        self._process.stdout.close()
        self._process.stderr.close()

    def done(self, expect_connect):
        """ Checks that the server has stopped and its exit code.
        If expect_connect is True, the exit code should be zero otherwise raise an error.
        If expect_connect is False, the exit code should be non-zero otherwise raise an error.
        If server hasn't stopped, kill it and raise an error.
        """
        polls = 100
        while polls:
            self._process.poll()
            if self._process.returncode is None:
                time.sleep(0.1)
                polls -= 1
            else:
                if self._process.returncode:
                    # Non-zero exit code, driver didn't connect
                    if expect_connect:
                        self._dump()
                        raise Exception("Expected to connect but didn't (or TLS server failed)")
                else:
                    # Zero exit code, driver connected
                    if not expect_connect:
                        self._dump()
                        raise Exception("Didn't expect connect but did")
                self._close_pipes()
                self._process = None
                return
        raise Exception("Timeout")

    def _dump(self):
        print("")
        print(">>>> Captured TLS server stdout")
        for line in self._process.stdout:
            sys.stdout.write(line)
        print("<<<< Captured TLS server stdout")
        print(">>>> Captured TLS server stderr")
        for line in self._process.stderr:
            sys.stdout.write(line)
        print("<<<< Captured TLS server stderr")
        self._close_pipes()

    def _kill(self):
        self._process.kill()
        self._process.wait()
        self._dump()
        self._process = None

    def reset(self):
        if self._process:
            self._kill()


"""
neo4j+ssc
    no tls                        - no connection
    valid cert                    - connection
    invalid date                  - no connection
neo4j+s:
    no tls                        - no connection
    trusted CA                    - connection
    trusted CA, wrong server name - no connection
    trusted CA, invalid date      - no connection
    untrusted CA                  - no connection
    trusted CA, tls 1.1 only      - connection
    trusted CA, tls 1.2 only      - connection
neo4j:
    trusted CA                    - no connection
    

"""

# TODO: Cleanup
address = os.environ.get("TEST_TLSSERVER_ADDRESS", '127.0.0.1')

class TestCASigned(unittest.TestCase):
    """ Tests URL scheme neo4j+s where server is assumed to present a server certificate
    signed by a certificate authority recognized by the driver.
    """
    def setUp(self):
        self._backend = new_backend()
        self._server = None
        # Doesn't really matter
        self._auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
        self._scheme = "neo4j+s://%s:%d" % (address, 6666)

    def tearDown(self):
        if self._server:
            # If test raised an exception this will make sure that the stub server
            # is killed and it's output is dumped for analys.
            self._server.reset()
            self._server = None

        """ Happy path, the server has a valid server certificate signed by a trusted
        certificate authority.
        """
    def test_connect_trusted_ca(self):
        self._server = TlsServer()
        self._server.start()
        driver = Driver(self._backend, self._scheme, self._auth)
        session = driver.session("r")
        try:
            session.run("RETURN 1")
            session.close()
            driver.close()
        except Exception as e:
            pass

        self._server.done(expect_connect=True)

"""

class TestSelfSigned(unittest.TestCase):
    pass


class TestUnsecure(unittest.TestCase):
    pass
"""

