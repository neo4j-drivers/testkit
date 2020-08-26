import subprocess, platform, os, time, sys
from tests.shared import *
from nutkit.frontend import Driver, AuthorizationToken

# Retrieve path to the repository containing this script.
# Use this path as base for locating a whole bunch of other stuff.
thisPath = os.path.dirname(os.path.abspath(__file__))

env_host_address = "TEST_TLSSERVER_ADDRESS"


class TlsServer:
    def __init__(self, server_cert):
        """ Name of server certificate, corresponds to a .pem and .key file.
        """

        # Determine which address that server should bind to
        addr = os.environ.get(env_host_address, "127.0.0.1")

        scriptPath = os.path.join(thisPath, "..", "..", "tlsserver", "tlsserver")
        certPath = os.path.join(thisPath, "certs", "server", "%s.pem" % server_cert)
        keyPath = os.path.join(thisPath, "certs", "server", "%s.key" % server_cert)
        self._process = subprocess.Popen([scriptPath, addr+":6666", certPath, keyPath],
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         close_fds=True,
                                         encoding='utf-8')
        # Wait until something is written to know it started
        line = self._process.stdout.readline()
        print(line)
        print("TLS server started")

    def _close_pipes(self):
        self._process.stdout.close()
        self._process.stderr.close()

    def connected(self):
        """ Checks that the server has stopped and its exit code to determine if
        driver connected or not.
        """
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


def try_connect(backend, server, scheme, host):
    url = "%s://%s:%d" % (scheme, host, 6666)
    # Doesn't really matter
    auth = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
    driver = Driver(backend, url, auth)
    session = driver.session("r")
    try:
        session.run("RETURN 1")
    except:
        pass
    session.close()
    driver.close()
    return server.connected()
