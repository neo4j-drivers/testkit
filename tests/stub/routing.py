import unittest, os

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


class Routing(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._driverName = get_driver_name()
        self._routingServer = StubServer(9001)
        self._routingServer.start(os.path.join(scripts_path, "router.script"))
        self._readServer = StubServer(9002)
        self._writeServer = StubServer(9003)
        uri = "neo4j://%s" % self._routingServer.address
        # Driver is configured to talk to "routing" stub server
        self._driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))

    def tearDown(self):
        self._driver.close()
        self._backend.close()
        self._routingServer.reset()
        self._readServer.reset()
        self._writeServer.reset()

    # Checks that routing is used to connect to correct server and that parameters for
    # session run is passed on to the target server (not the router).
    def test_session_run_read(self):
        script = "sessionrun_accessmode_read.script"
        if self._driverName in ["go"]:
            script = "sessionrun_accessmode_read_pull_all.script"
        self._readServer.start(os.path.join(scripts_path, script))
        session = self._driver.session('r')
        session.run("RETURN 1 as n")
        session.close()

    # Same test as for session.run but for transaction run.
    def test_tx_run_read(self):
        script = "txbegin_accessmode_read.script"
        if self._driverName in ["go"]:
            script = "txbegin_accessmode_read_pull_all.script"
        self._readServer.start(os.path.join(scripts_path, script))
        session = self._driver.session('r')
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()

    # Checks that write server is used
    def test_session_run_write(self):
        script = "sessionrun_accessmode_write.script"
        if self._driverName in ["go"]:
            script = "sessionrun_accessmode_write_pull_all.script"
        self._writeServer.start(os.path.join(scripts_path, script))
        session = self._driver.session('w')
        session.run("RETURN 1 as n")
        session.close()

    # Checks that write server is used
    def test_tx_run_write(self):
        script = "txbegin_accessmode_write.script"
        if self._driverName in ["go"]:
            script = "txbegin_accessmode_write_pull_all.script"
        self._writeServer.start(os.path.join(scripts_path, script))
        session = self._driver.session('w')
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()

