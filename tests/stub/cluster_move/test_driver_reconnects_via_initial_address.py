import time

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestDriverReconnectsUsingInitialAddress(TestkitTestCase):

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._servers = [StubServer(9010), StubServer(9020), StubServer(9030),
                         StubServer(9040), StubServer(9050), StubServer(9060)]
        self._uri = "neo4j://%s" % self._router.address
        self._auth = types.AuthorizationToken("basic", principal="",
                                              credentials="")

    def tearDown(self):
        self._router.reset()
        for s in self._servers:
            s.done()
        super().tearDown()

    def test_case(self):
        vars__ = {"#HOST#": self._router.host}
        self._router.start(path=self.script_path("router.script"),
                           vars_=vars__)

        for s in self._servers:
            s.start(path=self.script_path("server.script"))

        driver = Driver(self._backend, self._uri, self._auth)
        try:
            session1 = driver.session("r")
            session1.run("RETURN 1").consume()
            session1.close()

            self._router.reset()
            time.sleep(2)
            self._router.start(path=self.script_path("router2.script"),
                               vars_=vars__)

            session2 = driver.session("r")
            session2.run("RETURN 1").consume()
            session2.close()
        finally:
            driver.close()
            for s in self._servers:
                s.reset()

    def test_case_db(self):
        vars__ = {"#HOST#": self._router.host}
        self._router.start(path=self.script_path("router_db.script"),
                           vars_=vars__)
        for s in self._servers:
            s.start(path=self.script_path("server.script"))
        uri = "neo4j://%s" % self._router.address

        driver = Driver(self._backend, uri, self._auth)
        try:
            session1 = driver.session("r", database="db")
            session1.run("RETURN 1").consume()
            session1.close()

            self._router.reset()
            time.sleep(2)
            self._router.start(path=self.script_path("router_db2.script"),
                               vars_=vars__)

            session2 = driver.session("r", database="db")
            session2.run("RETURN 1").consume()
            session2.close()
        finally:
            driver.close()
            for s in self._servers:
                s.reset()
