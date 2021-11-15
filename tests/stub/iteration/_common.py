from contextlib import contextmanager

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class IterationTestBase(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    @contextmanager
    def _session(self, script_fn, fetch_size=2, vars_=None):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken(scheme="basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path("v4x0", script_fn),
                           vars_=vars_)
        try:
            session = driver.session("w", fetch_size=fetch_size)
            yield session
            session.close()
        finally:
            self._server.reset()
