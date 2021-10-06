from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


# Verifies that session.run parameters are sent as expected on the wire.
# These are the different cases tests:
#   Read mode
#   Write mode
#   Bookmarks + write mode
#   Transaction meta data + write mode
#   Transaction timeout + write mode
#   Read mode + transaction meta data + transaction timeout + bookmarks
class TestSessionRunParameters(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic", principal="",
                                                       credentials=""))

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        super().tearDown()

    def _run(self, session_args=None, session_kwargs=None,
             run_args=None, run_kwargs=None):
        if session_args is None:
            session_args = ()
        if session_kwargs is None:
            session_kwargs = {}
        if run_args is None:
            run_args = ()
        if run_kwargs is None:
            run_kwargs = {}
        session = self._driver.session(*session_args, **session_kwargs)
        try:
            result = session.run("RETURN 1 as n", *run_args, **run_kwargs)
            result.next()
        finally:
            session.close()

    def _start_server(self, script):
        self._server.start(path=self.script_path(script))

    def test_access_mode_read(self):
        self._start_server("access_mode_read.script")
        self._run(session_args=("r",))
        self._driver.close()
        self._server.done()

    def test_access_mode_write(self):
        self._start_server("access_mode_write.script")
        self._run(session_args=("w",))
        self._driver.close()
        self._server.done()

    def test_parameters(self):
        self._start_server("parameters.script")
        self._run(session_args=("w",),
                  run_kwargs={"params": {"p": types.CypherInt(1)}})
        self._driver.close()
        self._server.done()

    def test_bookmarks(self):
        self._start_server("bookmarks.script")
        self._run(session_args=("w",),
                  session_kwargs={"bookmarks": ["b1", "b2"]})
        self._driver.close()
        self._server.done()

    def test_tx_meta(self):
        self._start_server("tx_meta.script")
        self._run(session_args=("w",),
                  run_kwargs={"txMeta": {"akey": "aval"}})
        self._driver.close()
        self._server.done()

    def test_timeout(self):
        self._start_server("timeout.script")
        self._run(session_args=("w",), run_kwargs={"timeout": 17})
        self._driver.close()
        self._server.done()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_impersonation(self):
        self._start_server("imp_user.script")
        self._run(session_args=("w",),
                  session_kwargs={"impersonatedUser": "that-other-dude"})
        self._driver.close()
        self._server.done()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_combined(self):
        self._start_server("combined.script")
        self._run(session_args=("r",),
                  session_kwargs={
                      "bookmarks": ["b0"],
                      "impersonatedUser": "that-other-dude"
                  },
                  run_kwargs={
                      "params": {"p": types.CypherInt(1)},
                      "txMeta": {"k": "v"}, "timeout": 11
                  })
        self._driver.close()
        self._server.done()

    def test_empty_query(self):
        if get_driver_name() in ["javascript", "java"]:
            self.skipTest("rejects empty string")
        # Driver should pass empty string to server
        self._start_server("empty_query.script")
        session = self._driver.session("w")
        session.run("").next()
        session.close()
        self._driver.close()
        self._server.done()
