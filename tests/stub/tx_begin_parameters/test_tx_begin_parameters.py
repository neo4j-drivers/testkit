from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


# Verifies that session.beginTransaction parameters are sent as expected over
# the wire. These are the different cases tests:
#   Read mode
#   Write mode
#   Bookmarks + write mode
#   Transaction meta data + write mode
#   Transaction timeout + write mode
#   Read mode + transaction meta data + transaction timeout + bookmarks
class TestTxBeginParameters(TestkitTestCase):
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
        self._driver.close()
        self._server.reset()
        super().tearDown()

    def _run(self, tx_func_access_mode=None,
             session_args=None, session_kwargs=None,
             tx_args=None, tx_kwargs=None,
             run_args=None, run_kwargs=None):
        def work(tx_):
            # Need to do something on the tx, driver might do lazy begin
            list(tx_.run("RETURN 1 as n", *run_args, **run_kwargs))

        if session_args is None:
            session_args = ()
        if session_kwargs is None:
            session_kwargs = {}
        if tx_args is None:
            tx_args = ()
        if tx_kwargs is None:
            tx_kwargs = {}
        if run_args is None:
            run_args = ()
        if run_kwargs is None:
            run_kwargs = {}
        # FIXME: params are not used and dotnet fails when using them
        session = self._driver.session(*session_args, **session_kwargs)
        if tx_func_access_mode is None:
            tx = session.beginTransaction(*tx_args, **tx_kwargs)
            work(tx)
            tx.commit()
        elif tx_func_access_mode == "w":
            session.writeTransaction(work, *tx_args, **tx_kwargs)
        elif tx_func_access_mode == "r":
            session.readTransaction(work, *tx_args, **tx_kwargs)
        else:
            raise ValueError(tx_func_access_mode)
        session.close()

    def _start_server(self, script):
        self._server.start(path=self.script_path(script))

    def test_access_mode_read(self):
        self._start_server("access_mode_read.script")
        self._run(session_args=("r",))
        self._server.done()

    def test_tx_func_access_mode_read(self):
        for session_access_mode in ("r", "w"):
            with self.subTest("session_mode_" + session_access_mode):
                self._start_server("access_mode_read.script")
                try:
                    self._run(session_args=(session_access_mode[0],),
                              tx_func_access_mode="r")
                    self._server.done()
                finally:
                    self._server.reset()

    def test_access_mode_write(self):
        self._start_server("access_mode_write.script")
        self._run(session_args=("w",))
        self._server.done()

    def test_tx_func_access_mode(self):
        for session_access_mode in ("r", "w"):
            with self.subTest("session_mode_" + session_access_mode):
                self._start_server("access_mode_write.script")
                try:
                    self._run(session_args=(session_access_mode[0],),
                              tx_func_access_mode="w")
                    self._server.done()
                finally:
                    self._server.reset()

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
        self._server.done()

    def test_tx_meta(self):
        self._start_server("tx_meta.script")
        self._run(session_args=("w",), tx_kwargs={"txMeta": {"akey": "aval"}})
        self._server.done()

    def test_timeout(self):
        self._start_server("timeout.script")
        self._run(session_args=("w",), tx_kwargs={"timeout": 17})
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
                  run_kwargs={"params": {"p": types.CypherInt(1)}},
                  session_kwargs={"bookmarks": ["b0"]},
                  tx_kwargs={"txMeta": {"k": "v"}, "timeout": 11})
        self._server.done()

