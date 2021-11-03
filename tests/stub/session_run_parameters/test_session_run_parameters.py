import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    TestkitTestCase,
    driver_feature,
    get_driver_name,
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
        self._router = StubServer(9000)
        self._server = StubServer(9001)
        self._driver_name = get_driver_name()

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        self._router.reset()
        super().tearDown()

    def _start_servers_and_driver(self, script, routing, db, impersonation):
        if routing:
            router_script = "router%s%%s.script"
            if db:
                router_script = router_script % ("_" + db)
            else:
                router_script = router_script % "_default_db"
            if impersonation:
                router_script = router_script % "_impersonation"
            else:
                router_script = router_script % ""
            self._router.start(path=self.script_path(router_script),
                               vars={"#HOST#": self._router.host})
        if routing and not db:
            script += "_homedb.script"
        else:
            script += ".script"
        self._server.start(path=self.script_path(script))
        if routing:
            uri = "neo4j://%s" % self._router.address
        else:
            uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic", principal="",
                                                       credentials=""))

    def _run(self, script, routing, session_args=None, session_kwargs=None,
             run_args=None, run_kwargs=None):
        if session_args is None:
            session_args = ()
        if session_kwargs is None:
            session_kwargs = {}
        if run_args is None:
            run_args = ()
        if run_kwargs is None:
            run_kwargs = {}
        self._start_servers_and_driver(script, routing,
                                       session_kwargs.get("database"),
                                       session_kwargs.get("impersonatedUser"))
        session = self._driver.session(*session_args, **session_kwargs)
        try:
            result = session.run("RETURN 1 as n", *run_args, **run_kwargs)
            result.next()
            self._server.done()
        finally:
            self._server.reset()
            self._router.reset()
            session.close()
            self._driver.close()

    @driver_feature(types.Feature.BOLT_4_4)
    def test_access_mode_read(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._run("access_mode_read", routing,
                          session_args=("r",))

    @driver_feature(types.Feature.BOLT_4_4)
    def test_access_mode_write(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._run("access_mode_write", routing,
                          session_args=("w",))

    @driver_feature(types.Feature.BOLT_4_4)
    def test_parameters(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._run("parameters", routing,
                          session_args=("w",),
                          run_kwargs={"params": {"p": types.CypherInt(1)}})

    @driver_feature(types.Feature.BOLT_4_4)
    def test_bookmarks(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._run("bookmarks", routing,
                          session_args=("w",),
                          session_kwargs={"bookmarks": ["b1", "b2"]})

    @driver_feature(types.Feature.BOLT_4_4)
    def test_tx_meta(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._run("tx_meta", routing,
                          session_args=("w",),
                          run_kwargs={"txMeta": {"akey": "aval"}})

    @driver_feature(types.Feature.BOLT_4_4)
    def test_timeout(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._run("timeout", routing,
                          session_args=("w",), run_kwargs={"timeout": 17})

    @driver_feature(types.Feature.IMPERSONATION,
                    types.Feature.BOLT_4_4)
    def test_database(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._run("adb", routing,
                          session_args=("w",),
                          session_kwargs={"database": "adb"})

    @driver_feature(types.Feature.IMPERSONATION,
                    types.Feature.BOLT_4_4)
    def test_impersonation(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._run("imp_user", routing,
                          session_args=("w",),
                          session_kwargs={
                              "impersonatedUser": "that-other-dude"
                          })

    @driver_feature(types.Feature.IMPERSONATION,
                    types.Feature.BOLT_4_3)
    def test_impersonation_fails_on_v4x3(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                with self.assertRaises(types.DriverError) as exc:
                    self._run("imp_user_v4x3", routing,
                              session_args=("w",),
                              session_kwargs={
                                  "impersonatedUser": "that-other-dude"
                              })
                if self._driver_name in ["python"]:
                    self.assertEqual(
                        exc.exception.errorType,
                        "<class 'neo4j.exceptions.ConfigurationError'>"
                    )
                    self.assertIn("that-other-dude", exc.exception.msg)
                elif self._driver_name in ["java"]:
                    self.assertEqual(
                        exc.exception.errorType,
                        "org.neo4j.driver.exceptions.ClientException"
                    )
                elif self._driver_name in ["go"]:
                    self.assertIn("impersonation", exc.exception.msg)

    @driver_feature(types.Feature.IMPERSONATION,
                    types.Feature.BOLT_4_4)
    def test_combined(self):
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._run("combined", routing,
                          session_args=("r",),
                          session_kwargs={
                              "bookmarks": ["b0"],
                              "database": "adb",
                              "impersonatedUser": "that-other-dude"
                          },
                          run_kwargs={
                              "params": {"p": types.CypherInt(1)},
                              "txMeta": {"k": "v"}, "timeout": 11
                          })

    @driver_feature(types.Feature.BOLT_4_4)
    def test_empty_query(self):
        # Driver should pass empty string to server
        # TODO remove this block once all languages work
        if get_driver_name() in ["javascript", "java"]:
            self.skipTest("rejects empty string")
        for routing in (True, False):
            with self.subTest("routing" if routing else "direct"):
                self._start_servers_and_driver("empty_query", routing,
                                               None, None)
                session = self._driver.session("w")
                session.run("").next()
                session.close()
                self._driver.close()
                self._server.done()
