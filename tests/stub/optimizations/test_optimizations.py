from nutkit.frontend import Driver
import nutkit.protocol as types

from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestOptimizations(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._router = StubServer(9000)

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        self._router.reset()
        super().tearDown()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_pull_pipelining(self):
        def test():
            script = "pull_pipeline{}.script".format("_tx" if use_tx else "")
            self._server.start(path=self.script_path(script),
                               vars={"#TYPE#": mode[0]})
            auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                            credentials="pass")
            driver = Driver(self._backend, "bolt://%s" % self._server.address,
                            auth)

            session = driver.session(mode[0])
            if use_tx:
                tx = session.beginTransaction()
                res = tx.run("CYPHER")
                result = list(map(lambda r: r.value, res.next().values))
                tx.commit()
            else:
                res = session.run("CYPHER")
                result = list(map(lambda r: r.value, res.next().values))
            self.assertEqual(result, [1])

            session.close()
            driver.close()
            self._server.done()

        for mode in ("read", "write"):
            for use_tx in (True, False):
                with self.subTest(mode + ("_tx" if use_tx else "")):
                    test()
                self._server.reset()

    def double_read(self, mode, new_session, use_tx, routing,
                    check_single_connection, check_no_reset):
        script = "run_twice{}.script".format("_tx" if use_tx else "")
        self._server.start(path=self.script_path(script),
                           vars={"#TYPE#": mode[0]})
        if routing:
            self._router.start(path=self.script_path("router.script"),
                               vars={"#HOST#": self._router.host})
        auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                        credentials="pass")
        if routing:
            uri = "neo4j://%s" % self._router.address
        else:
            uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri, auth)

        results = []
        session = None
        for i in range(2):
            if session is None:
                session = driver.session(mode[0])
            elif new_session:
                session.close()
                session = driver.session(mode[0])
            if use_tx:
                tx = session.beginTransaction()
                res = tx.run("QUERY %i" % (i + 1))
                results.append(
                    list(map(lambda r: r.value, res.next().values))
                )
                tx.commit()
            else:
                res = session.run("QUERY %i" % (i + 1))
                results.append(
                    list(map(lambda r: r.value, res.next().values))
                )

        session.close()
        driver.close()
        self.assertEqual(results, [[1], [2]])
        self._server.done()
        self._router.reset()
        if check_single_connection:
            self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
        if check_no_reset:
            self.assertEqual(self._server.count_requests("RESET"), 0)

    @driver_feature(types.Feature.OPT_CONNECTION_REUSE)
    def test_reuses_connection(self):
        for routing in (False, True):
            for mode in ("read", "write"):
                for use_tx in (True, False):
                    for new_session in (True, False):
                        with self.subTest(mode
                                          + ("_tx" if use_tx else "")
                                          + ("_one_shot_session" if new_session
                                             else "_reuse_session")
                                          + ("_routing" if routing
                                             else "_direct")):
                            self.double_read(mode, new_session, use_tx, routing,
                                             True, False)
                        self._server.reset()

    @driver_feature(types.Feature.OPT_MINIMAL_RESETS)
    def test_no_reset_on_clean_connection(self):
        mode = "write"
        for use_tx in (True, False):
            for new_session in (True, False):
                with self.subTest(mode
                                  + ("_tx" if use_tx else "")
                                  + ("_one_shot_session" if new_session
                                     else"_reuse_session")):
                    self.double_read(mode, new_session, use_tx, False,
                                     False, True)
                self._server.reset()

    @driver_feature(types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS)
    def test_uses_implicit_default_arguments(self):
        def test():
            self._server.start(path=self.script_path("all_default.script"))
            auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                            credentials="pass")
            driver = Driver(self._backend, "bolt://%s" % self._server.address,
                            auth)
            session = driver.session("w")  # write is default
            if use_tx:
                tx = session.beginTransaction()
                res = tx.run("CYPHER")
                res.next()
                tx.commit()
            else:
                res = session.run("CYPHER")
                res.next()

            session.close()
            driver.close()
            self._server.done()

        for use_tx in (True, False):
            with self.subTest("tx" if use_tx else "auto_commit"):
                test()
            self._server.reset()
