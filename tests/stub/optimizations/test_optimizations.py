import re

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    TestkitTestCase,
    driver_feature,
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

    @driver_feature(types.Feature.OPT_PULL_PIPELINING,
                    types.Feature.BOLT_4_3)
    def test_pull_pipelining(self):
        def test():
            script = "pull_pipeline{}.script".format("_tx" if use_tx else "")
            self._server.start(path=self.script_path("v4x3", script),
                               vars={"#TYPE#": mode[0]})
            auth = types.AuthorizationToken("basic", principal="neo4j",
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
                    version="v4x3", consume=False,
                    check_single_connection=False, check_no_reset=False):
        assert use_tx in (None, "commit", "rollback")
        script = "run_twice{}{}.script".format("_tx" if use_tx else "",
                                               "_discard" if consume else "")
        self._server.start(path=self.script_path(version, script),
                           vars={"#TYPE#": mode[0]})
        if routing:
            self._router.start(path=self.script_path("v4x3", "router.script"),
                               vars={"#HOST#": self._router.host})
        auth = types.AuthorizationToken("basic", principal="neo4j",
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
                if consume:
                    res.consume()
                else:
                    results.append(
                        list(map(lambda r: r.value, res.next().values))
                    )
                if use_tx == "commit":
                    tx.commit()
                else:
                    tx.rollback()
            else:
                res = session.run("QUERY %i" % (i + 1))
                if consume:
                    res.consume()
                else:
                    results.append(
                        list(map(lambda r: r.value, res.next().values))
                    )

        session.close()
        driver.close()
        discard_count = \
            self._server.count_requests(re.compile(r"DISCARD(_ALL)?"))
        if use_tx:
            commit_count = self._server.count_requests("COMMIT")
            rollback_count = self._server.count_requests("ROLLBACK")
            if use_tx == "commit":
                self.assertEqual(commit_count, 2)
                self.assertEqual(rollback_count, 0)
            else:
                self.assertEqual(commit_count, 0)
                self.assertEqual(rollback_count, 2)
        if consume:
            self.assertEqual(discard_count, 2)
        else:
            self.assertEqual(results, [[1], [2]])
            self.assertEqual(discard_count, 0)
        self._server.done()
        self._router.reset()
        if check_single_connection:
            self.assertEqual(self._server.count_responses("<ACCEPT>"), 1)
        if check_no_reset:
            self.assertEqual(self._server.count_requests("RESET"), 0)
            self.assertEqual(self._router.count_requests("RESET"), 0)

    @driver_feature(types.Feature.OPT_CONNECTION_REUSE,
                    types.Feature.BOLT_4_3)
    def test_reuses_connection(self):
        for routing in (False, True):
            for mode in ("read", "write"):
                for use_tx in (None, "commit", "rollback"):
                    for new_session in (True, False):
                        with self.subTest(mode
                                          + (("_tx_" + use_tx) if use_tx
                                             else "")
                                          + ("_one_shot_session" if new_session
                                             else "_reuse_session")
                                          + ("_routing" if routing
                                             else "_direct")):
                            self.double_read(mode, new_session, use_tx, routing,
                                             check_single_connection=True)
                        self._server.reset()

    @driver_feature(types.Feature.OPT_MINIMAL_RESETS)
    def test_no_reset_on_clean_connection(self):
        mode = "write"
        for version in ("v4x3", "v3"):
            if not self.driver_supports_bolt(version):
                continue
            for consume in (True, False):
                if version == "v3" and consume:
                    # Drivers with types.Feature.OPT_PULL_PIPELINING will issue
                    # PULL_ALL, so there is nothing left to DISCARD. Since this
                    # is an old protocol version anyway, TestKit does not offer
                    # test coverage for this case.
                    continue
                for use_tx in (None, "commit", "rollback"):
                    for new_session in (True, False):
                        with self.subTest(mode
                                          + "_" + version
                                          + (("_tx_" + use_tx) if use_tx
                                             else "")
                                          + ("_one_shot_session" if new_session
                                             else"_reuse_session")
                                          + ("_discard" if consume
                                             else "_pull")):
                            self.double_read(mode, new_session, use_tx, False,
                                             version=version, consume=consume,
                                             check_no_reset=True)
                        self._server.reset()

    @driver_feature(types.Feature.OPT_MINIMAL_RESETS)
    def test_exactly_one_reset_on_failure(self):
        def test():
            script_path = self.script_path(
                version, "failure_on_{}.script".format(fail_on)
            )
            if routing:
                self._router.start(
                    path=self.script_path("v4x3", "router.script"),
                    vars={"#HOST#": self._router.host}
                )
            self._server.start(path=script_path)
            auth = types.AuthorizationToken("basic", principal="neo4j",
                                            credentials="pass")
            if routing:
                driver = Driver(self._backend,
                                "neo4j://%s" % self._router.address, auth)
            else:
                driver = Driver(self._backend,
                                "bolt://%s" % self._server.address, auth)
            session = driver.session("w")
            if use_tx:
                with self.assertRaises(types.DriverError):
                    tx = session.beginTransaction()
                    res = tx.run("CYPHER")
                    res.next()
            else:
                with self.assertRaises(types.DriverError):
                    res = session.run("CYPHER")
                    res.next()
            session.close()
            driver.close()
            self._server.done()
            if routing:
                self._router.done()
                reset_count_router = self._router.count_requests("RESET")
                self.assertEqual(reset_count_router, 0)
            reset_count = self._server.count_requests("RESET")
            self.assertEqual(reset_count, 1)

        for version in ("v3", "v4x3"):
            if not self.driver_supports_bolt(version):
                continue
            for use_tx in (False, True):
                for routing in (False, True):
                    for fail_on in ("pull", "run", "begin"):
                        if fail_on == "begin" and not use_tx:
                            continue
                        with self.subTest(version
                                          + ("_tx" if use_tx else "_autocommit")
                                          + "_{}".format(fail_on)
                                          + ("_routing"
                                             if routing else "_no_routing")):
                            test()
                        self._server.reset()
                        self._router.reset()

    @driver_feature(types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS)
    def test_uses_implicit_default_arguments(self):
        def test():
            if routing:
                self._router.start(
                    path=self.script_path(version, "all_default_router.script"),
                    vars={"#HOST#": self._router.host}
                )
                self._server.start(path=self.script_path(
                    version, "all_default_routing.script"
                ))
            else:
                self._server.start(path=self.script_path(version,
                                                         "all_default.script"))
            auth = types.AuthorizationToken("basic", principal="neo4j",
                                            credentials="pass")
            if routing:
                driver = Driver(self._backend,
                                "neo4j://%s" % self._router.address, auth)
            else:
                driver = Driver(self._backend,
                                "bolt://%s" % self._server.address, auth)
            session = driver.session("w")  # write is default
            if use_tx:
                tx = session.beginTransaction()
                res = tx.run("CYPHER")
                if consume:
                    res.consume()
                else:
                    res.next()
                    res.next()
                tx.commit()
            else:
                res = session.run("CYPHER")
                if consume:
                    res.consume()
                else:
                    res.next()
                    res.next()

            session.close()
            driver.close()
            self._server.done()
            if routing:
                self._router.done()

        for version in ("v4x3", "v4x4"):
            if not self.driver_supports_bolt(version):
                continue
            for use_tx in (True, False):
                for consume in (True, False):
                    for routing in (True, False):
                        with self.subTest(("tx" if use_tx else "auto_commit")
                                          + ("_discard" if consume else "_pull")
                                          + ("_routing"
                                             if routing else "_no_routing")):
                            test()
                        self._server.reset()
                        self._router.reset()

    @driver_feature(types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS)
    def test_uses_implicit_default_arguments_multi_query(self):
        def test():
            self._server.start(path=self.script_path(
                version, "all_default_multi_query.script"
            ))
            auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                            credentials="pass")
            driver = Driver(self._backend, "bolt://%s" % self._server.address,
                            auth)
            session = driver.session("w")  # write is default
            tx = session.beginTransaction()
            res = tx.run("CYPHER")
            if consume1:
                res.consume()
            else:
                res.next()
                res.next()
            res = tx.run("CYPHER")
            if consume2:
                res.consume()
            else:
                res.next()
                res.next()
            tx.commit()

            session.close()
            driver.close()
            self._server.done()

        for version in ("v4x3", "v4x4"):
            for consume1 in (True, False):
                for consume2 in (True, False):
                    with self.subTest(("discard1" if consume1 else "pull1")
                                      + ("_discard2" if consume2
                                         else "_pull2")):
                        test()
                    self._server.reset()
                    self._router.reset()

    @driver_feature(types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS)
    def test_uses_implicit_default_arguments_multi_query_nested(self):
        def test():
            self._server.start(path=self.script_path(
                version, "all_default_multi_query_nested.script"
            ))
            auth = types.AuthorizationToken(scheme="basic", principal="neo4j",
                                            credentials="pass")
            driver = Driver(self._backend, "bolt://%s" % self._server.address,
                            auth)
            session = driver.session("w")  # write is default
            tx = session.beginTransaction()
            res1 = tx.run("CYPHER")
            res1.next()
            res2 = tx.run("CYPHER")
            if consume2:
                res2.consume()
            else:
                res2.next()
                res2.next()
            if consume1:
                res1.consume()
            else:
                res1.next()
            tx.commit()

            session.close()
            driver.close()
            self._server.done()

        for version in ("v4x3", "v4x4"):
            if not self.driver_supports_bolt(version):
                continue
            for consume1 in (True, False):
                for consume2 in (True, False):
                    with self.subTest(("discard1" if consume1 else "pull1")
                                      + ("_discard2"
                                         if consume2 else "_pull2")):
                        test()
                    self._server.reset()
