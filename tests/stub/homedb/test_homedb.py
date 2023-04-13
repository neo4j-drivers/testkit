from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestHomeDb(TestkitTestCase):

    required_features = types.Feature.BOLT_4_4,

    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader1 = StubServer(9010)
        self._reader2 = StubServer(9011)
        self._auth_token = types.AuthorizationToken("basic", principal="p",
                                                    credentials="c")
        self._uri = "neo4j://%s" % self._router.address

    def tearDown(self):
        self._reader1.reset()
        self._reader2.reset()
        self._router.reset()
        super().tearDown()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_should_resolve_db_per_session_session_run(self):
        def _test():
            self._router.start(
                path=self.script_path("router_change_homedb.script"),
                vars_={"#HOST#": self._router.host}
            )

            self._reader1.start(
                path=self.script_path("reader_change_homedb.script")
            )

            driver = Driver(self._backend, self._uri, self._auth_token)

            session1 = driver.session("r", impersonated_user="the-imposter")
            result = session1.run("RETURN 1")
            result.consume()
            if not parallel_sessions:
                session1.close()

            session2 = driver.session(
                "r", bookmarks=["bookmark"], impersonated_user="the-imposter"
            )
            result = session2.run("RETURN 2")
            result.consume()
            session2.close()
            if parallel_sessions:
                session1.close()

            driver.close()

            self._router.done()
            self._reader1.done()

        for parallel_sessions in (True, False):
            with self.subTest(parallel_sessions=parallel_sessions):
                _test()
            self._router.reset()
            self._reader1.reset()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_should_resolve_db_per_session_tx_run(self):
        def _test():
            self._router.start(
                path=self.script_path("router_change_homedb.script"),
                vars_={"#HOST#": self._router.host}
            )

            self._reader1.start(
                path=self.script_path("reader_tx_change_homedb.script")
            )

            driver = Driver(self._backend, self._uri, self._auth_token)

            session1 = driver.session("r", impersonated_user="the-imposter")
            tx = session1.begin_transaction()
            result = tx.run("RETURN 1")
            result.consume()
            tx.commit()
            if not parallel_sessions:
                session1.close()

            session2 = driver.session(
                "r", bookmarks=["bookmark"], impersonated_user="the-imposter"
            )
            tx = session2.begin_transaction()
            result = tx.run("RETURN 2")
            result.consume()
            tx.commit()
            session2.close()
            if parallel_sessions:
                session1.close()

            driver.close()

            self._router.done()
            self._reader1.done()

        for parallel_sessions in (True, False):
            with self.subTest(parallel_sessions=parallel_sessions):
                _test()
            self._router.reset()
            self._reader1.reset()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_should_resolve_db_per_session_tx_func_run(self):
        def _test():
            def work(tx):
                result = tx.run(query)
                result.consume()

            self._router.start(
                path=self.script_path("router_change_homedb.script"),
                vars_={"#HOST#": self._router.host}
            )

            self._reader1.start(
                path=self.script_path("reader_tx_change_homedb.script")
            )

            driver = Driver(self._backend, self._uri, self._auth_token)

            session1 = driver.session("r", impersonated_user="the-imposter")
            query = "RETURN 1"
            session1.execute_read(work)
            if not parallel_sessions:
                session1.close()

            session2 = driver.session(
                "r", bookmarks=["bookmark"], impersonated_user="the-imposter"
            )
            query = "RETURN 2"
            session2.execute_read(work)
            session2.close()
            if parallel_sessions:
                session1.close()

            driver.close()

            self._router.done()
            self._reader1.done()

        for parallel_sessions in (True, False):
            with self.subTest(parallel_sessions=parallel_sessions):
                _test()
            self._router.reset()
            self._reader1.reset()

    @driver_feature(types.Feature.IMPERSONATION)
    def test_session_should_cache_home_db_despite_new_rt(self):
        i = 0

        def work(tx):
            nonlocal i
            i += 1
            if i == 1:
                with self.assertRaises(types.DriverError) as exc:
                    res = tx.run("RETURN 1")
                    return res.next()
                self._router.done()
                self._reader1.done()
                self._router.start(
                    path=self.script_path("router_explicit_homedb.script"),
                    vars_={"#HOST#": self._router.host}
                )
                self._reader2.start(
                    path=self.script_path("reader_tx_homedb.script")
                )
                raise exc.exception
            else:
                res = tx.run("RETURN 1")
                return res.next()

        driver = Driver(self._backend, self._uri, self._auth_token)

        self._router.start(
            path=self.script_path("router_homedb.script"),
            vars_={"#HOST#": self._router.host}
        )
        self._reader1.start(
            path=self.script_path("reader_tx_exits.script")
        )

        session = driver.session("r", impersonated_user="the-imposter")
        session.execute_read(work)
        session.close()

        driver.close()

        self._router.done()
        self._reader2.done()
        self.assertEqual(i, 2)
