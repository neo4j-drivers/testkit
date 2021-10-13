from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestHomeDb(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._router = StubServer(9000)
        self._reader1 = StubServer(9010)
        self._reader2 = StubServer(9011)
        self._authtoken = types.AuthorizationToken(
            "basic", principal="p", credentials="c")
        self._uri = "neo4j://%s" % self._router.address

    def tearDown(self):
        self._reader1.reset()
        self._reader2.reset()
        self._router.reset()
        super().tearDown()

    @driver_feature(types.Feature.IMPERSONATION, types.Feature.BOLT_4_4)
    def test_should_resolve_db_per_session(self):
        self._router.start(path=self.script_path(
            "router_change_homedb.script"), vars={"#HOST#": self._router.host})

        self._reader1.start(path=self.script_path(
            "reader_change_homedb.script"))

        driver = Driver(self._backend, self._uri, self._authtoken)

        session = driver.session("r", impersonatedUser="the-imposter")
        result = session.run("RETURN 1")
        result.consume()
        session.close()

        session2 = driver.session(
            "r", bookmarks=["bookmark"], impersonatedUser="the-imposter")
        result2 = session2.run("RETURN 2")
        result2.consume()
        session2.close()

        driver.close()

        self._router.done()
        self._reader1.done()

    @driver_feature(types.Feature.IMPERSONATION, types.Feature.BOLT_4_4)
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
                self._router.start(path=self.script_path(
                    "router_explicit_homedb.script"),
                    vars={"#HOST#": self._router.host})
                self._reader2.start(path=self.script_path(
                    "reader_tx_homedb.script"))
                raise exc.exception
            else:
                res = tx.run("RETURN 1")
                return res.next()

        driver = Driver(self._backend, self._uri, self._authtoken)

        self._router.start(
            path=self.script_path("router_homedb.script"),
            vars={"#HOST#": self._router.host}
        )
        self._reader1.start(
            path=self.script_path("reader_tx_exits.script")
        )

        session = driver.session("r", impersonatedUser="the-imposter")
        session.readTransaction(work)
        session.close()

        driver.close()

        self._router.done()
        self._reader2.done()
