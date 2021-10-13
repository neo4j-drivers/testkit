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
        self._reader = StubServer(9010)
        self._authtoken = types.AuthorizationToken(
            "basic", principal="p", credentials="c")
        self._uri = "neo4j://%s" % self._router.address

    def tearDown(self):
        self._reader.reset()
        self._router.reset()
        super().tearDown()

    @driver_feature(types.Feature.IMPERSONATION, types.Feature.BOLT_4_4)
    def test_should_resolve_db_per_session(self):
        self._router.start(path=self.script_path(
            "router_change_homedb.script"), vars={"#HOST#": self._router.host})

        self._reader.start(path=self.script_path(
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
