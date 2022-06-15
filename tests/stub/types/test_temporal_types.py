from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestTemporalTypesV4x4(TestkitTestCase):

    required_features = types.Feature.BOLT_4_4,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._session = None
        self._driver = None

    def tearDown(self):
        if self._session is not None:
            self._session.close()
        if self._driver is not None:
            self._driver.close()
        self._server.reset()
        super().tearDown()

    def _create_direct_driver(self):
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic", principal="",
                                                       credentials=""))

    def test_date_time(self):
        self._server.start(
            path=self.script_path("v4x4", "echo_date_time.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("w")
        result = self._session.run("RETURN $dt AS dt", params={
            "dt": types.CypherDateTime(2022, 6, 7, 11, 52, 5, 0,
                                       utc_offset_s=7200)
        })
        list(result)

    @driver_feature(types.Feature.BOLT_PATCH_UTC)
    def test_date_time_with_patch(self):
        self._server.start(
            path=self.script_path("v4x4", "echo_date_time_patched.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("w")
        result = self._session.run("RETURN $dt AS dt", params={
            "dt": types.CypherDateTime(2022, 6, 7, 11, 52, 5, 0,
                                       utc_offset_s=7200)
        })
        list(result)

    def test_zoned_date_time(self):
        self._server.start(
            path=self.script_path("v4x4", "echo_zoned_date_time.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("w")
        result = self._session.run("RETURN $dt AS dt", params={
            "dt": types.CypherDateTime(
                2022, 6, 7, 11, 52, 5, 0,
                utc_offset_s=7200, timezone_id="Europe/Stockholm"
            )
        })
        list(result)

    @driver_feature(types.Feature.BOLT_PATCH_UTC)
    def test_zoned_date_time_with_patch(self):
        self._server.start(
            path=self.script_path("v4x4",
                                  "echo_zoned_date_time_patched.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("w")
        result = self._session.run("RETURN $dt AS dt", params={
            "dt": types.CypherDateTime(
                2022, 6, 7, 11, 52, 5, 0,
                utc_offset_s=7200, timezone_id="Europe/Stockholm"
            )
        })
        list(result)


class TestTemporalTypesV5x0(TestkitTestCase):

    required_features = types.Feature.BOLT_5_0,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._session = None
        self._driver = None

    def tearDown(self):
        if self._session is not None:
            self._session.close()
        if self._driver is not None:
            self._driver.close()
        self._server.reset()
        super().tearDown()

    def _create_direct_driver(self):
        uri = "bolt://%s" % self._server.address
        self._driver = Driver(self._backend, uri,
                              types.AuthorizationToken("basic",
                                                       principal="",
                                                       credentials=""))

    def test_date_time(self):
        self._server.start(
            path=self.script_path("v5x0", "echo_date_time.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("w")
        result = self._session.run("RETURN $dt AS dt", params={
            "dt": types.CypherDateTime(2022, 6, 7, 11, 52, 5, 0,
                                       utc_offset_s=7200)
        })
        list(result)

    def test_zoned_date_time(self):
        self._server.start(
            path=self.script_path("v5x0", "echo_zoned_date_time.script")
        )
        self._create_direct_driver()
        self._session = self._driver.session("w")
        result = self._session.run("RETURN $dt AS dt", params={
            "dt": types.CypherDateTime(
                2022, 6, 7, 11, 52, 5, 0,
                utc_offset_s=7200, timezone_id="Europe/Stockholm"
            )
        })
        list(result)
