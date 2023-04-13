from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class _TestTemporalTypes(TestkitTestCase):

    required_features = types.Feature.API_TYPE_TEMPORAL,
    bolt_version = None

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

    def _start_server(self, script):
        version_folder = "v{}".format(self.bolt_version.replace(".", "x"))
        self._server.start(path=self.script_path(version_folder, script))

    def _create_direct_driver(self):
        uri = "bolt://%s" % self._server.address
        auth = types.AuthorizationToken("basic", principal="", credentials="")
        self._driver = Driver(self._backend, uri, auth)

    def _test_date_time(self, patched):
        script = "echo_date_time.script"
        if patched:
            script = "echo_date_time_patched.script"
        self._start_server(script)
        self._create_direct_driver()
        self._session = self._driver.session("w")
        dt = types.CypherDateTime(2022, 6, 7, 11, 52, 5, 0, utc_offset_s=7200)
        result = self._session.run("RETURN $dt AS dt", params={"dt": dt})
        records = list(result)
        self.assertEqual(len(records), 1)
        self.assertEqual(len(records[0].values), 1)
        self.assertEqual(dt, records[0].values[0])

    def _test_zoned_date_time(self, patched):
        script = "echo_zoned_date_time.script"
        if patched:
            script = "echo_zoned_date_time_patched.script"
        self._start_server(script)
        self._create_direct_driver()
        self._session = self._driver.session("w")
        dt = types.CypherDateTime(
            2022, 6, 7, 11, 52, 5, 0,
            utc_offset_s=7200, timezone_id="Europe/Stockholm"
        )
        result = self._session.run("RETURN $dt AS dt", params={"dt": dt})
        records = list(result)
        self.assertEqual(len(records), 1)
        self.assertEqual(len(records[0].values), 1)
        self.assertEqual(dt, records[0].values[0])

    def _test_unknown_zoned_date_time(self, patched):
        tx_count = 0

        def work(tx):
            nonlocal tx_count
            tx_count += 1
            res = tx.run("MATCH (n:State) RETURN n.founded AS dt")
            with self.assertRaises(types.DriverError) as exc:
                res.next()
            self.assertIn("Europe/Neo4j", exc.exception.msg)
            raise exc.exception

        script = "echo_unknown_zoned_date_time.script"
        if patched:
            script = "echo_unknown_zoned_date_time_patched.script"
        self._start_server(script)
        self._create_direct_driver()
        self._session = self._driver.session("w")
        with self.assertRaises(types.DriverError):
            self._session.execute_read(work)
        self._server.done()
        # TODO: Remove once all drivers roll back failed managed transactions
        if get_driver_name() not in ["go"]:
            rollback_count = self._server.count_requests("ROLLBACK")
            self.assertEqual(rollback_count, 1)
        self.assertEqual(tx_count, 1)

    def _test_unknown_then_known_zoned_date_time(self, patched):
        def work(tx):
            res = tx.run("MATCH (n:State) RETURN n.founded AS dt")
            with self.assertRaises(types.DriverError) as exc:
                res.next()
            self.assertIn("Europe/Neo4j", exc.exception.msg)

            rec = res.next()
            self.assertIsInstance(rec, types.Record)
            self.assertEqual(len(rec.values), 1)
            self.assertEqual(
                rec.values[0],
                types.CypherDateTime(1970, 1, 1, 1, 0, 0, 0, utc_offset_s=1800)
            )

        script = "echo_unknown_then_known_zoned_date_time.script"
        if patched:
            script = "echo_unknown_then_known_zoned_date_time_patched.script"
        self._start_server(script)
        self._create_direct_driver()
        self._session = self._driver.session("w")
        self._session.execute_read(work)


class TestTemporalTypesV3x0(_TestTemporalTypes):

    required_features = (
        *_TestTemporalTypes.required_features,
        types.Feature.BOLT_3_0,
    )
    bolt_version = "3.0"

    def test_date_time(self):
        super()._test_date_time(patched=False)

    def test_zoned_date_time(self):
        super()._test_zoned_date_time(patched=False)


class TestTemporalTypesV4x1(_TestTemporalTypes):
    required_features = (
        *_TestTemporalTypes.required_features,
        types.Feature.BOLT_4_1,
    )
    bolt_version = "4.1"

    def test_date_time(self):
        super()._test_date_time(patched=False)

    def test_zoned_date_time(self):
        super()._test_zoned_date_time(patched=False)


class TestTemporalTypesV4x2(_TestTemporalTypes):

    required_features = (
        *_TestTemporalTypes.required_features,
        types.Feature.BOLT_4_2,
    )
    bolt_version = "4.2"

    def test_date_time(self):
        super()._test_date_time(patched=False)

    def test_zoned_date_time(self):
        super()._test_zoned_date_time(patched=False)


class TestTemporalTypesV4x3(_TestTemporalTypes):

    required_features = (
        *_TestTemporalTypes.required_features,
        types.Feature.BOLT_4_3,
    )
    bolt_version = "4.3"

    def test_date_time(self):
        super()._test_date_time(patched=False)

    @driver_feature(types.Feature.BOLT_PATCH_UTC)
    def test_date_time_with_patch(self):
        super()._test_date_time(patched=True)

    def test_zoned_date_time(self):
        super()._test_zoned_date_time(patched=False)

    @driver_feature(types.Feature.BOLT_PATCH_UTC)
    def test_zoned_date_time_with_patch(self):
        super()._test_zoned_date_time(patched=True)


class TestTemporalTypesV4x4(_TestTemporalTypes):

    required_features = (
        *_TestTemporalTypes.required_features,
        types.Feature.BOLT_4_4,
    )
    bolt_version = "4.4"

    def test_date_time(self):
        super()._test_date_time(patched=False)

    @driver_feature(types.Feature.BOLT_PATCH_UTC)
    def test_date_time_with_patch(self):
        super()._test_date_time(patched=True)

    def test_zoned_date_time(self):
        super()._test_zoned_date_time(patched=False)

    @driver_feature(types.Feature.BOLT_PATCH_UTC)
    def test_zoned_date_time_with_patch(self):
        super()._test_zoned_date_time(patched=True)

    def test_unknown_zoned_date_time(self):
        super()._test_unknown_zoned_date_time(patched=False)

    @driver_feature(types.Feature.BOLT_PATCH_UTC)
    def test_unknown_zoned_date_time_patched(self):
        super()._test_unknown_zoned_date_time(patched=True)

    def test_unknown_then_known_zoned_date_time(self):
        super()._test_unknown_then_known_zoned_date_time(patched=False)

    @driver_feature(types.Feature.BOLT_PATCH_UTC)
    def test_unknown_then_known_zoned_date_time_patched(self):
        super()._test_unknown_then_known_zoned_date_time(patched=True)


class TestTemporalTypesV5x0(_TestTemporalTypes):

    required_features = (
        *_TestTemporalTypes.required_features,
        types.Feature.BOLT_5_0,
    )
    bolt_version = "5.0"

    def test_date_time(self):
        super()._test_date_time(patched=False)

    def test_zoned_date_time(self):
        super()._test_zoned_date_time(patched=False)

    def test_unknown_zoned_date_time(self):
        super()._test_unknown_zoned_date_time(patched=False)

    def test_unknown_then_known_zoned_date_time(self):
        super()._test_unknown_then_known_zoned_date_time(patched=False)
