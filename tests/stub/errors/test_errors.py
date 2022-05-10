from nutkit import protocol as types
from nutkit.frontend import Driver
from nutkit.protocol import Feature
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestErrors(TestkitTestCase):
    required_features = Feature.DETAIL_MAPS_ERROR_CODE,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        self._session = None

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def _test(self, script, throw_code, expected_code):
        params = {
            "#ERROR_CODE#": throw_code
        }
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path(script),
                           vars_=params)
        session = driver.session("r", fetch_size=1)

        with self.assertRaises(types.DriverError) as err:
            result_handle = session.run("MATCH (n) RETURN n LIMIT 1")
            result_handle.next()

        self.assertEqual(expected_code,
                         err.exception.code)

        session.close()
        driver.close()
        self._server.done()
        self._server.reset()

    @driver_feature(types.Feature.BOLT_4_4)
    def test_terminated_4x4_throws_as_client_error(self):
        self._test("test_error.script",
                   "Neo.TransientError.Transaction.Terminated",
                   "Neo.ClientError.Transaction.Terminated")

    @driver_feature(types.Feature.BOLT_5_0)
    def test_terminated_5x0_throws_as_client_error(self):
        self._test("test_error.script",
                   "Neo.ClientError.Transaction.Terminated",
                   "Neo.ClientError.Transaction.Terminated")

    @driver_feature(types.Feature.BOLT_4_4)
    def test_lock_client_stopped_4x4_throws_as_client_error(self):
        self._test("test_error.script",
                   "Neo.TransientError.Transaction.LockClientStopped",
                   "Neo.ClientError.Transaction.LockClientStopped")

    @driver_feature(types.Feature.BOLT_5_0)
    def test_lock_client_stopped_5x0_throws_as_client_error(self):
        self._test("test_error.script",
                   "Neo.ClientError.Transaction.LockClientStopped",
                   "Neo.ClientError.Transaction.LockClientStopped")
