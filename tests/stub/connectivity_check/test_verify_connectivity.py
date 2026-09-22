from nutkit import protocol as types
from tests.stub.connectivity_check import test_get_server_info


# Driver.VerifyConnectivity should do exactly what Driver.GetServerInfo does,
# except it should void the result.
class TestVerifyConnectivity(test_get_server_info.TestGetServerInfo):
    required_features = types.Feature.API_DRIVER_VERIFY_CONNECTIVITY,

    def _build_result_check_cb(self, *args, **kwargs):
        return None

    def _test_call(self, driver, result_check_cb=None):
        driver.verify_connectivity()
