from nutkit import protocol as types
from tests.stub.routing.test_no_routing_v4x1 import NoRoutingV4x1


class NoRoutingV3(NoRoutingV4x1):

    required_features = types.Feature.BOLT_3_0,
    bolt_version = "3"
    version_dir = "v3_no_routing"
    server_agent = "Neo4j/3.5.0"
    adb = None

    def get_vars(self):
        return {
            "#VERSION#": "3",
            "#SERVER_AGENT#": "Neo4j/3.5.0",
            "#USER_AGENT#": "007",
            "#ROUTING#": ""
        }

    def test_should_accept_custom_fetch_size_using_driver_configuration(self):
        pass

    def test_should_accept_custom_fetch_size_using_session_configuration(self):
        pass

    def test_should_pull_custom_size_and_then_all_using_session_configuration(
            self):
        pass

    def test_should_read_successfully_with_database_name_using_session_run(
            self):
        pass

    def test_should_read_successfully_with_database_name_using_tx_function(
            self):
        pass

    def _assert_supports_multi_db(self, supports_multi_db):
        self.assertFalse(supports_multi_db)
