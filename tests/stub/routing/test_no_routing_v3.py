from tests.shared import (
    get_driver_name,
)
from ._routing import get_extra_hello_props
from .test_no_routing_v4x1 import NoRoutingV4x1


class NoRoutingV3(NoRoutingV4x1):
    bolt_version = "3"
    version_dir = "v3_no_routing"
    server_agent = "Neo4j/3.5.0"
    adb = None

    def get_vars(self):
        return {
            "#VERSION#": "3",
            "#SERVER_AGENT#": "Neo4j/3.5.0",
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
            "#USER_AGENT#": '007',
            "#ROUTING#": ''
        }

    def test_should_accept_custom_fetch_size_using_driver_configuration(self):
        pass

    def test_should_accept_custom_fetch_size_using_session_configuration(
            self):
        pass
