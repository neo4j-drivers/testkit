from tests.shared import get_driver_name
from ._routing import get_extra_hello_props
from .test_routing_v4x3 import RoutingV4x3


class RoutingV3(RoutingV4x3):
    bolt_version = "3"
    server_agent = "Neo4j/3.5.0"

    def get_vars(self, host=None):
        if host is None:
            host = self._routingServer1.host
        v = {
            "#VERSION#": self.bolt_version,
            "#HOST#": host,
            "#SERVER_AGENT#": self.server_agent,
            "#ROUTINGCTX#": (
                '{"address": "' + host
                + ':9000", "region": "china", "policy": "my_policy"}'
            ),
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
            "#EXTR_HELLO_ROUTING_PROPS#": "",
            "#EXTR_HELLO_ROUTING_PROPS_EMPTY_CTX#": ""
        }

        if get_driver_name() in ['java']:
            v["#EXTR_HELLO_ROUTING_PROPS#"] = \
                ', "routing": ' + v['#ROUTINGCTX#']
            v["#EXTR_HELLO_ROUTING_PROPS_EMPTY_CTX#"] = \
                ', "routing": {"address": "' + host + ':9000"}'

        return v

    adb = None

    def route_call_count(self, server):
        return server.count_requests(
            'RUN "CALL dbms.cluster.routing.getRoutingTable('
        )

    def should_support_multi_db(self):
        return False

    def test_should_read_successfully_from_reachable_db_after_trying_unreachable_db(self):
        pass

    def test_should_pass_system_bookmark_when_getting_rt_for_multi_db(self):
        pass

    def test_should_send_system_bookmark_with_route(self):
        pass