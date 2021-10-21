from nutkit.frontend import Driver
import nutkit.protocol as types
from ...shared import get_driver_name
from .test_routing_v4x4 import RoutingV4x4


class RoutingV3(RoutingV4x4):
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

    def test_should_fail_on_empty_routing_response(self):
        # This test is BOLT v3 only because the server will return an Exception
        # instead of an empty routing table starting from version 4
        self.start_server(
            self._routingServer1,
            "router_yielding_empty_response_then_shuts_down.script"
        )
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        session = driver.session("w")
        failed = False
        try:
            session.run("RETURN 1 AS x").consume()
        except types.DriverError as e:
            failed = True
            if get_driver_name() in ['python']:
                self.assertEqual(
                    e.errorType,
                    "<class 'neo4j.exceptions.ServiceUnavailable'>"
                )
                self.assertIn("routing", e.msg)

        self.assertTrue(failed)

    def test_should_fail_with_routing_failure_on_invalid_bookmark_discovery_failure(
            self):
        pass
