import json

from nutkit.frontend import Driver
from .test_routing_v4x3 import RoutingV4x3


class RoutingV4x1(RoutingV4x3):
    bolt_version = "4.1"
    server_agent = "Neo4j/4.1.0"

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
        }

        return v

    def route_call_count(self, server):
        return server.count_requests(
            'RUN "CALL dbms.routing.getRoutingTable('
        )

    # Ignore this on older protocol versions than 4.3
    def test_should_read_successfully_from_reader_using_session_run_with_default_db_driver(self):
        pass

    def test_should_send_system_bookmark_with_route(self):
        pass

    def test_should_pass_system_bookmark_when_getting_rt_for_multi_db(self):
        # passing bookmarks of the system db when fetching the routing table
        # makes sure that newly (asynchronously) created databases exist.
        # (causal consistency on database existence)
        bookmarks = ["sys:1234", "foo:5678"]

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1, "router_with_bookmarks.script",
            vars_={
                "#BOOKMARKS#": ', "bookmarks{}": ' + json.dumps(bookmarks),
                **self.get_vars()
            }
        )
        self.start_server(self._readServer1, "reader_with_bookmarks.script")

        session = driver.session('r', database=self.adb,
                                 bookmarks=bookmarks)
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        last_bookmarks = session.lastBookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)
        self.assertEqual(["foo:6678"], last_bookmarks)

    def test_should_ignore_system_bookmark_when_getting_rt_for_multi_db(self):
        pass
