import json
import re

from nutkit.frontend import (
    Driver,
    Neo4jBookmarkManagerConfig,
)
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestNeo4jBookmarkManager(TestkitTestCase):
    required_features = (
        types.Feature.BOLT_5_0,
        types.Feature.API_BOOKMARK_MANAGER,
    )

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._router = StubServer(9000)
        self._driver = None

    def tearDown(self):
        self._server.reset()
        self._router.reset()

        if self._driver:
            self._driver.close()

        return super().tearDown()

    def test_should_keep_track_of_session_run(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "session_run_chaining.script")

        self._driver = self._new_driver()

        s1 = self._driver.session("w")
        s1.run("QUERY1").consume()
        s1.close()

        s2 = self._driver.session("w")
        s2.run("QUERY2").consume()
        s2.close()

        s3 = self._driver.session("w")
        s3.run("QUERY3").consume()
        s3.close()

        self._router.reset()

        run_requests = self._server.get_requests("RUN")

        self.assertEqual(len(run_requests), 3)
        self.assert_run(run_requests[0])
        self.assert_run(
            run_requests[1],
            bookmarks=["bm1"]
        )
        self.assert_run(
            run_requests[2],
            bookmarks=["bm2"]
        )

    def test_should_keep_track_of_tx_in_sequence(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()

        s1 = self._driver.session("w")
        tx1 = s1.begin_transaction({"order": "1st"})
        tx1.run("RETURN 1 as n").consume()
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w")
        tx2 = s2.begin_transaction({"order": "2nd"})
        tx2.run("RETURN 1 as n").consume()
        tx2.commit()
        s2.close()

        s3 = self._driver.session("w")
        tx3 = s3.begin_transaction({"order": "2nd"})
        tx3.run("RETURN 1 as n").consume()
        tx3.commit()
        s3.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 3)
        self.assert_begin(begin_requests[0])
        self.assert_begin(
            begin_requests[1],
            bookmarks=["bm1"]
        )
        self.assert_begin(
            begin_requests[2],
            bookmarks=["bm2"]
        )

    def test_should_not_replace_bookmarks_with_empty_bookmarks(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()

        s1 = self._driver.session("w")
        tx1 = s1.begin_transaction({"order": "1st"})
        tx1.run("RETURN 1 as n").consume()
        tx1.commit()
        s1.close()

        s2 = self._driver.session("r")
        tx2 = s2.begin_transaction({"order": "2nd"})
        tx2.run("RETURN 1 as n").consume()
        tx2.commit()
        s2.close()

        s3 = self._driver.session("w")
        tx3 = s3.begin_transaction({"order": "3rd"})
        tx3.run("RETURN 1 as n").consume()
        tx3.commit()
        s3.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 3)
        self.assert_begin(begin_requests[0])
        self.assert_begin(
            begin_requests[1],
            bookmarks=["bm1"]
        )
        self.assert_begin(
            begin_requests[2],
            bookmarks=["bm1"]
        )

    def test_should_keep_track_of_tx_in_parallel(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()

        s1 = self._driver.session("w")
        tx1 = s1.begin_transaction({"order": "1st"})
        tx1.run("RETURN 1 as n").consume()

        s2 = self._driver.session("w")
        tx2 = s2.begin_transaction({"order": "2nd"})
        tx2.run("RETURN 1 as n").consume()

        tx1.commit()
        s1.close()
        tx2.commit()
        s2.close()

        s3 = self._driver.session("w")
        tx3 = s3.begin_transaction({"order": "2nd"})
        tx3.run("RETURN 1 as n").consume()
        tx3.commit()
        s3.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 3)

        self.assert_begin(begin_requests[0])
        self.assert_begin(begin_requests[1])
        self.assert_begin(
            begin_requests[2],
            bookmarks=["bm1", "bm2"]
        )

    def test_should_manage_explicity_session_bookmarks(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()

        s1 = self._driver.session("w")
        tx1 = s1.begin_transaction({"order": "1st"})
        tx1.run("RETURN 1 as n").consume()
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", bookmarks=[])
        tx2 = s2.begin_transaction({"order": "2nd"})
        tx2.run("RETURN 1 as n").consume()
        tx2.commit()
        s2.close()

        s3 = self._driver.session("w", bookmarks=["unmanaged"])
        tx3 = s3.begin_transaction({"order": "3rd"})
        tx3.run("RETURN 1 as n").consume()
        tx3.commit()
        s3.close()

        s4 = self._driver.session("w")
        tx4 = s4.begin_transaction({"order": "3rd"})
        tx4.run("RETURN 1 as n").consume()
        tx4.commit()
        s4.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 4)
        self.assert_begin(begin_requests[0])
        self.assert_begin(
            begin_requests[1],
            bookmarks=["bm1"]
        )
        self.assert_begin(
            begin_requests[2],
            bookmarks=["bm2", "unmanaged"]
        )
        self.assert_begin(
            begin_requests[3],
            bookmarks=["bm3"]
        )

    def test_should_be_able_to_ignore_bookmark_manager_in_a_sesssion(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()

        s1 = self._driver.session("w")
        tx1 = s1.begin_transaction({"order": "1st"})
        tx1.run("RETURN 1 as n").consume()
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", ignore_bookmark_manager=True)
        tx2 = s2.begin_transaction({"order": "2nd"})
        tx2.run("RETURN 1 as n").consume()
        tx2.commit()
        s2.close()

        s3 = self._driver.session(
            "w",
            ignore_bookmark_manager=True,
            bookmarks=["unmanaged"]
        )
        tx3 = s3.begin_transaction({"order": "3rd"})
        tx3.run("RETURN 1 as n").consume()
        tx3.commit()
        s3.close()

        s4 = self._driver.session("w")
        tx4 = s4.begin_transaction({"order": "3rd"})
        tx4.run("RETURN 1 as n").consume()
        tx4.commit()
        s4.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 4)
        self.assert_begin(begin_requests[0])
        self.assert_begin(
            begin_requests[1]
        )
        self.assert_begin(
            begin_requests[2],
            bookmarks=["unmanaged"]
        )
        self.assert_begin(
            begin_requests[3],
            bookmarks=["bm1"]
        )

    def test_should_use_initial_bookmark_set_in_the_fist_tx(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver(Neo4jBookmarkManagerConfig(
            initial_bookmarks={"neo4j": ["fist_bm"]}
        ))

        s1 = self._driver.session("w", database="neo4j")
        tx1 = s1.begin_transaction({"order": "1st"})
        tx1.run("RETURN 1 as n").consume()
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", database="neo4j")
        tx2 = s2.begin_transaction({"order": "2nd"})
        tx2.run("RETURN 1 as n").consume()
        tx2.commit()
        s2.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 2)
        self.assert_begin(
            begin_requests[0],
            bookmarks=["fist_bm"]
        )
        self.assert_begin(
            begin_requests[1],
            bookmarks=["bm1"]
        )

    def test_should_send_all_db_bookmarks_and_update_only_relevant(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver(Neo4jBookmarkManagerConfig(
            initial_bookmarks={"neo4j": ["fist_bm"], "adb": ["adb:bm1"]}
        ))

        s1 = self._driver.session("w", database="neo4j")
        tx1 = s1.begin_transaction({"order": "1st"})
        tx1.run("RETURN 1 as n").consume()
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", database="neo4j")
        tx2 = s2.begin_transaction({"order": "2nd"})
        tx2.run("RETURN 1 as n").consume()
        tx2.commit()
        s2.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 2)
        self.assert_begin(
            begin_requests[0],
            bookmarks=["fist_bm", "adb:bm1"]
        )
        self.assert_begin(
            begin_requests[1],
            bookmarks=["bm1", "adb:bm1"]
        )

    def test_should_handle_database_redirection_in_tx(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()

        s1 = self._driver.session("w", database="neo4j")
        tx1 = s1.begin_transaction({"order": "1st"})
        tx1.run("RETURN 1 as n").consume()
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", database="neo4j")
        tx2 = s2.begin_transaction({"order": "adb"})
        tx2.run("USE adb RETURN 1 as n").consume()
        tx2.commit()
        s2.close()

        s3 = self._driver.session("w", database="neo4j")
        tx3 = s3.begin_transaction({"order": "2nd"})
        tx3.run("RETURN 1 as n").consume()
        tx3.commit()
        s3.close()

        s4 = self._driver.session("w", database="neo4j")
        tx4 = s4.begin_transaction({"order": "3rd"})
        tx4.run("RETURN 1 as n").consume()
        tx4.commit()
        s4.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 4)
        self.assert_begin(
            begin_requests[0]
        )
        self.assert_begin(
            begin_requests[1],
            bookmarks=["bm1"]
        )
        self.assert_begin(
            begin_requests[2],
            bookmarks=["bm1", "adb:bm4"]
        )
        self.assert_begin(
            begin_requests[3],
            bookmarks=["bm2", "adb:bm4"]
        )

    def test_should_handle_database_redirection_in_session_run(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "session_run_chaining.script")

        self._driver = self._new_driver()

        s1 = self._driver.session("w")
        s1.run("QUERY1").consume()
        s1.close()

        s2 = self._driver.session("w")
        s2.run("USE adb QUERY2").consume()
        s2.close()

        s3 = self._driver.session("w")
        s3.run("QUERY2").consume()
        s3.close()

        s4 = self._driver.session("w")
        s4.run("QUERY3").consume()
        s4.close()

        self._server.reset()
        run_requests = self._server.get_requests("RUN")

        self.assertEqual(len(run_requests), 4)
        self.assert_run(run_requests[0])
        self.assert_run(
            run_requests[1],
            bookmarks=["bm1"]
        )
        self.assert_run(
            run_requests[2],
            bookmarks=["bm1", "adb:bm4"]
        )
        self.assert_run(
            run_requests[3],
            bookmarks=["bm2", "adb:bm4"]
        )

    def test_should_resolve_database_name_with_system_bookmarks(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver(Neo4jBookmarkManagerConfig(
            initial_bookmarks={"system": ["sys:bm1"]}
        ))

        s1 = self._driver.session("w")
        tx1 = s1.begin_transaction({"order": "1st"})
        tx1.run("RETURN 1 as n").consume()
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w")
        tx2 = s2.begin_transaction({"order": "2nd"})
        tx2.run("RETURN 1 as n").consume()
        tx2.commit()
        s2.close()

        self._router.reset()
        self._server.reset()
        route_requests = self._router.get_requests("ROUTE")
        begin_requests = self._server.get_requests("BEGIN")

        self.assertGreaterEqual(len(route_requests), 1)
        self.assertEqual(len(begin_requests), 2)

        self.assert_route(route_requests[0], bookmarks=["sys:bm1"])

        self.assert_begin(
            begin_requests[0],
            bookmarks=["sys:bm1"]
        )
        self.assert_begin(
            begin_requests[1],
            bookmarks=["sys:bm1", "bm1"]
        )

    def _new_driver(self, bookmark_manager_config=None):
        if bookmark_manager_config is None:
            bookmark_manager_config = Neo4jBookmarkManagerConfig()

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        return Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=bookmark_manager_config
        )

    def _start_server(self, server, script):
        server.start(self.script_path(script),
                     vars_={"#HOST#": self._router.host})

    def assert_begin(self, line: str, bookmarks=None):
        if bookmarks is None:
            bookmarks = []
        begin_prefix = "BEGIN "
        self.assertTrue(
            line.startswith(begin_prefix),
            "Line should start with begin"
        )
        begin_properties = json.loads(line[len(begin_prefix):])["{}"]
        if not bookmarks:
            self.assertFalse(
                "bookmarks" in begin_properties,
                "bookmarks should not be in the begin"
            )
        else:
            self.assertEqual(
                sorted(bookmarks),
                sorted(begin_properties.get("bookmarks", []))
            )

    def assert_route(self, line, bookmarks=None):
        if bookmarks is None:
            bookmarks = []
        self.assertTrue(
            line.startswith("ROUTE "),
            "Line should start with ROUTE"
        )
        regex = r".*(\[.*\])"
        matches = re.match(regex, line)
        bookmarks_sent = json.loads(matches.group(1))
        self.assertEqual(sorted(bookmarks), sorted(bookmarks_sent), line)

    def assert_run(self, line, bookmarks=None):
        if bookmarks is None:
            bookmarks = []
        self.assertTrue(
            line.startswith("RUN "),
            "Line should start with RUN"
        )
        regex = r".*\"bookmarks\":\ (\[.*\])"
        matches = re.match(regex, line)
        if matches is None:
            bookmarks_sent = []
        else:
            bookmarks_sent = json.loads(matches.group(1))
        self.assertEqual(sorted(bookmarks), sorted(bookmarks_sent), line)
