import json
import re

from nutkit.frontend import (
    DefaultBookmarkManagerConfig,
    Driver,
)
import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestDefaultBookmarkManager(TestkitTestCase):
    required_features = (
        types.Feature.BOLT_5_0,
        types.Feature.API_BOOKMARK_MANAGER,
    )

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._router = StubServer(9000)
        self._driver = None
        self._session = None
        self._sessions = []

    def tearDown(self):
        self._server.reset()
        self._router.reset()

        for s in self._sessions:
            s.close()

        if self._session:
            self._session.close()

        if self._driver:
            self._driver.close()

        return super().tearDown()

    def test_should_keep_track_of_session_run(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "session_run_chaining.script")

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig()
        )

        s1 = self._driver.session("w")
        s1.run("QUERY1").consume()
        s1.close()

        s2 = self._driver.session("w")
        s2.run("QUERY2").consume()
        s2.close()

        s3 = self._driver.session("w")
        s3.run("QUERY3").consume()
        s3.close()

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

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig()
        )

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

    def test_should_not_replace_bookmarks_by_empty_bookmarks(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig()
        )

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

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig()
        )

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

        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 3)

        self.assert_begin(begin_requests[0])
        self.assert_begin(begin_requests[1])
        self.assert_begin(
            begin_requests[2],
            bookmarks=["bm1", "bm2"]
        )

    def test_should_not_manager_explicity_sesssion_bookmarks(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig()
        )

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
        tx3 = s3.begin_transaction({"order": "2nd"})
        tx3.run("RETURN 1 as n").consume()
        tx3.commit()
        s3.close()

        s4 = self._driver.session("w")
        tx4 = s4.begin_transaction({"order": "2nd"})
        tx4.run("RETURN 1 as n").consume()
        tx4.commit()
        s4.close()

        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 4)
        self.assert_begin(begin_requests[0])
        self.assert_begin(
            begin_requests[1],
            bookmarks=None
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

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig(
                initial_bookmarks={"neo4j": ["fist_bm"]}
            )
        )

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

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig(
                initial_bookmarks={"neo4j": ["fist_bm"], "adb": ["adb:bm1"]}
            )
        )

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

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig()
        )

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

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig()
        )

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

        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        self._driver = Driver(
            self._backend,
            uri, auth,
            bookmark_manager_config=DefaultBookmarkManagerConfig(
                initial_bookmarks={"system": ["sys:bm1"]}
            )
        )

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

    def _start_server(self, server, script):
        server.start(self.script_path(script),
                     vars_={"#HOST#": self._router.host})

    def assert_begin(self, line: str, bookmarks=None):
        begin_prefix = "BEGIN "
        self.assertTrue(
            line.startswith(begin_prefix),
            "Line should start with begin"
        )
        begin_properties = json.loads(line.removeprefix(begin_prefix))["{}"]
        if bookmarks is None:
            self.assertFalse(
                "bookmarks" in begin_properties,
                "bookmarks should not be in the begin"
            )
        else:
            self.assertEqual(
                bookmarks,
                begin_properties.get("bookmarks", None)
            )

    def assert_route(self, line: str, bookmarks=None):
        if bookmarks is None:
            bookmarks = []
        route_prefix = "ROUTE "
        self.assertTrue(
            line.startswith(route_prefix),
            "Line should start with ROUTE"
        )
        regex = r".*(\[.*\])"
        matches = re.match(regex, line, re.MULTILINE)
        bookmarks_sent = json.loads(matches.group(1))
        self.assertEqual(bookmarks, bookmarks_sent, line)

    def assert_run(self, line: str, bookmarks=None):

        run_prefix = "RUN "
        self.assertTrue(
            line.startswith(run_prefix),
            "Line should start with RUN"
        )
        regex = r".*\"bookmarks\":\ (\[.*\])"
        matches = re.match(regex, line, re.MULTILINE)
        if matches is None:
            bookmarks_sent = None
        else:
            bookmarks_sent = json.loads(matches.group(1))
        self.assertEqual(bookmarks, bookmarks_sent, line)
