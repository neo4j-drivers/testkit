import json
import re

import nutkit.protocol as types
from nutkit.frontend import (
    BookmarkManager,
    Driver,
    Neo4jBookmarkManagerConfig,
)
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
        self._bookmark_managers = []

    def tearDown(self):
        self._server.reset()
        self._router.reset()

        if self._driver:
            self._driver.close()
        for bookmark_manager in self._bookmark_managers:
            bookmark_manager.close()
        self._bookmark_managers.clear()

        return super().tearDown()

    def test_should_keep_track_of_session_run(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "session_run_chaining.script")

        self._driver = self._new_driver()
        manager = self._new_bookmark_manager()

        s1 = self._driver.session("w", bookmark_manager=manager)
        list(s1.run("RETURN BOOKMARK bm1"))
        s1.close()

        s2 = self._driver.session("w", bookmark_manager=manager)
        list(s2.run("RETURN BOOKMARK bm2"))
        s2.close()

        s3 = self._driver.session("w", bookmark_manager=manager)
        list(s3.run("RETURN BOOKMARK bm3"))
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
        manager = self._new_bookmark_manager()

        s1 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        s3 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx3 = s3.begin_transaction(tx_meta=tx_meta)
        list(tx3.run("RETURN 1 AS n"))
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
        manager = self._new_bookmark_manager()

        s1 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session("r", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("empty")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        s3 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm3")}
        tx3 = s3.begin_transaction(tx_meta=tx_meta)
        list(tx3.run("RETURN 1 AS n"))
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
        manager = self._new_bookmark_manager()

        s1 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))

        s2 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))

        tx1.commit()
        s1.close()
        tx2.commit()
        s2.close()

        s3 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx3 = s3.begin_transaction(tx_meta=tx_meta)
        list(tx3.run("RETURN 1 AS n"))
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

    def test_should_manage_explicitly_session_bookmarks(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()
        manager = self._new_bookmark_manager()

        s1 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", bookmarks=[], bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        s3 = self._driver.session(
            "w",
            bookmarks=["bm2", "unmanaged"],
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm3")}
        tx3 = s3.begin_transaction(tx_meta=tx_meta)
        list(tx3.run("RETURN 1 AS n"))
        tx3.commit()
        s3.close()

        s4 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm3")}
        tx4 = s4.begin_transaction(tx_meta=tx_meta)
        list(tx4.run("RETURN 1 AS n"))
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

    def test_should_ignore_bookmark_manager_not_set_in_a_session(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()
        manager = self._new_bookmark_manager()

        s1 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w")
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        s3 = self._driver.session(
            "w",
            bookmarks=["unmanaged"]
        )
        tx_meta = {"return_bookmark": types.CypherString("bm3")}
        tx3 = s3.begin_transaction(tx_meta=tx_meta)
        list(tx3.run("RETURN 1 AS n"))
        tx3.commit()
        s3.close()

        s4 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm3")}
        tx4 = s4.begin_transaction(tx_meta=tx_meta)
        list(tx4.run("RETURN 1 AS n"))
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

    def test_should_use_initial_bookmark_set_in_the_first_tx(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver, manager = self._new_driver_and_bookmark_manager(
            Neo4jBookmarkManagerConfig(
                initial_bookmarks=["first_bm"]
            )
        )

        s1 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 2)
        self.assert_begin(
            begin_requests[0],
            bookmarks=["first_bm"]
        )
        self.assert_begin(
            begin_requests[1],
            bookmarks=["bm1"]
        )

    def test_should_send_all_bookmarks_and_replace_it_by_the_new_one(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver, manager = self._new_driver_and_bookmark_manager(
            Neo4jBookmarkManagerConfig(
                initial_bookmarks=["first_bm", "adb:bm1"]
            )
        )

        s1 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 2)
        self.assert_begin(
            begin_requests[0],
            bookmarks=["first_bm", "adb:bm1"]
        )
        self.assert_begin(
            begin_requests[1],
            bookmarks=["bm1"]
        )

    def test_should_handle_database_redirection_in_tx(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()
        manager = self._new_bookmark_manager()

        s1 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"order": types.CypherString("adb")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("USE adb RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        s3 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx3 = s3.begin_transaction(tx_meta=tx_meta)
        list(tx3.run("RETURN 1 AS n"))
        tx3.commit()
        s3.close()

        s4 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm3")}
        tx4 = s4.begin_transaction(tx_meta=tx_meta)
        list(tx4.run("RETURN 1 AS n"))
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
            bookmarks=["adb:bm4"]
        )
        self.assert_begin(
            begin_requests[3],
            bookmarks=["bm2"]
        )

    def test_should_handle_database_redirection_in_session_run(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "session_run_chaining.script")

        self._driver = self._new_driver()
        manager = self._new_bookmark_manager()

        s1 = self._driver.session("w", bookmark_manager=manager)
        list(s1.run("RETURN BOOKMARK bm1"))
        s1.close()

        s2 = self._driver.session("w", bookmark_manager=manager)
        list(s2.run("USE adb RETURN BOOKMARK adb:bm4"))
        s2.close()

        s3 = self._driver.session("w", bookmark_manager=manager)
        list(s3.run("RETURN BOOKMARK bm2"))
        s3.close()

        s4 = self._driver.session("w", bookmark_manager=manager)
        list(s4.run("RETURN BOOKMARK bm3"))
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
            bookmarks=["adb:bm4"]
        )
        self.assert_run(
            run_requests[3],
            bookmarks=["bm2"]
        )

    def test_should_resolve_database_name_with_bookmarks(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver, manager = self._new_driver_and_bookmark_manager(
            Neo4jBookmarkManagerConfig(
                initial_bookmarks=["sys:bm1"]
            )
        )

        s1 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
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
            bookmarks=["bm1"]
        )

    def test_should_enrich_bookmarks_with_bookmark_supplier_result(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        adb_bookmarks = ["adb:bm1"]
        get_bookmarks_calls = 0

        def get_bookmarks():
            nonlocal get_bookmarks_calls
            get_bookmarks_calls += 1
            return ["extra", f"bookmarks{get_bookmarks_calls}"]

        self._driver, manager = self._new_driver_and_bookmark_manager(
            Neo4jBookmarkManagerConfig(
                initial_bookmarks=adb_bookmarks,
                bookmarks_supplier=get_bookmarks
            )
        )

        s1 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        self.assertIn(
            get_bookmarks_calls,
            [
                # some drivers call the supplier for each time they acquire a
                # connection from a routing pool as updating a routing table
                # might be required
                5,
                # for drivers that only call the bookmarks supplier if a new
                # routing table is really needed
                4,
            ]
        )

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")
        self.assertEqual(len(begin_requests), 2)
        self.assert_begin(
            begin_requests[0],
            bookmarks=[*adb_bookmarks, "extra", "bookmarks2"]
        )
        self.assert_begin(
            begin_requests[1],
            bookmarks=["bm1", "extra", f"bookmarks{get_bookmarks_calls}"]
        )

        self._router.reset()
        route_requests = self._router.get_requests("ROUTE")
        self.assertEqual(len(route_requests), 2)
        self.assert_route(
            route_requests[0],
            bookmarks=[*adb_bookmarks, "extra", "bookmarks1"]
        )
        self.assert_route(
            route_requests[1],
            bookmarks=["bm1", "extra", "bookmarks3"]
        )

    def test_should_not_change_bmm_state_with_supplied_bookmarks(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        get_bookmarks_calls = 0

        def get_bookmarks():
            nonlocal get_bookmarks_calls
            get_bookmarks_calls += 1
            if get_bookmarks_calls == 2:
                return ["extra"]
            return []

        self._driver, manager = self._new_driver_and_bookmark_manager(
            Neo4jBookmarkManagerConfig(
                bookmarks_supplier=get_bookmarks
            )
        )

        s1 = self._driver.session(
            "r",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("empty")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        s3 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx3 = s3.begin_transaction(tx_meta=tx_meta)
        list(tx3.run("RETURN 1 AS n"))
        tx3.commit()
        s3.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 3)
        self.assert_begin(begin_requests[0], bookmarks=["extra"])
        self.assert_begin(
            begin_requests[1]
        )
        self.assert_begin(
            begin_requests[2],
            bookmarks=["bm1"]
        )

    def test_should_call_bookmarks_consumer_when_new_bookmarks_arrive(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        adb_bookmarks = ["adb:bm1"]
        bookmarks_consumer_calls = []

        def bookmarks_consumer(bookmarks):
            bookmarks_consumer_calls.append(bookmarks)

        self._driver, manager = self._new_driver_and_bookmark_manager(
            Neo4jBookmarkManagerConfig(
                initial_bookmarks=adb_bookmarks,
                bookmarks_consumer=bookmarks_consumer
            )
        )

        s1 = self._driver.session(
            "w",
            database="neo4j",
            bookmark_manager=manager
        )
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        s2 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"order": types.CypherString("adb")}
        tx2 = s2.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("USE adb RETURN 1 AS n"))
        tx2.commit()
        s2.close()

        self.assertEqual(2, len(bookmarks_consumer_calls))
        self.assertEqual(
            [
                # first tx
                ["bm1"],
                # second tx
                ["adb:bm4"],
            ],
            bookmarks_consumer_calls
        )

    def test_should_call_bookmarks_consumer_for_default_db(self):
        self._start_server(self._router, "router.script")
        self._start_server(self._server, "transaction_chaining.script")

        adb_bookmarks = ["adb:bm1"]
        bookmarks_consumer_calls = []

        def bookmarks_consumer(bookmarks):
            bookmarks_consumer_calls.append(bookmarks)

        self._driver, manager = self._new_driver_and_bookmark_manager(
            Neo4jBookmarkManagerConfig(
                initial_bookmarks=adb_bookmarks,
                bookmarks_consumer=bookmarks_consumer
            )
        )

        s1 = self._driver.session("w", bookmark_manager=manager)
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        s1.close()

        self.assertEqual(1, len(bookmarks_consumer_calls))
        self.assertEqual(bookmarks_consumer_calls, [
            [
                # first tx
                "bm1"
            ]
        ])

    def test_multiple_bookmark_manager(self):
        self._start_server(self._router, "router_with_db_name.script")
        self._start_server(self._server, "transaction_chaining.script")

        self._driver = self._new_driver()

        manager1 = self._new_bookmark_manager(
            Neo4jBookmarkManagerConfig(
                initial_bookmarks=["manager_01_initial_bm"]
            )
        )

        manager2 = self._new_bookmark_manager(
            Neo4jBookmarkManagerConfig(
                initial_bookmarks=["manager_02_initial_bm"]
            )
        )

        manager1_s1 = self._driver.session("w", bookmark_manager=manager1)
        tx_meta = {"return_bookmark": types.CypherString("bm1")}
        tx1 = manager1_s1.begin_transaction(tx_meta=tx_meta)
        list(tx1.run("RETURN 1 AS n"))
        tx1.commit()
        manager1_s1.close()

        manager2_s1 = self._driver.session("w", bookmark_manager=manager2)
        tx_meta = {"return_bookmark": types.CypherString("bm2")}
        tx2 = manager2_s1.begin_transaction(tx_meta=tx_meta)
        list(tx2.run("RETURN 1 AS n"))
        tx2.commit()
        manager2_s1.close()

        manager2_s2 = self._driver.session("w", bookmark_manager=manager2)
        tx_meta = {"return_bookmark": types.CypherString("bm3")}
        tx3 = manager2_s2.begin_transaction(tx_meta=tx_meta)
        list(tx3.run("RETURN 1 AS n"))
        tx3.commit()
        manager2_s2.close()

        manager1_s2 = self._driver.session("w", bookmark_manager=manager1)
        tx_meta = {"return_bookmark": types.CypherString("bm4")}
        tx4 = manager1_s2.begin_transaction(tx_meta=tx_meta)
        list(tx4.run("RETURN 1 AS n"))
        tx4.commit()
        manager1_s2.close()

        self._server.reset()
        begin_requests = self._server.get_requests("BEGIN")

        self.assertEqual(len(begin_requests), 4)
        self.assert_begin(
            begin_requests[0],
            bookmarks=["manager_01_initial_bm"]
        )
        self.assert_begin(
            begin_requests[1],
            bookmarks=["manager_02_initial_bm"]
        )
        self.assert_begin(
            begin_requests[2],
            bookmarks=["bm2"]
        )
        self.assert_begin(
            begin_requests[3],
            bookmarks=["bm1"]
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
            bookmarks_sent = begin_properties.get("bookmarks", [])
            if not self.supports_minimal_bookmarks():
                bookmarks_sent = [*set(bookmarks_sent)]
            self.assertEqual(
                sorted(bookmarks),
                sorted(bookmarks_sent)
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
        if not self.supports_minimal_bookmarks():
            bookmarks_sent = [*set(bookmarks_sent)]
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
            if not self.supports_minimal_bookmarks():
                bookmarks_sent = [*set(bookmarks_sent)]
        self.assertEqual(sorted(bookmarks), sorted(bookmarks_sent), line)

    def supports_minimal_bookmarks(self):
        return self.driver_supports_features(
            types.Feature.OPT_MINIMAL_BOOKMARKS_SET
        )

    def _new_driver(self):
        uri = "neo4j://%s" % self._router.address
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        driver = Driver(
            self._backend,
            uri, auth
        )
        return driver

    def _new_bookmark_manager(self, bookmark_manager_config=None):
        if bookmark_manager_config is None:
            bookmark_manager_config = Neo4jBookmarkManagerConfig()
        bookmark_manager = BookmarkManager(
            self._backend,
            bookmark_manager_config
        )
        self._bookmark_managers.append(bookmark_manager)
        return bookmark_manager

    def _new_driver_and_bookmark_manager(self, bookmark_manager_config=None):
        bookmark_manager = self._new_bookmark_manager(bookmark_manager_config)
        driver = self._new_driver()
        return driver, bookmark_manager
