from collections import defaultdict

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_dns_resolved_server_address,
    get_driver_name,
    get_ip_addresses,
    driver_feature
)
from ._routing import RoutingBase


class RoutingV4x3(RoutingBase):
    bolt_version = "4.3"
    server_agent = "Neo4j/4.3.0"
    adb = "adb"

    def route_call_count(self, server):
        return server.count_requests("ROUTE")

    def test_should_successfully_get_routing_table_with_context(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("needs verifyConnectivity support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_connectivity_db.script")
        driver.verifyConnectivity()
        driver.close()

        self._routingServer1.done()

    def test_should_successfully_get_routing_table(self):
        # TODO: remove this block once all languages support routing table test
        #       API
        # TODO: when all driver support this,
        #       test_should_successfully_get_routing_table_with_context
        #       and all tests (ab)using verifyConnectivity to refresh the RT
        #       should be updated. Tests for verifyConnectivity should be added.
        if get_driver_name() in ['go', 'java', 'javascript', 'dotnet']:
            self.skipTest("needs routing table API support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        vars_ = self.get_vars()
        self.start_server(self._routingServer1, "router_adb.script",
                          vars_=vars_)
        driver.updateRoutingTable(self.adb)
        self._routingServer1.done()
        rt = driver.getRoutingTable(self.adb)
        driver.close()
        assert rt.database == self.adb
        assert rt.ttl == 1000
        assert rt.routers == [vars_["#HOST#"] + ":9000"]
        assert sorted(rt.readers) == [vars_["#HOST#"] + ":9010",
                                      vars_["#HOST#"] + ":9011"]
        assert sorted(rt.writers) == [vars_["#HOST#"] + ":9020",
                                      vars_["#HOST#"] + ":9021"]

    # Checks that routing is used to connect to correct server and that
    # parameters for session run is passed on to the target server
    # (not the router).
    def test_should_read_successfully_from_reader_using_session_run(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")

        session = driver.session('r', database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        summary = result.consume()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._readServer1))
        self.assertEqual([1], sequence)

    def test_should_read_successfully_from_reader_using_session_run_with_default_db_driver(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_default_db.script")
        self.start_server(self._readServer1, "reader_default_db.script")

        session = driver.session('r')
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        summary = result.consume()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._readServer1))
        self.assertEqual([1], sequence)

    # Same test as for session.run but for transaction run.
    def test_should_read_successfully_from_reader_using_tx_run(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader_tx.script")

        session = driver.session('r', database=self.adb)
        tx = session.beginTransaction()
        result = tx.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        summary = result.consume()
        tx.commit()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._readServer1))
        self.assertEqual([1], sequence)

    def test_should_send_system_bookmark_with_route(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_system_then_adb_with_bookmarks.script"
        )
        self.start_server(
            self._writeServer1,
            "router_create_adb_with_bookmarks.script"
        )

        session = driver.session('w', database='system')
        tx = session.beginTransaction()
        list(tx.run("CREATE database foo"))
        tx.commit()

        session2 = driver.session('w', bookmarks=session.lastBookmarks(),
                                  database=self.adb)
        result = session2.run("RETURN 1 as n")
        sequence2 = self.collectRecords(result)
        session.close()
        session2.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual([1], sequence2)

    def test_should_read_successfully_from_reader_using_tx_function(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("crashes the backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader_tx.script")

        session = driver.session('r', database=self.adb)
        sequences = []
        summaries = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))
            summaries.append(result.consume())

        session.readTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([[1]], sequences)
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0].server_info.address,
                         get_dns_resolved_server_address(self._readServer1))

    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_using_session_run(self):
        # TODO remove this block once all languages wor
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1,
                          "reader_with_unexpected_interruption.script")

        session = driver.session('r', database=self.adb)
        failed = False
        try:
            # drivers doing eager loading will fail here
            session.run("RETURN 1 as n")
        except types.DriverError as e:
            session.close()
            failed = True
        else:
            try:
                # else they should fail here
                session.close()
            except types.DriverError as e:
                if get_driver_name() in ['java']:
                    self.assertEqual(
                        'org.neo4j.driver.exceptions.SessionExpiredException',
                        e.errorType)
                failed = True
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._readServer1,
            "reader_tx_with_unexpected_interruption.script"
        )

        session = driver.session('r', database=self.adb)
        tx = session.beginTransaction()
        failed = False
        try:
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.SessionExpiredException',
                    e.errorType
                )
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(failed)

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME)
    def test_should_fail_when_reading_from_unexpectedly_interrupting_readers_using_tx_function(
            self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, maxTxRetryTimeMs=5000)
        self.start_server(self._routingServer1,
                          "router_adb_multi_no_bookmarks.script")
        self.start_server(
            self._readServer1,
            "reader_tx_with_unexpected_interruption.script"
        )
        self.start_server(
            self._readServer2,
            "reader_tx_with_unexpected_interruption.script"
        )

        session = driver.session('r', database=self.adb)

        def work(tx):
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()

        with self.assertRaises(types.DriverError) as exc:
            session.readTransaction(work)

        session.close()
        driver.close()

        if get_driver_name() in ['java']:
            self.assertEqual(
                'org.neo4j.driver.exceptions.SessionExpiredException',
                exc.exception.errorType
            )
        self._routingServer1.done()
        self._readServer1.done()
        self._readServer2.done()

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME)
    def test_should_fail_when_writing_to_unexpectedly_interrupting_writers_using_tx_function(
            self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, maxTxRetryTimeMs=5000)
        self.start_server(self._routingServer1,
                          "router_adb_multi_no_bookmarks.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_unexpected_interruption.script"
        )
        self.start_server(
            self._writeServer2,
            "writer_tx_with_unexpected_interruption.script"
        )

        session = driver.session('w', database=self.adb)

        def work(tx):
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()

        with self.assertRaises(types.DriverError) as exc:
            session.writeTransaction(work)

        session.close()
        driver.close()

        if get_driver_name() in ['java']:
            self.assertEqual(
                'org.neo4j.driver.exceptions.SessionExpiredException',
                exc.exception.errorType
            )
        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()

    # Checks that write server is used
    def test_should_write_successfully_on_writer_using_session_run(self):
        # FIXME: test assumes that first writer in RT will be contacted first
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1, "writer.script")

        session = driver.session('w', database=self.adb)
        res = session.run("RETURN 1 as n")
        list(res)
        summary = res.consume()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        assert (summary.server_info.address
                == get_dns_resolved_server_address(self._writeServer1))

    # Checks that write server is used
    def test_should_write_successfully_on_writer_using_tx_run(self):
        # FIXME: test assumes that first writer in RT will be contacted first
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1, "writer_tx.script")

        session = driver.session('w', database=self.adb)
        tx = session.beginTransaction()
        res = tx.run("RETURN 1 as n")
        list(res)
        summary = res.consume()
        tx.commit()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        assert (summary.server_info.address
                == get_dns_resolved_server_address(self._writeServer1))

    def test_should_write_successfully_on_writer_using_tx_function(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("crashes the backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1, "writer_tx.script")

        session = driver.session('w', database=self.adb)
        res = None
        summary = None

        def work(tx):
            nonlocal res, summary
            res = tx.run("RETURN 1 as n")
            list(res)
            summary = res.consume()

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertIsNotNone(res)
        assert (summary.server_info.address
                == get_dns_resolved_server_address(self._writeServer1))

    def test_should_write_successfully_on_leader_switch_using_tx_function(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, None)
        self.start_server(self._routingServer1,
                          "router_adb_multi_no_bookmarks.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_leader_switch_and_retry.script"
        )

        session = driver.session('w', database=self.adb)
        sequences = []

        work_count = 1
        def work(tx):
            nonlocal work_count
            try:
                result = tx.run("RETURN %i.1 as n" % work_count)
                sequences.append(self.collectRecords(result))
                result = tx.run("RETURN %i.2 as n" % work_count)
                sequences.append(self.collectRecords(result))
            finally:
                # don't simply increase work_count: there is a second writer in
                # in the RT that the driver could try to contact. In that case
                # the tx function will be called 3 times in total
                work_count = 2

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        if self.driver_supports_features(types.Feature.OPT_CONNECTION_REUSE):
            self.assertEqual(self._writeServer1.count_responses("<ACCEPT>"), 1)
        else:
            self.assertLessEqual(
                self._writeServer1.count_responses("<ACCEPT>"), 2
            )
        self.assertEqual([[1], [1]], sequences)
        self.assertEqual(self.route_call_count(self._routingServer1), 2)

    def test_should_retry_write_until_success_with_leader_change_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_with_leader_change.script"
        )
        self.start_server(
            self._writeServer1,
            "writer_tx_with_unexpected_interruption.script"
        )
        self.start_server(self._writeServer2, "writer_tx.script")

        session = driver.session('w', database=self.adb)
        sequences = []
        num_retries = 0

        def work(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(2, num_retries)

    def test_should_retry_write_until_success_with_leader_shutdown_during_tx_using_tx_function(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_with_leader_change.script"
        )
        self.start_server(
            self._writeServer1,
            "writer_tx_yielding_database_unavailable_failure_on_commit.script"
        )
        self.start_server(self._writeServer2, "writer_tx.script")

        session = driver.session('w', database=self.adb)
        sequences = []
        num_retries = 0

        def work(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[], []], sequences)
        self.assertEqual(2, num_retries)

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_using_session_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_with_unexpected_interruption.script"
        )

        session = driver.session('w', database=self.adb)
        failed = False
        try:
            # drivers doing eager loading will fail here
            result = session.run("RETURN 1 as n")
            # drivers doing lazy loading should fail here
            result.next()
        except types.DriverError as e:
            session.close()
            failed = True
        else:
            try:
                # else they should fail here
                session.close()
            except types.DriverError as e:
                if get_driver_name() in ['java']:
                    self.assertEqual(
                        'org.neo4j.driver.exceptions.SessionExpiredException',
                        e.errorType
                    )
                elif get_driver_name() in ['python']:
                    self.assertEqual(
                        "<class 'neo4j.exceptions.SessionExpired'>",
                        e.errorType
                    )
                failed = True
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")
        if get_driver_name() in ['go', 'java', 'javascript', 'dotnet']:
            self.skipTest("needs routing table API support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_unexpected_interruption.script"
        )

        session = driver.session('w', database=self.adb)
        tx = session.beginTransaction()
        failed = False
        try:
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.SessionExpiredException',
                    e.errorType
                )
            failed = True
        session.close()
        driver.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.getRoutingTable(self.adb).writers)
        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_discovery_when_router_fails_with_procedure_not_found_code(self):
        # TODO add support and remove this block
        if get_driver_name() in ['go']:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_procedure_not_found_failure_connectivity_db.script"
        )

        failed = False
        try:
            driver.verifyConnectivity()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.ServiceUnavailableException',
                    e.errorType
                )
            elif get_driver_name() in ['python']:
                self.assertEqual(
                    "<class 'neo4j.exceptions.ServiceUnavailable'>",
                    e.errorType
                )
            failed = True
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def test_should_fail_discovery_when_router_fails_with_unknown_code(self):
        # TODO add support and remove this block
        if get_driver_name() in ['go']:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_unknown_failure.script"
        )

        failed = False
        try:
            driver.verifyConnectivity()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.ServiceUnavailableException',
                    e.errorType
                )
            failed = True
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'java', 'javascript', 'dotnet']:
            self.skipTest("needs routing table API support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_yielding_failure_on_run.script",
            vars_={
                **self.get_vars(),
                "#FAILURE#": '{"code": "Neo.ClientError.Cluster.NotALeader", '
                             '"message": "blabla"}'
            }
        )

        session = driver.session('w', database=self.adb)
        failed = False
        try:
            session.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.SessionExpiredException',
                    e.errorType
                )
            elif get_driver_name() in ['python']:
                self.assertEqual(
                    "<class 'neo4j.exceptions.NotALeader'>",
                    e.errorType
                )
                self.assertEqual(
                    "Neo.ClientError.Cluster.NotALeader",
                    e.code
                )
            failed = True
        session.close()
        driver.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.getRoutingTable(self.adb).writers)
        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_forbidden_on_read_only_database(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'java', 'javascript', 'dotnet']:
            self.skipTest("needs routing table API support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_yielding_failure_on_run.script",
            vars_={
                **self.get_vars(),
                "#FAILURE#":
                    '{"code": '
                    '"Neo.ClientError.General.ForbiddenOnReadOnlyDatabase", '
                    '"message": "Unable to write"}'
            }
        )

        session = driver.session('w', database=self.adb)
        failed = False
        try:
            session.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ['python']:
                self.assertEqual(
                    "<class 'neo4j.exceptions.ForbiddenOnReadOnlyDatabase'>",
                    e.errorType
                )
                self.assertEqual(
                    "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase",
                    e.code
                )
            failed = True
        session.close()
        driver.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.getRoutingTable(self.adb).writers)
        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_database_unavailable(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'java', 'javascript', 'dotnet']:
            self.skipTest("needs routing table API support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_yielding_failure_on_run.script",
            vars_={
                **self.get_vars(),
                "#FAILURE#":
                    '{"code": '
                    '"Neo.ClientError.General.DatabaseUnavailable", '
                    '"message": "Database is busy doing store copy"}'
            }
        )

        session = driver.session('w', database=self.adb)
        failed = False
        try:
            session.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ['python']:
                self.assertEqual(
                    "<class 'neo4j.exceptions.ClientError'>",
                    e.errorType
                )
                self.assertEqual(
                    "Neo.ClientError.General.DatabaseUnavailable",
                    e.code
                )
            failed = True
        session.close()
        driver.close()

        self.assertIn(self._writeServer1.address,
                      driver.getRoutingTable(self.adb).writers)
        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")
        if get_driver_name() in ['go', 'java', 'javascript', 'dotnet']:
            self.skipTest("needs routing table API support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_yielding_failure_on_run.script",
            vars_={
                **self.get_vars(),
                "#FAILURE#": '{"code": "Neo.ClientError.Cluster.NotALeader",'
                             ' "message": "blabla"}'
            }
        )

        session = driver.session('w', database=self.adb)
        failed = False

        try:
            # drivers doing eager loading will fail here
            result = session.run("RETURN 1 as n")
            # drivers doing lazy loading should fail here
            result.next()
        except types.DriverError as e:
            session.close()
            failed = True
        else:
            try:
                # else they should fail here
                session.close()
            except types.DriverError as e:
                if get_driver_name() in ['java']:
                    self.assertEqual(
                        'org.neo4j.driver.exceptions.SessionExpiredException',
                        e.errorType
                    )
                elif get_driver_name() in ['python']:
                    self.assertEqual(
                        "<class 'neo4j.exceptions.NotALeader'>",
                        e.errorType
                    )
                failed = True

        driver.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.getRoutingTable(self.adb).writers)
        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("consume not implemented in backend")
        if get_driver_name() in ['go', 'java', 'javascript', 'dotnet']:
            self.skipTest("needs routing table API support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_yielding_failure_on_run.script",
            vars_={
                **self.get_vars(),
                "#FAILURE#": '{"code": "Neo.ClientError.Cluster.NotALeader", '
                             '"message": "blabla"}'
            }
        )

        session = driver.session('w', database=self.adb)
        tx = session.beginTransaction()
        failed = False
        try:
            tx.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.SessionExpiredException',
                    e.errorType
                )
            elif get_driver_name() in ['python']:
                self.assertEqual(
                    "<class 'neo4j.exceptions.NotALeader'>",
                    e.errorType
                )
            failed = True
        session.close()
        driver.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.getRoutingTable(self.adb).writers)
        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'java', 'javascript', 'dotnet']:
            self.skipTest("needs routing table API support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_yielding_failure_on_run.script",
            vars_={
                **self.get_vars(),
                "#FAILURE#": '{"code": "Neo.ClientError.Cluster.NotALeader", '
                             '"message": "blabla"}'
            }
        )

        session = driver.session('w', database=self.adb)
        tx = session.beginTransaction()
        failed = False
        try:
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.SessionExpiredException',
                    e.errorType
                )
            elif get_driver_name() in ['python']:
                self.assertEqual(
                    "<class 'neo4j.exceptions.NotALeader'>",
                    e.errorType
                )
            failed = True
        session.close()
        driver.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.getRoutingTable(self.adb).writers)
        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_use_write_session_mode_and_initial_bookmark_when_writing_using_tx_run(self):
        # TODO remove this block once all languages work
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_bookmarks.script"
        )

        session = driver.session('w', bookmarks=["OldBookmark"],
                                 database=self.adb)
        tx = session.beginTransaction()
        list(tx.run("RETURN 1 as n"))
        tx.commit()
        last_bookmarks = session.lastBookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual(["NewBookmark"], last_bookmarks)

    def test_should_use_read_session_mode_and_initial_bookmark_when_reading_using_tx_run(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader_tx_with_bookmarks.script")

        session = driver.session('r', bookmarks=["OldBookmark"],
                                 database=self.adb)
        tx = session.beginTransaction()
        result = tx.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        tx.commit()
        last_bookmarks = session.lastBookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)
        self.assertEqual(["NewBookmark"], last_bookmarks)

    def test_should_pass_bookmark_from_tx_to_tx_using_tx_run(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_and_reader_tx_with_bookmark.script"
        )

        session = driver.session('w', bookmarks=["BookmarkA"],
                                 database=self.adb)
        tx = session.beginTransaction()
        list(tx.run("CREATE (n {name:'Bob'})"))
        tx.commit()
        first_bookmark = session.lastBookmarks()
        tx = session.beginTransaction()
        result = tx.run("MATCH (n) RETURN n.name AS name")
        sequence = self.collectRecords(result)
        tx.commit()
        second_bookmark = session.lastBookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual(['Bob'], sequence)
        self.assertEqual(["BookmarkB"], first_bookmark)
        self.assertEqual(["BookmarkC"], second_bookmark)

    def test_should_retry_read_tx_until_success_on_error(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._readServer1,
            "reader_tx_with_unexpected_interruption.script"
        )
        self.start_server(
            self._readServer2,
            "reader_tx_with_unexpected_interruption.script"
        )

        session = driver.session('r', database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            try:
                result = tx.run("RETURN 1 as n")
                sequences.append(self.collectRecords(result))
            except types.DriverError:
                reader1_con_count = \
                    self._readServer1.count_responses("<ACCEPT>")
                reader2_con_count = \
                    self._readServer2.count_responses("<ACCEPT>")
                if reader1_con_count == 1 and reader2_con_count == 0:
                    working_reader = self._readServer2
                elif reader1_con_count == 0 and reader2_con_count == 1:
                    working_reader = self._readServer1
                else:
                    raise
                working_reader.reset()
                self.start_server(working_reader, "reader_tx.script")
                raise

        session.readTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual([[1]], sequences)
        self.assertEqual(2, try_count)

    def test_should_retry_read_tx_until_success_on_no_connection(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._readServer1,
            "reader_tx.script"
        )
        self.start_server(
            self._readServer2,
            "reader_tx.script"
        )

        session = driver.session('r', database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        self._routingServer1.done()
        connection_counts = (
            self._readServer1.count_responses("<ACCEPT>"),
            self._readServer2.count_responses("<ACCEPT>")
        )
        self.assertIn(connection_counts, {(0, 1), (1, 0)})
        if connection_counts == (1, 0):
            self._readServer1.done()
        else:
            self._readServer2.done()
        self.assertEqual([[1]], sequences)
        self.assertEqual(1, try_count)

        session.readTransaction(work)
        session.close()
        driver.close()

        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual([[1], [1]], sequences)
        # Drivers might or might not try the first server again
        self.assertLessEqual(try_count, 3)
        # TODO: Design a test that makes sure the driver doesn't run the tx func
        #       if it can't establish a working connection to the server. So
        #       that `try_count == 2`. When doing so be aware that drivers could
        #       do round robin, e.g. Java.

    def test_should_retry_write_tx_until_success_on_error(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_unexpected_interruption.script"
        )
        self.start_server(
            self._writeServer2,
            "writer_tx_with_unexpected_interruption.script"
        )

        session = driver.session('w', database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            try:
                result = tx.run("RETURN 1 as n")
                sequences.append(self.collectRecords(result))
            except types.DriverError:
                writer1_con_count = \
                    self._writeServer1.count_responses("<ACCEPT>")
                writer2_con_count = \
                    self._writeServer2.count_responses("<ACCEPT>")
                if writer1_con_count == 1 and writer2_con_count == 0:
                    working_writer = self._writeServer2
                elif writer1_con_count == 0 and writer2_con_count == 1:
                    working_writer = self._writeServer1
                else:
                    raise
                working_writer.reset()
                self.start_server(working_writer, "writer_tx.script")
                raise

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(2, try_count)

    def test_should_retry_write_tx_until_success_on_no_connection(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_tx.script"
        )
        self.start_server(
            self._writeServer2,
            "writer_tx.script"
        )

        session = driver.session('r', database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.writeTransaction(work)
        self._routingServer1.done()
        connection_counts = (
            self._writeServer1.count_responses("<ACCEPT>"),
            self._writeServer2.count_responses("<ACCEPT>")
        )
        self.assertIn(connection_counts, {(0, 1), (1, 0)})
        if connection_counts == (1, 0):
            self._writeServer1.done()
        else:
            self._writeServer2.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(1, try_count)

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[], []], sequences)
        # Drivers might or might not try the first server again
        self.assertLessEqual(try_count, 3)
        # TODO: Design a test that makes sure the driver doesn't run the tx func
        #       if it can't establish a working connection to the server. So
        #       that `try_count == 2`. When doing so be aware that drivers could
        #       do round robin, e.g. Java.

    def test_should_retry_read_tx_and_rediscovery_until_success(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_router2.script"
        )
        self.start_server(self._routingServer2,
                          "router_yielding_reader2_adb.script")
        self.start_server(
            self._readServer1,
            "reader_tx_with_unexpected_interruption.script"
        )
        self.start_server(self._readServer2, "reader_tx.script")
        self.start_server(
            self._readServer3,
            "reader_tx_with_unexpected_interruption.script"
        )

        session = driver.session('r', database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._readServer1.done()
        self._readServer2.done()
        self._readServer3.done()
        self.assertEqual([[1]], sequences)
        self.assertEqual(3, try_count)

    def test_should_retry_write_tx_and_rediscovery_until_success(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_router2.script"
        )
        self.start_server(self._routingServer2,
                          "router_yielding_reader2_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_unexpected_interruption.script"
        )
        self.start_server(self._writeServer2, "writer_tx.script")
        self.start_server(
            self._writeServer3,
            "writer_tx_with_unexpected_interruption.script"
        )

        session = driver.session('w', database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self._writeServer3.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(3, try_count)

    def test_should_use_initial_router_for_discovery_when_others_unavailable(self):
        # TODO add support and remove this block
        if get_driver_name() in ['go']:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_router2_and_fake_reader.script"
        )
        self.start_server(self._readServer1, "reader_tx.script")

        driver.verifyConnectivity()
        self._routingServer1.done()
        self.start_server(self._routingServer1, "router_adb.script")
        session = driver.session('r', database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([[1]], sequences)

    def test_should_successfully_read_from_readable_router_using_tx_function(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        # Some drivers (for instance, java) may use separate connections for
        # readers and writers when they are addressed by domain names in routing
        # table. Since this test is not for testing DNS resolution, it has been
        # switched to IP-based address model.
        ip_address = get_ip_addresses()[0]
        driver = Driver(
            self._backend,
            self._uri_template_with_context % (ip_address,
                                               self._routingServer1.port),
            self._auth,
            self._userAgent
        )
        self.start_server(
            self._routingServer1,
            "router_and_reader.script",
            vars_=self.get_vars(host=ip_address))

        session = driver.session('r', database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        # TODO: it's not a gold badge to connect more than once.
        self.assertLessEqual(
            self._routingServer1.count_responses("<ACCEPT>"), 2
        )
        self.assertEqual(self._routingServer1.count_requests("COMMIT"), 1)
        self.assertEqual([[1]], sequences)

    def test_should_send_empty_hello(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        # Some drivers (for instance, java) may use separate connections for
        # readers and writers when they are addressed by domain names in routing
        # table. Since this test is not for testing DNS resolution, it has been
        # switched to IP-based address model.
        ip_address = get_ip_addresses()[0]
        driver = Driver(
            self._backend,
            self._uri_template % (ip_address, self._routingServer1.port),
            self._auth,
            self._userAgent
        )
        self.start_server(
            self._routingServer1,
            "router_and_reader_with_empty_routing_context.script",
            vars_=self.get_vars(host=ip_address)
        )

        session = driver.session('r', database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self.assertEqual(self._routingServer1.count_requests("COMMIT"), 1)
        self.assertEqual([[1]], sequences)

    def test_should_serve_reads_and_fail_writes_when_no_writers_available(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['dotnet', 'go']:
            self.skipTest("consume not implemented in backend "
                          "or requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_no_writers_adb.script"
        )
        self.start_server(
            self._routingServer2,
            "router_yielding_no_writers_adb.script"
        )
        self.start_server(self._readServer1, "reader_tx.script")

        session = driver.session('w', database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)

        failed = False
        try:
            session.run("CREATE (n {name:'Bob'})").consume()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.SessionExpiredException',
                    e.errorType
                )
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._readServer1.done()
        self.assertEqual([[1]], sequences)
        self.assertTrue(failed)

    def test_should_accept_routing_table_without_writers_and_then_rediscover(self):
        # TODO add support and remove this block
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['go', 'dotnet']:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_no_writers_any_db.script"
        )
        self.start_server(self._readServer1, "reader_tx_with_bookmarks.script")
        self.start_server(self._writeServer1, "writer_with_bookmark.script")

        driver.verifyConnectivity()
        session = driver.session('w', bookmarks=["OldBookmark"],
                                 database=self.adb)
        sequences = []
        self._routingServer1.done()
        try:
            driver.verifyConnectivity()
        except types.DriverError:
            # make sure the driver noticed that its old connection to
            # _routingServer1 is dead
            pass
        self.start_server(self._routingServer1, "router_adb.script")

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        list(session.run("RETURN 1 as n"))
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self._writeServer1.done()
        self.assertEqual([[1]], sequences)

    def test_should_fail_on_routing_table_with_no_reader(self):
        if get_driver_name() in ['go', 'java', 'javascript', 'dotnet']:
            self.skipTest("needs routing table API support")
        self.start_server(
            self._routingServer1,
            "router_yielding_no_readers_any_db.script"
        )
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        failed = False
        try:
            driver.updateRoutingTable()
        except types.DriverError as exc:
            failed = True
            if get_driver_name() in ['python']:
                self.assertEqual(
                    exc.errorType,
                    "<class 'neo4j.exceptions.ServiceUnavailable'>"
                )

        self.assertTrue(failed)
        routing_table = driver.getRoutingTable()
        self.assertEqual(routing_table.routers, [])
        self.assertEqual(routing_table.readers, [])
        self.assertEqual(routing_table.writers, [])
        self._routingServer1.done()
        driver.close()

    def test_should_accept_routing_table_with_single_router(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")
        self.start_server(self._readServer2, "reader.script")

        session = driver.session('r', database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        session.close()
        driver.close()

        self._routingServer1.done()

        connection_count_rs1 = self._readServer1.count_responses("<ACCEPT>")
        connection_count_rs2 = self._readServer2.count_responses("<ACCEPT>")
        self.assertEqual(connection_count_rs1 + connection_count_rs2, 1)
        if connection_count_rs1 == 1:
            self._readServer1.done()
            self._readServer2.reset()
        else:
            self._readServer1.reset()
            self._readServer2.done()
        self.assertEqual([1], sequence)

    def test_should_successfully_send_multiple_bookmarks(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1,
                          "writer_tx_with_multiple_bookmarks.script")

        session = driver.session(
            'w',
            bookmarks=[
                "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29",
                "neo4j:bookmark:v1:tx94", "neo4j:bookmark:v1:tx56",
                "neo4j:bookmark:v1:tx16", "neo4j:bookmark:v1:tx68"
            ],
            database=self.adb
        )
        tx = session.beginTransaction()
        list(tx.run("RETURN 1 as n"))
        tx.commit()
        last_bookmarks = session.lastBookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual(["neo4j:bookmark:v1:tx95"], last_bookmarks)

    def test_should_forget_address_on_database_unavailable_error(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['dotnet', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1,
                          "router_yielding_writer1.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_yielding_database_unavailable_failure.script"
        )
        self.start_server(
            self._routingServer2,
            "router_yielding_writer2.script"
        )
        self.start_server(self._writeServer2, "writer_tx.script")

        session = driver.session('w', database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(2, try_count)

    def test_should_use_resolver_during_rediscovery_when_existing_routers_fail(self):
        # TODO add support and remove this block
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['dotnet']:
            self.skipTest("resolver not implemented in backend")
        resolver_invoked = 0

        def resolver(address):
            nonlocal resolver_invoked
            if address != self._routingServer1.address:
                return [address]

            resolver_invoked += 1
            if resolver_invoked == 1:
                return [address]
            elif resolver_invoked == 2:
                return [self._routingServer2.address]
            self.fail("unexpected")

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, resolverFn=resolver)
        self.start_server(
            self._routingServer1,
            "router_yielding_reader1_and_exit.script"
        )
        self.start_server(self._routingServer2, "router_adb.script")
        self.start_server(self._readServer1, "reader_tx_with_exit.script")
        self.start_server(self._readServer2, "reader_tx.script")

        session = driver.session('w', database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        session.readTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual([[1], [1]], sequences)

    def test_should_revert_to_initial_router_if_known_router_throws_protocol_errors(self):
        # TODO add support and remove this block
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['dotnet']:
            self.skipTest("resolver not implemented in backend")

        resolver_calls = defaultdict(lambda: 0)

        def resolver(address):
            resolver_calls[address] += 1
            if address == self._routingServer1.address:
                return [self._routingServer1.address,
                        self._routingServer3.address]
            return [address]

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, resolver)
        self.start_server(
            self._routingServer1,
            "router_yielding_router2_and_non_existent_reader.script"
        )
        self.start_server(
            self._routingServer2,
            "router_yielding_empty_response_then_shuts_down.script"
        )
        self.start_server(self._routingServer3, "router_adb.script")
        self.start_server(self._readServer1, "reader_tx.script")

        session = driver.session('r', database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)

        session.close()
        driver.close()
        self._routingServer1.done()
        self._routingServer2.done()
        self._routingServer3.done()
        self._readServer1.done()
        self.assertEqual([[1]], sequences)
        if len(resolver_calls) == 1:
            # driver that calls resolver function only on initial router address
            self.assertEqual(resolver_calls.keys(),
                             {self._routingServer1.address})
            # depending on whether the resolve result is treated equally to a
            # RT table entry or is discarded after an RT has been retrieved
            # successfully.
            self.assertEqual(resolver_calls[self._routingServer1.address], 2)
        else:
            fake_reader_address = self._routingServer1.host + ":9099"
            # driver that calls resolver function for every address (initial
            # router and server addresses returned in routing table
            self.assertLessEqual(resolver_calls.keys(),
                                 {self._routingServer1.address,
                                  fake_reader_address,
                                  self._routingServer2.address,
                                  self._readServer1.address,
                                  # readServer2 isn't part of this test but is
                                  # in the RT of router_script_adb by default
                                  self._readServer2.address})
            self.assertEqual(resolver_calls[self._routingServer1.address], 2)

            self.assertEqual(resolver_calls[fake_reader_address], 1)
            self.assertEqual(resolver_calls[self._readServer1.address], 1)

    def should_support_multi_db(self):
        return True

    def test_should_successfully_check_if_support_for_multi_db_is_available(self):
        # TODO add support and remove this block
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['go', 'dotnet']:
            self.skipTest("supportsMultiDb not implemented in backend")

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_default_db.script")
        self.start_server(self._readServer1, "empty_reader.script")

        supports_multi_db = driver.supportsMultiDB()

        # we don't expect the router or the reader to play the whole
        # script
        self._routingServer1.reset()
        self._readServer1.reset()
        driver.close()
        self.assertLessEqual(self._readServer1.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._readServer1.count_requests("RUN"), 0)
        self.assertEqual(self.should_support_multi_db(), supports_multi_db)

    def test_should_read_successfully_on_empty_discovery_result_using_session_run(self):
        # TODO add support and remove this block
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['dotnet']:
            self.skipTest("resolver not implemented in backend")

        def resolver(address):
            if address == self._routingServer1.address:
                return (self._routingServer1.address,
                        self._routingServer2.address)
            return address,

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, resolver)
        self.start_server(
            self._routingServer1,
            "router_yielding_empty_response_then_shuts_down.script"
        )
        self.start_server(self._routingServer2, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")

        session = driver.session('r', database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)

    def test_should_fail_with_routing_failure_on_db_not_found_discovery_failure(self):
        # TODO add support and remove this block
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['go', 'dotnet']:
            self.skipTest("add code support")
        if not self.should_support_multi_db():
            return

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_db_not_found_failure.script"
        )

        session = driver.session('r', database=self.adb)
        failed = False
        try:
            result = session.run("RETURN 1 as n")
            result.next()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.FatalDiscoveryException',
                    e.errorType
                )
            self.assertEqual('Neo.ClientError.Database.DatabaseNotFound',
                             e.code)
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def test_should_read_successfully_from_reachable_db_after_trying_unreachable_db(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_unreachable_db_then_adb.script"
        )
        self.start_server(self._readServer1, "reader.script")

        session = driver.session('r', database="unreachable")
        failed_on_unreachable = False
        try:
            result = session.run("RETURN 1 as n")
            self.collectRecords(result)
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'org.neo4j.driver.exceptions.ServiceUnavailableException',
                    e.errorType
                )
            failed_on_unreachable = True
        session.close()

        session = driver.session('r', database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        self.assertEqual(self.route_call_count(self._routingServer1), 2)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(failed_on_unreachable)
        self.assertEqual([1], sequence)

    def test_should_pass_system_bookmark_when_getting_rt_for_multi_db(self):
        pass

    def test_should_ignore_system_bookmark_when_getting_rt_for_multi_db(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader_with_bookmarks.script")

        session = driver.session('r', database=self.adb,
                                 bookmarks=["sys:1234", "foo:5678"])
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        last_bookmarks = session.lastBookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)
        self.assertEqual(["foo:6678"], last_bookmarks)

    def test_should_request_rt_from_all_initial_routers_until_successful(self):
        # TODO add support and remove this block
        if get_driver_name() in ['dotnet']:
            self.skipTest("needs ROUTE bookmark list support")
        if get_driver_name() in ['go', 'dotnet']:
            self.skipTest("add resolvers and connection timeout support")

        resolver_calls = {}
        domain_name_resolver_call_num = 0
        resolved_addresses = [
            "host1:%s" % self._routingServer1.port,
            "host2:%s" % self._routingServer2.port,
            "host3:%s" % self._routingServer3.port
        ]
        resolved_domain_name_addresses = [
            self._routingServer1.host,
            self._routingServer2.host,
            self._routingServer3.host
        ]

        # The resolver is used to convert the original address to multiple fake
        # domain names.
        def resolver(address):
            nonlocal resolver_calls
            nonlocal resolved_addresses
            resolver_calls[address] = resolver_calls.get(address, 0) + 1
            if address != self._routingServer1.address:
                return [address]
            return resolved_addresses

        # The domain name resolver is used to verify that the fake domain names
        # are given to it and to convert them to routing server addresses.
        # This resolver is expected to be called multiple times.
        # The combined use of resolver and domain name resolver allows to host
        # multiple initial routers on a single IP.
        def domainNameResolver(name):
            nonlocal domain_name_resolver_call_num
            nonlocal resolved_addresses
            nonlocal resolved_domain_name_addresses
            if domain_name_resolver_call_num >= len(resolved_addresses):
                return [name]
            expected_name = \
                resolved_addresses[domain_name_resolver_call_num].split(":")[0]
            self.assertEqual(expected_name, name)
            resolved_domain_name_address = \
                resolved_domain_name_addresses[domain_name_resolver_call_num]
            domain_name_resolver_call_num += 1
            return [resolved_domain_name_address]

        driver = Driver(
            self._backend, self._uri_with_context, self._auth, self._userAgent,
            resolverFn=resolver, domainNameResolverFn=domainNameResolver,
            connectionTimeoutMs=1000
        )
        self.start_server(
            self._routingServer1,
            "router_yielding_unknown_failure.script"
        )
        # _routingServer2 is deliberately turned off
        self.start_server(self._routingServer3, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")

        session = driver.session('r', database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer3.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)
        self.assertGreaterEqual(resolver_calls.items(),
                                {self._routingServer1.address: 1}.items())
        self.assertTrue(all(count == 1 for count in resolver_calls.values()))

    def test_should_successfully_acquire_rt_when_router_ip_changes(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("needs verifyConnectivity support")
        ip_addresses = get_ip_addresses()
        if len(ip_addresses) < 2:
            self.skipTest("at least 2 IP addresses are required for this test "
                          "and only linux is supported at the moment")

        router_ip_address = ip_addresses[0]

        def domain_name_resolver(_):
            nonlocal router_ip_address
            return [router_ip_address]

        driver = Driver(
            self._backend, self._uri_with_context, self._auth, self._userAgent,
            domainNameResolverFn=domain_name_resolver
        )
        self.start_server(
            self._routingServer1,
            "router_yielding_reader1_and_exit.script"
        )

        driver.verifyConnectivity()
        self._routingServer1.done()
        router_ip_address = ip_addresses[1]
        self.start_server(
            self._routingServer1,
            "router_yielding_reader1_and_exit.script"
        )
        driver.verifyConnectivity()
        # we don't expect the second router to play the whole script
        self._routingServer1.reset()
        driver.close()

    def test_should_successfully_get_server_protocol_version(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        userAgent=self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")

        session = driver.session('r', database=self.adb)
        result = session.run("RETURN 1 as n")
        list(result)
        summary = result.consume()
        protocol_version = summary.server_info.protocol_version
        session.close()
        driver.close()

        # the server info returns protocol versions in x.y format
        expected_protocol_version = self.bolt_version
        if "." not in expected_protocol_version:
            expected_protocol_version = expected_protocol_version + ".0"
        self.assertEqual(expected_protocol_version, protocol_version)
        self._routingServer1.done()
        self._readServer1.done()

    def test_should_successfully_get_server_agent(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        userAgent=self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1,
                          "reader_with_explicit_hello.script")

        session = driver.session('r', database=self.adb)
        result = session.run("RETURN 1 as n")
        list(result)
        summary = result.consume()
        agent = summary.server_info.agent
        session.close()
        driver.close()

        self.assertEqual(self.server_agent, agent)
        self._routingServer1.done()
        self._readServer1.done()

    def test_should_fail_when_driver_closed_using_session_run(
            self):
        # TODO remove this block once fixed
        if get_driver_name() in ["dotnet", "go", "javascript"]:
            self.skipTest("Skipped because it needs investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")

        session = driver.session('r', database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        summary = result.consume()
        session.close()
        session = driver.session('r', database=self.adb)
        driver.close()

        failed_on_run = False
        try:
            session.run("RETURN 1 as n")
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual(
                    'java.lang.IllegalStateException',
                    e.errorType
                )
            failed_on_run = True

        self.assertTrue(failed_on_run)
        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual(summary.server_info.address,
                         get_dns_resolved_server_address(self._readServer1))
        self.assertEqual([1], sequence)
