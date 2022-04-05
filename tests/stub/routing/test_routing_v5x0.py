from collections import defaultdict
import time

from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    get_dns_resolved_server_address,
    get_driver_name,
    get_ip_addresses,
)

from ._routing import RoutingBase


class RoutingV5x0(RoutingBase):

    required_features = types.Feature.BOLT_5_0,
    bolt_version = "5.0"
    server_agent = "Neo4j/5.0.0"
    adb = "adb"

    def route_call_count(self, server):
        return server.count_requests("ROUTE")

    @driver_feature(types.Feature.BACKEND_RT_FORCE_UPDATE)
    def test_should_successfully_get_routing_table_with_context(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("needs verifyConnectivity support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1,
                          "router_connectivity_db.script")
        driver.update_routing_table()
        driver.close()

        self._routingServer1.done()

    @driver_feature(types.Feature.BACKEND_RT_FETCH)
    def test_should_successfully_get_routing_table(self):
        # TODO: remove this block once all languages support routing table test
        #       API
        # TODO: when all driver support this,
        #       test_should_successfully_get_routing_table_with_context
        #       and all tests (ab)using verifyConnectivity to refresh the RT
        #       should be updated. Tests for verifyConnectivity should be
        #       added.
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        vars_ = self.get_vars()
        self.start_server(self._routingServer1, "router_adb.script",
                          vars_=vars_)
        driver.update_routing_table(self.adb)
        self._routingServer1.done()
        rt = driver.get_routing_table(self.adb)
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

        session = driver.session("r", database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        summary = result.consume()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._readServer1),
                         self._readServer1.address])
        self.assertEqual([1], sequence)

    def test_should_read_successfully_from_reader_using_session_run_with_default_db_driver(  # noqa: E501
            self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_default_db.script")
        self.start_server(self._readServer1, "reader_default_db.script")

        session = driver.session("r")
        result = session.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        summary = result.consume()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._readServer1),
                         self._readServer1.address])
        self.assertEqual([1], sequence)

    # Same test as for session.run but for transaction run.
    def test_should_read_successfully_from_reader_using_tx_run_adb(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader_tx.script")

        session = driver.session("r", database=self.adb)
        tx = session.begin_transaction()
        result = tx.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        summary = result.consume()
        tx.commit()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._readServer1),
                         self._readServer1.address])
        self.assertEqual([1], sequence)

    def test_should_read_successfully_from_reader_using_tx_run_default_db(
            self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_default_db.script")
        self.start_server(self._readServer1, "reader_tx_default_db.script")

        session = driver.session("r")
        tx = session.begin_transaction()
        result = tx.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        summary = result.consume()
        tx.commit()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._readServer1),
                         self._readServer1.address])
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

        session = driver.session("w", database="system")
        tx = session.begin_transaction()
        list(tx.run("CREATE database foo"))
        tx.commit()

        session2 = driver.session("w", bookmarks=session.last_bookmarks(),
                                  database=self.adb)
        result = session2.run("RETURN 1 as n")
        sequence2 = self.collect_records(result)
        session.close()
        session2.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual([1], sequence2)

    def test_should_read_successfully_from_reader_using_tx_function(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("crashes the backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader_tx.script")

        session = driver.session("r", database=self.adb)
        sequences = []
        summaries = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))
            summaries.append(result.consume())

        session.read_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([[1]], sequences)
        self.assertEqual(len(summaries), 1)
        self.assertTrue(summaries[0].server_info.address in
                        [get_dns_resolved_server_address(self._readServer1),
                         self._readServer1.address])

    def _should_fail_when_reading_from_unexpectedly_interrupting_reader_using_session_run(  # noqa: E501
            self, interrupting_reader_script):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, interrupting_reader_script)

        session = driver.session("r", database=self.adb)
        failed = False
        try:
            # drivers doing eager loading will fail here
            session.run("RETURN 1 as n")
        except types.DriverError:
            session.close()
            failed = True
        else:
            try:
                # else they should fail here
                session.close()
            except types.DriverError as e:
                if get_driver_name() in ["java"]:
                    self.assertEqual(
                        "org.neo4j.driver.exceptions.SessionExpiredException",
                        e.errorType)
                elif get_driver_name() in ["ruby"]:
                    self.assertEqual(
                        "Neo4j::Driver::Exceptions::SessionExpiredException",
                        e.errorType)
                failed = True
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(failed)

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_using_session_run(  # noqa: E501
            self):
        self._should_fail_when_reading_from_unexpectedly_interrupting_reader_using_session_run(  # noqa: E501
            "reader_with_unexpected_interruption_on_pipelined_pull.script"
        )

    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_on_run_using_session_run(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("requires investigation")
        self._should_fail_when_reading_from_unexpectedly_interrupting_reader_using_session_run(  # noqa: E501
            "reader_with_unexpected_interruption_on_run.script"
        )

    def _should_fail_when_reading_from_unexpectedly_interrupting_reader_using_tx_run(  # noqa: E501
            self, interrupting_reader_script):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, interrupting_reader_script)

        session = driver.session("r", database=self.adb)
        tx = session.begin_transaction()
        failed = False
        try:
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.SessionExpiredException",
                    e.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::SessionExpiredException",
                    e.errorType
                )
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(failed)

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_using_tx_run(  # noqa: E501
            self):
        self._should_fail_when_reading_from_unexpectedly_interrupting_reader_using_tx_run(  # noqa: E501
            "reader_tx_with_unexpected_interruption_on_pipelined_pull.script"
        )

    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_on_run_using_tx_run(  # noqa: E501
            self):
        self._should_fail_when_reading_from_unexpectedly_interrupting_reader_using_tx_run(  # noqa: E501
            "reader_tx_with_unexpected_interruption_on_run.script"
        )

    def _should_fail_when_reading_from_unexpectedly_interrupting_readers_using_tx_function(  # noqa: E501
            self, interrupting_reader_script):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, max_tx_retry_time_ms=5000)
        self.start_server(self._routingServer1,
                          "router_adb_multi_no_bookmarks.script")
        self.start_server(self._readServer1, interrupting_reader_script)
        self.start_server(self._readServer2, interrupting_reader_script)

        session = driver.session("r", database=self.adb)

        def work(tx):
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()

        with self.assertRaises(types.DriverError) as exc:
            session.read_transaction(work)

        session.close()
        driver.close()

        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.SessionExpiredException",
                exc.exception.errorType
            )
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::SessionExpiredException",
                exc.exception.errorType
            )
        self._routingServer1.done()
        self._readServer1.done()
        self._readServer2.done()

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME,
                    types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_reading_from_unexpectedly_interrupting_readers_using_tx_function(  # noqa: E501
            self):
        self._should_fail_when_reading_from_unexpectedly_interrupting_readers_using_tx_function(  # noqa: E501
            "reader_tx_with_unexpected_interruption_on_pipelined_pull.script"
        )

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME)
    def test_should_fail_when_reading_from_unexpectedly_interrupting_readers_on_run_using_tx_function(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("requires investigation")
        self._should_fail_when_reading_from_unexpectedly_interrupting_readers_using_tx_function(  # noqa: E501
            "reader_tx_with_unexpected_interruption_on_run.script"
        )

    def _should_fail_when_writing_to_unexpectedly_interrupting_writers_using_tx_function(  # noqa: E501
            self, interrupting_writer_script):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, max_tx_retry_time_ms=5000)
        self.start_server(self._routingServer1,
                          "router_adb_multi_no_bookmarks.script")
        self.start_server(self._writeServer1, interrupting_writer_script)
        self.start_server(self._writeServer2, interrupting_writer_script)

        session = driver.session("w", database=self.adb)

        def work(tx):
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()

        with self.assertRaises(types.DriverError) as exc:
            session.write_transaction(work)

        session.close()
        driver.close()

        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.SessionExpiredException",
                exc.exception.errorType
            )
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::SessionExpiredException",
                exc.exception.errorType
            )
        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME,
                    types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_writing_to_unexpectedly_interrupting_writers_using_tx_function(  # noqa: E501
            self):
        self._should_fail_when_writing_to_unexpectedly_interrupting_writers_using_tx_function(  # noqa: E501
            "writer_tx_with_unexpected_interruption_on_pipelined_pull.script"
        )

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME)
    def test_should_fail_when_writing_to_unexpectedly_interrupting_writers_on_run_using_tx_function(  # noqa: E501
            self):
        self._should_fail_when_writing_to_unexpectedly_interrupting_writers_using_tx_function(  # noqa: E501
            "writer_tx_with_unexpected_interruption_on_run.script"
        )

    # Checks that write server is used
    def test_should_write_successfully_on_writer_using_session_run(self):
        # FIXME: test assumes that first writer in RT will be contacted first
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1, "writer.script")

        session = driver.session("w", database=self.adb)
        res = session.run("RETURN 1 as n")
        list(res)
        summary = res.consume()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._writeServer1),
                         self._writeServer1.address])

    # Checks that write server is used
    def test_should_write_successfully_on_writer_using_tx_run(self):
        # FIXME: test assumes that first writer in RT will be contacted first
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1, "writer_tx.script")

        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        res = tx.run("RETURN 1 as n")
        list(res)
        summary = res.consume()
        tx.commit()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._writeServer1),
                         self._writeServer1.address])

    def test_should_write_successfully_on_writer_using_tx_function(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("crashes the backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1, "writer_tx.script")

        session = driver.session("w", database=self.adb)
        res = None
        summary = None

        def work(tx):
            nonlocal res, summary
            res = tx.run("RETURN 1 as n")
            list(res)
            summary = res.consume()

        session.write_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertIsNotNone(res)
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._writeServer1),
                         self._writeServer1.address])

    def test_should_write_successfully_on_leader_switch_using_tx_function(
            self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, None)
        self.start_server(self._routingServer1,
                          "router_adb_multi_no_bookmarks.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_leader_switch_and_retry.script"
        )

        session = driver.session("w", database=self.adb)
        sequences = []

        work_count = 1

        def work(tx):
            nonlocal work_count
            try:
                result = tx.run("RETURN %i.1 as n" % work_count)
                sequences.append(self.collect_records(result))
                result = tx.run("RETURN %i.2 as n" % work_count)
                sequences.append(self.collect_records(result))
            finally:
                # don't simply increase work_count: there is a second writer in
                # in the RT that the driver could try to contact. In that case
                # the tx function will be called 3 times in total
                work_count = 2

        session.write_transaction(work)
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

    def _should_retry_write_until_success_with_leader_change_using_tx_function(
            self, leader_switch_script):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_with_leader_change.script"
        )
        self.start_server(self._writeServer1, leader_switch_script)
        self.start_server(self._writeServer2, "writer_tx.script")

        session = driver.session("w", database=self.adb)
        sequences = []
        num_retries = 0

        def work(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.write_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(2, num_retries)

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_write_until_success_with_leader_change_using_tx_function(  # noqa: E501
            self):
        self._should_retry_write_until_success_with_leader_change_using_tx_function(  # noqa: E501
            "writer_tx_with_unexpected_interruption_on_pipelined_pull.script"
        )

    def test_should_retry_write_until_success_with_leader_change_on_run_using_tx_function(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("requires investigation")
        self._should_retry_write_until_success_with_leader_change_using_tx_function(  # noqa: E501
            "writer_tx_with_unexpected_interruption_on_run.script"
        )

    def test_should_retry_write_until_success_with_leader_shutdown_during_tx_using_tx_function(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
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

        session = driver.session("w", database=self.adb)
        sequences = []
        num_retries = 0

        def work(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.write_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[], []], sequences)
        self.assertEqual(2, num_retries)

    def _should_fail_when_writing_on_unexpectedly_interrupting_writer_using_session_run(  # noqa: E501
            self, interrupting_writer_script):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1, interrupting_writer_script)

        session = driver.session("w", database=self.adb)
        failed = False
        try:
            # drivers doing eager loading will fail here
            result = session.run("RETURN 1 as n")
            # drivers doing lazy loading should fail here
            result.next()
        except types.DriverError:
            session.close()
            failed = True
        else:
            try:
                # else they should fail here
                session.close()
            except types.DriverError as e:
                if get_driver_name() in ["java"]:
                    self.assertEqual(
                        "org.neo4j.driver.exceptions.SessionExpiredException",
                        e.errorType
                    )
                elif get_driver_name() in ["python"]:
                    self.assertEqual(
                        "<class 'neo4j.exceptions.SessionExpired'>",
                        e.errorType
                    )
                elif get_driver_name() in ["ruby"]:
                    self.assertEqual(
                        "Neo4j::Driver::Exceptions::SessionExpiredException",
                        e.errorType
                    )
                failed = True
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_using_session_run(  # noqa: E501
            self):
        self._should_fail_when_writing_on_unexpectedly_interrupting_writer_using_session_run(  # noqa: E501
            "writer_with_unexpected_interruption_on_pipelined_pull.script"
        )

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_run_using_session_run(  # noqa: E501
            self):
        self._should_fail_when_writing_on_unexpectedly_interrupting_writer_using_session_run(  # noqa: E501
            "writer_with_unexpected_interruption_on_run.script"
        )

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_pull_using_session_run(  # noqa: E501
            self):
        self._should_fail_when_writing_on_unexpectedly_interrupting_writer_using_session_run(  # noqa: E501
            "writer_with_unexpected_interruption_on_pull.script"
        )

    def _should_fail_when_writing_on_unexpectedly_interrupting_writer_using_tx_run(  # noqa: E501
            self, interrupting_writer_script, fails_on_next=False):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        if get_driver_name() in ["go", "dotnet"]:
            self.skipTest("needs routing table API support")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1, interrupting_writer_script)

        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        # TODO: It will be removed as soon as JS Driver
        #       has async iterator api
        if get_driver_name() in ["javascript"]:
            fails_on_next = True
        if fails_on_next:
            result = tx.run("RETURN 1 as n")
            with self.assertRaises(types.DriverError) as exc:
                result.next()
        else:
            with self.assertRaises(types.DriverError) as exc:
                tx.run("RETURN 1 as n")

        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.SessionExpiredException",
                exc.exception.errorType
            )
        session.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.get_routing_table(self.adb).writers)

        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_using_tx_run(  # noqa: E501
            self):
        self._should_fail_when_writing_on_unexpectedly_interrupting_writer_using_tx_run(  # noqa: E501
            "writer_tx_with_unexpected_interruption_on_pipelined_pull.script"
        )

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_run_using_tx_run(  # noqa: E501
            self):
        self._should_fail_when_writing_on_unexpectedly_interrupting_writer_using_tx_run(  # noqa: E501
            "writer_tx_with_unexpected_interruption_on_run.script"
        )

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_pull_using_tx_run(  # noqa: E501
            self):
        self._should_fail_when_writing_on_unexpectedly_interrupting_writer_using_tx_run(  # noqa: E501
            "writer_tx_with_unexpected_interruption_on_pull.script",
            fails_on_next=True
        )

    def test_should_fail_discovery_when_router_fails_with_procedure_not_found_code(  # noqa: E501
            self):
        # TODO add support and remove this block
        if get_driver_name() in ["go"]:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_procedure_not_found_failure_connectivity_db"
            ".script"
        )

        failed = False
        try:
            driver.verify_connectivity()
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.ServiceUnavailableException",
                    e.errorType
                )
            elif get_driver_name() in ["python"]:
                self.assertEqual(
                    "<class 'neo4j.exceptions.ServiceUnavailable'>",
                    e.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::ServiceUnavailableException",
                    e.errorType
                )
            failed = True
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def test_should_fail_discovery_when_router_fails_with_unknown_code(self):
        # TODO add support and remove this block
        if get_driver_name() in ["go"]:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_unknown_failure.script"
        )

        failed = False
        try:
            driver.verify_connectivity()
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.ServiceUnavailableException",
                    e.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::ServiceUnavailableException",
                    e.errorType
                )
            failed = True
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go", "dotnet"]:
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

        session = driver.session("w", database=self.adb)
        failed = False
        try:
            session.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.SessionExpiredException",
                    e.errorType
                )
            elif get_driver_name() in ["python"]:
                self.assertEqual(
                    "<class 'neo4j.exceptions.NotALeader'>",
                    e.errorType
                )
                self.assertEqual(
                    "Neo.ClientError.Cluster.NotALeader",
                    e.code
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::SessionExpiredException",
                    e.errorType
                )
            failed = True
        session.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.get_routing_table(self.adb).writers)

        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_forbidden_on_read_only_database(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go", "dotnet"]:
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

        session = driver.session("w", database=self.adb)
        failed = False
        try:
            session.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ["python"]:
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

        self.assertNotIn(self._writeServer1.address,
                         driver.get_routing_table(self.adb).writers)

        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_database_unavailable(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go", "dotnet"]:
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

        session = driver.session("w", database=self.adb)
        failed = False
        try:
            session.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ["python"]:
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

        self.assertIn(self._writeServer1.address,
                      driver.get_routing_table(self.adb).writers)

        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        if get_driver_name() in ["go", "dotnet"]:
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

        session = driver.session("w", database=self.adb)
        failed = False

        try:
            # drivers doing eager loading will fail here
            result = session.run("RETURN 1 as n")
            # drivers doing lazy loading should fail here
            result.next()
        except types.DriverError:
            session.close()
            failed = True
        else:
            try:
                # else they should fail here
                session.close()
            except types.DriverError as e:
                if get_driver_name() in ["java"]:
                    self.assertEqual(
                        "org.neo4j.driver.exceptions.SessionExpiredException",
                        e.errorType
                    )
                elif get_driver_name() in ["python"]:
                    self.assertEqual(
                        "<class 'neo4j.exceptions.NotALeader'>",
                        e.errorType
                    )
                elif get_driver_name() in ["ruby"]:
                    self.assertEqual(
                        "Neo4j::Driver::Exceptions::SessionExpiredException",
                        e.errorType
                    )
                failed = True

        self.assertNotIn(self._writeServer1.address,
                         driver.get_routing_table(self.adb).writers)

        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code_using_tx_run(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("consume not implemented in backend")
        if get_driver_name() in ["go", "dotnet"]:
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

        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        failed = False
        try:
            tx.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.SessionExpiredException",
                    e.errorType
                )
            elif get_driver_name() in ["python"]:
                self.assertEqual(
                    "<class 'neo4j.exceptions.NotALeader'>",
                    e.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::SessionExpiredException",
                    e.errorType
                )
            failed = True
        session.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.get_routing_table(self.adb).writers)

        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code_using_tx_run(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        # TODO remove this block once all languages work
        if get_driver_name() in ["go", "dotnet"]:
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

        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        failed = False
        try:
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.SessionExpiredException",
                    e.errorType
                )
            elif get_driver_name() in ["python"]:
                self.assertEqual(
                    "<class 'neo4j.exceptions.NotALeader'>",
                    e.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::SessionExpiredException",
                    e.errorType
                )
            failed = True
        session.close()

        self.assertNotIn(self._writeServer1.address,
                         driver.get_routing_table(self.adb).writers)

        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_use_write_session_mode_and_initial_bookmark_when_writing_using_tx_run(  # noqa: E501
            self):
        # TODO remove this block once all languages work
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_bookmarks.script"
        )

        session = driver.session("w", bookmarks=["OldBookmark"],
                                 database=self.adb)
        tx = session.begin_transaction()
        list(tx.run("RETURN 1 as n"))
        tx.commit()
        last_bookmarks = session.last_bookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual(["NewBookmark"], last_bookmarks)

    def test_should_use_read_session_mode_and_initial_bookmark_when_reading_using_tx_run(  # noqa: E501
            self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader_tx_with_bookmarks.script")

        session = driver.session("r", bookmarks=["OldBookmark"],
                                 database=self.adb)
        tx = session.begin_transaction()
        result = tx.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        tx.commit()
        last_bookmarks = session.last_bookmarks()
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

        session = driver.session("w", bookmarks=["BookmarkA"],
                                 database=self.adb)
        tx = session.begin_transaction()
        list(tx.run("CREATE (n {name:'Bob'})"))
        tx.commit()
        first_bookmark = session.last_bookmarks()
        tx = session.begin_transaction()
        result = tx.run("MATCH (n) RETURN n.name AS name")
        sequence = self.collect_records(result)
        tx.commit()
        second_bookmark = session.last_bookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual(["Bob"], sequence)
        self.assertEqual(["BookmarkB"], first_bookmark)
        self.assertEqual(["BookmarkC"], second_bookmark)

    def _should_retry_read_tx_until_success_on_error(
            self, interrupting_reader_script):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, interrupting_reader_script)
        self.start_server(self._readServer2, interrupting_reader_script)

        session = driver.session("r", database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            try:
                result = tx.run("RETURN 1 as n")
                sequences.append(self.collect_records(result))
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

        session.read_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual([[1]], sequences)
        self.assertEqual(2, try_count)

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_read_tx_until_success_on_error(self):
        self._should_retry_read_tx_until_success_on_error(
            "reader_tx_with_unexpected_interruption_on_pipelined_pull.script"
        )

    def test_should_retry_read_tx_until_success_on_run_error(self):
        self._should_retry_read_tx_until_success_on_error(
            "reader_tx_with_unexpected_interruption_on_run.script"
        )

    def test_should_retry_read_tx_until_success_on_pull_error(self):
        self._should_retry_read_tx_until_success_on_error(
            "reader_tx_with_unexpected_interruption_on_pull.script"
        )

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

        session = driver.session("r", database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.read_transaction(work)
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

        session.read_transaction(work)
        session.close()
        driver.close()

        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual([[1], [1]], sequences)
        # Drivers might or might not try the first server again
        self.assertLessEqual(try_count, 3)
        # TODO: Design a test that makes sure the driver doesn't run the tx
        #       func if it can't establish a working connection to the server.
        #       So that `try_count == 2`. When doing so be aware that drivers
        #       could do round robin, e.g. Java.

    def _should_retry_write_tx_until_success_on_error(
            self, interrupting_writer_script):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1, interrupting_writer_script)
        self.start_server(self._writeServer2, interrupting_writer_script)

        session = driver.session("w", database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            try:
                result = tx.run("RETURN 1 as n")
                sequences.append(self.collect_records(result))
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

        session.write_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(2, try_count)

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_write_tx_until_success_on_error(self):
        self._should_retry_write_tx_until_success_on_error(
            "writer_tx_with_unexpected_interruption_on_pipelined_pull.script"
        )

    def test_should_retry_write_tx_until_success_on_run_error(self):
        self._should_retry_write_tx_until_success_on_error(
            "writer_tx_with_unexpected_interruption_on_run.script"
        )

    def test_should_retry_write_tx_until_success_on_pull_error(self):
        self._should_retry_write_tx_until_success_on_error(
            "writer_tx_with_unexpected_interruption_on_pull.script"
        )

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

        session = driver.session("r", database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.write_transaction(work)
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

        session.write_transaction(work)
        session.close()
        driver.close()

        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[], []], sequences)
        # Drivers might or might not try the first server again
        self.assertLessEqual(try_count, 3)
        # TODO: Design a test that makes sure the driver doesn't run the tx
        #       func if it can't establish a working connection to the server.
        #       So that `try_count == 2`. When doing so be aware that drivers
        #       could do round robin, e.g. Java.

    def _should_retry_read_tx_and_rediscovery_until_success(
            self, interrupting_reader_script):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_router2.script"
        )
        self.start_server(self._routingServer2,
                          "router_yielding_reader2_adb.script")
        self.start_server(self._readServer1, interrupting_reader_script)
        self.start_server(self._readServer2, "reader_tx.script")
        self.start_server(self._readServer3, interrupting_reader_script)

        session = driver.session("r", database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.read_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._readServer1.done()
        self._readServer2.done()
        self._readServer3.done()
        self.assertEqual([[1]], sequences)
        self.assertEqual(3, try_count)

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_read_tx_and_rediscovery_until_success(self):
        self._should_retry_read_tx_and_rediscovery_until_success(
            "reader_tx_with_unexpected_interruption_on_pipelined_pull.script"
        )

    def test_should_retry_read_tx_and_rediscovery_until_success_on_run_failure(
            self):
        self._should_retry_read_tx_and_rediscovery_until_success(
            "reader_tx_with_unexpected_interruption_on_run.script"
        )

    def test_should_retry_read_tx_and_rediscovery_until_success_on_pull_failure(  # noqa: E501
            self):
        self._should_retry_read_tx_and_rediscovery_until_success(
            "reader_tx_with_unexpected_interruption_on_pull.script"
        )

    def _should_retry_write_tx_and_rediscovery_until_success(
            self, interrupting_writer_script):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_router2.script"
        )
        self.start_server(self._routingServer2,
                          "router_yielding_reader2_adb.script")
        self.start_server(self._writeServer1, interrupting_writer_script)
        self.start_server(self._writeServer2, "writer_tx.script")
        self.start_server(self._writeServer3, interrupting_writer_script)

        session = driver.session("w", database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.write_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self._writeServer3.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(3, try_count)

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_write_tx_and_rediscovery_until_success(self):
        self._should_retry_write_tx_and_rediscovery_until_success(
            "writer_tx_with_unexpected_interruption_on_pipelined_pull.script"
        )

    def test_should_retry_write_tx_and_rediscovery_until_success_on_run_failure(  # noqa: E501
            self):
        self._should_retry_write_tx_and_rediscovery_until_success(
            "writer_tx_with_unexpected_interruption_on_run.script"
        )

    def test_should_retry_write_tx_and_rediscovery_until_success_on_pull_failure(  # noqa: E501
            self):
        self._should_retry_write_tx_and_rediscovery_until_success(
            "writer_tx_with_unexpected_interruption_on_pull.script"
        )

    @driver_feature(types.Feature.BACKEND_RT_FORCE_UPDATE)
    def test_should_use_initial_router_for_discovery_when_others_unavailable(
            self):
        # TODO add support and remove this block
        if get_driver_name() in ["go"]:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_router2_and_fake_reader.script"
        )
        self.start_server(self._readServer1, "reader_tx.script")

        driver.update_routing_table()
        self._routingServer1.done()
        self.start_server(self._routingServer1, "router_adb.script")
        session = driver.session("r", database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.read_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([[1]], sequences)

    def test_should_successfully_read_from_readable_router_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Test failing for some reason")
        # Some drivers (for instance, java) may use separate connections for
        # readers and writers when they are addressed by domain names in
        # routing table. Since this test is not for testing DNS resolution,
        # it has been switched to IP-based address model.
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

        session = driver.session("r", database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.read_transaction(work)
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
        # Some drivers (for instance, java) may use separate connections for
        # readers and writers when they are addressed by domain names in
        # routing table. Since this test is not for testing DNS resolution,
        # it has been switched to IP-based address model.
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

        session = driver.session("r", database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.read_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self.assertEqual(self._routingServer1.count_requests("COMMIT"), 1)
        self.assertEqual([[1]], sequences)

    def test_should_serve_reads_and_fail_writes_when_no_writers_available(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
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

        session = driver.session("w", database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.read_transaction(work)

        failed = False
        try:
            session.run("CREATE (n {name:'Bob'})").consume()
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.SessionExpiredException",
                    e.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::SessionExpiredException",
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

    @driver_feature(types.Feature.BACKEND_RT_FORCE_UPDATE)
    def test_should_accept_routing_table_without_writers_and_then_rediscover(
            self):
        # TODO add support and remove this block
        if get_driver_name() in ["go"]:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_no_writers_any_db.script"
        )
        self.start_server(self._readServer1, "reader_tx_with_bookmarks.script")
        self.start_server(self._writeServer1, "writer_with_bookmark.script")

        driver.update_routing_table()
        session = driver.session("w", bookmarks=["OldBookmark"],
                                 database=self.adb)
        sequences = []
        self._routingServer1.done()
        try:
            driver.update_routing_table()
        except types.DriverError:
            # make sure the driver noticed that its old connection to
            # _routingServer1 is dead
            pass
        self.start_server(self._routingServer1, "router_adb.script")

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.read_transaction(work)
        list(session.run("RETURN 1 as n"))
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self._writeServer1.done()
        self.assertEqual([[1]], sequences)

    @driver_feature(types.Feature.BACKEND_RT_FETCH,
                    types.Feature.BACKEND_RT_FORCE_UPDATE)
    def test_should_fail_on_routing_table_with_no_reader(self):
        if get_driver_name() in ["go", "java", "dotnet"]:
            self.skipTest("needs routing table API support")
        self.start_server(
            self._routingServer1,
            "router_yielding_no_readers_any_db.script"
        )
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)

        with self.assertRaises(types.DriverError) as exc:
            driver.update_routing_table()

        if get_driver_name() in ["python"]:
            self.assertEqual(
                exc.exception.errorType,
                "<class 'neo4j.exceptions.ServiceUnavailable'>"
            )
        elif get_driver_name() in ["javascript"]:
            self.assertEqual(
                exc.exception.code,
                "ServiceUnavailable"
            )

        routing_table = driver.get_routing_table()
        self.assertEqual(routing_table.routers, [])
        self.assertEqual(routing_table.readers, [])
        self.assertEqual(routing_table.writers, [])
        self._routingServer1.done()
        driver.close()

    def test_should_accept_routing_table_with_single_router(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")
        self.start_server(self._readServer2, "reader.script")

        session = driver.session("r", database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collect_records(result)
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
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._writeServer1,
                          "writer_tx_with_multiple_bookmarks.script")

        session = driver.session(
            "w",
            bookmarks=[
                "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29",
                "neo4j:bookmark:v1:tx94", "neo4j:bookmark:v1:tx56",
                "neo4j:bookmark:v1:tx16", "neo4j:bookmark:v1:tx68"
            ],
            database=self.adb
        )
        tx = session.begin_transaction()
        list(tx.run("RETURN 1 as n"))
        tx.commit()
        last_bookmarks = session.last_bookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual(["neo4j:bookmark:v1:tx95"], last_bookmarks)

    def test_should_forget_address_on_database_unavailable_error(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
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

        session = driver.session("w", database=self.adb)
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.write_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(2, try_count)

    def test_should_use_resolver_during_rediscovery_when_existing_routers_fail(
            self):
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
                        self._userAgent, resolver_fn=resolver)
        self.start_server(
            self._routingServer1,
            "router_yielding_reader1_and_exit.script"
        )
        self.start_server(self._routingServer2, "router_adb.script")
        self.start_server(self._readServer1, "reader_tx_with_exit.script")
        self.start_server(self._readServer2, "reader_tx.script")

        session = driver.session("w", database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.read_transaction(work)
        session.read_transaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual([[1], [1]], sequences)

    def test_should_revert_to_initial_router_if_known_router_throws_protocol_errors(  # noqa: E501
            self):
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

        session = driver.session("r", database=self.adb)
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collect_records(result))

        session.read_transaction(work)

        session.close()
        driver.close()
        self._routingServer1.done()
        self._routingServer2.done()
        self._routingServer3.done()
        self._readServer1.done()
        self.assertEqual([[1]], sequences)
        if len(resolver_calls) == 1:
            # driver that calls resolver function only on initial router
            # address
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

    def test_should_successfully_check_if_support_for_multi_db_is_available(
            self):
        # TODO add support and remove this block
        if get_driver_name() in ["go"]:
            self.skipTest("supportsMultiDb not implemented in backend")

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_default_db.script")
        self.start_server(self._readServer1, "empty_reader.script")

        supports_multi_db = driver.supports_multi_db()

        # we don't expect the router or the reader to play the whole
        # script
        self._routingServer1.reset()
        self._readServer1.reset()
        driver.close()
        self.assertLessEqual(self._readServer1.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._readServer1.count_requests("RUN"), 0)
        self.assertEqual(self.should_support_multi_db(), supports_multi_db)

    def test_should_read_successfully_on_empty_discovery_result_using_session_run(  # noqa: E501
            self):
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

        session = driver.session("r", database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer2.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)

    def test_should_fail_with_routing_failure_on_db_not_found_discovery_failure(  # noqa: E501
            self):
        if not self.should_support_multi_db():
            return

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_db_not_found_failure.script"
        )

        session = driver.session("r", database=self.adb)
        failed = False
        try:
            result = session.run("RETURN 1 as n")
            result.next()
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.FatalDiscoveryException",
                    e.errorType
                )
            elif get_driver_name() in ["python"]:
                self.assertEqual(
                    "<class 'neo4j.exceptions.ClientError'>",
                    e.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::FatalDiscoveryException",
                    e.errorType
                )

            self.assertEqual("Neo.ClientError.Database.DatabaseNotFound",
                             e.code)
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def _test_fast_fail_discover(self, script):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, script)

        session = driver.session("r", database=self.adb, bookmarks=["foobar"])
        with self.assertRaises(types.DriverError) as exc:
            result = session.run("RETURN 1 as n")
            result.next()

        session.close()
        driver.close()

        self._routingServer1.done()

        return exc

    @driver_feature(types.Feature.TMP_FAST_FAILING_DISCOVERY)
    def test_should_fail_with_routing_failure_on_invalid_bookmark_discovery_failure(  # noqa: E501
            self):
        exc = self._test_fast_fail_discover(
            "router_yielding_invalid_bookmark_failure.script",
        )
        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.ClientException",
                exc.exception.errorType
            )
        elif get_driver_name() in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.ClientError'>",
                exc.exception.errorType
            )
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::ClientException",
                exc.exception.errorType
            )
        self.assertEqual("Neo.ClientError.Transaction.InvalidBookmark",
                         exc.exception.code)

    @driver_feature(types.Feature.TMP_FAST_FAILING_DISCOVERY)
    def test_should_fail_with_routing_failure_on_invalid_bookmark_mixture_discovery_failure(  # noqa: E501
            self):
        exc = self._test_fast_fail_discover(
            "router_yielding_invalid_bookmark_mixture_failure.script",
        )
        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.ClientException",
                exc.exception.errorType
            )
        elif get_driver_name() in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.ClientError'>",
                exc.exception.errorType
            )
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::ClientException",
                exc.exception.errorType
            )

        self.assertEqual("Neo.ClientError.Transaction.InvalidBookmarkMixture",
                         exc.exception.code)

    @driver_feature(types.Feature.TMP_FAST_FAILING_DISCOVERY)
    def test_should_fail_with_routing_failure_on_forbidden_discovery_failure(
            self):
        exc = self._test_fast_fail_discover(
            "router_yielding_forbidden_failure.script",
        )
        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.SecurityException",
                exc.exception.errorType
            )
        elif get_driver_name() in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.Forbidden'>",
                exc.exception.errorType
            )
        if get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::SecurityException",
                exc.exception.errorType
            )
        self.assertEqual(
            "Neo.ClientError.Security.Forbidden",
            exc.exception.code)

    @driver_feature(types.Feature.TMP_FAST_FAILING_DISCOVERY)
    def test_should_fail_with_routing_failure_on_any_security_discovery_failure(  # noqa: E501
            self):
        exc = self._test_fast_fail_discover(
            "router_yielding_any_security_failure.script",
        )
        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.SecurityException",
                exc.exception.errorType
            )
        elif get_driver_name() in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.ClientError'>",
                exc.exception.errorType
            )
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::SecurityException",
                exc.exception.errorType
            )
        self.assertEqual(
            "Neo.ClientError.Security.MadeUpSecurityError",
            exc.exception.code)

    def test_should_read_successfully_from_reachable_db_after_trying_unreachable_db(  # noqa: E501
            self):
        if get_driver_name() in ["go"]:
            self.skipTest("requires investigation")

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_unreachable_db_then_adb.script"
        )
        self.start_server(self._readServer1, "reader.script")

        session = driver.session("r", database="unreachable")
        failed_on_unreachable = False
        try:
            result = session.run("RETURN 1 as n")
            self.collect_records(result)
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.ServiceUnavailableException",
                    e.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::ServiceUnavailableException",
                    e.errorType
                )
            failed_on_unreachable = True
        session.close()

        session = driver.session("r", database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        self.assertEqual(self.route_call_count(self._routingServer1), 2)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(failed_on_unreachable)
        self.assertEqual([1], sequence)

    def test_should_ignore_system_bookmark_when_getting_rt_for_multi_db(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader_with_bookmarks.script")

        session = driver.session("r", database=self.adb,
                                 bookmarks=["sys:1234", "foo:5678"])
        result = session.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        last_bookmarks = session.last_bookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)
        self.assertEqual(["foo:6678"], last_bookmarks)

    def _test_should_request_rt_from_all_initial_routers_until_successful(
            self, failure_script):

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
        def domain_name_resolver(name):
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
            resolver_fn=resolver, domain_name_resolver_fn=domain_name_resolver,
            connection_timeout_ms=1000
        )
        self.start_server(self._routingServer1, failure_script)
        # _routingServer2 is deliberately turned off
        self.start_server(self._routingServer3, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")

        session = driver.session("r", database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer3.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)
        self.assertGreaterEqual(resolver_calls.items(),
                                {self._routingServer1.address: 1}.items())
        self.assertTrue(all(count == 1 for count in resolver_calls.values()))

    @driver_feature(types.Feature.TMP_FAST_FAILING_DISCOVERY)
    def test_should_request_rt_from_all_initial_routers_until_successful_on_unknown_failure(  # noqa: E501
            self):
        self._test_should_request_rt_from_all_initial_routers_until_successful(
            "router_yielding_unknown_failure.script"
        )

    @driver_feature(types.Feature.TMP_FAST_FAILING_DISCOVERY)
    def test_should_request_rt_from_all_initial_routers_until_successful_on_authorization_expired(  # noqa: E501
            self):
        self._test_should_request_rt_from_all_initial_routers_until_successful(
            "router_yielding_authorization_expired_failure.script"
        )

    @driver_feature(types.Feature.BACKEND_RT_FORCE_UPDATE)
    def test_should_successfully_acquire_rt_when_router_ip_changes(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ["go"]:
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
            domain_name_resolver_fn=domain_name_resolver
        )
        self.start_server(
            self._routingServer1,
            "router_yielding_reader1_and_exit.script"
        )

        driver.update_routing_table()
        self._routingServer1.done()
        router_ip_address = ip_addresses[1]
        self.start_server(
            self._routingServer1,
            "router_yielding_reader1_and_exit.script"
        )
        driver.update_routing_table()
        # we don't expect the second router to play the whole script
        self._routingServer1.reset()
        driver.close()

    def test_should_successfully_get_server_protocol_version(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")

        session = driver.session("r", database=self.adb)
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
                        user_agent=self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1,
                          "reader_with_explicit_hello.script")

        session = driver.session("r", database=self.adb)
        result = session.run("RETURN 1 as n")
        list(result)
        summary = result.consume()
        agent = summary.server_info.agent
        session.close()
        driver.close()

        self.assertEqual(self.server_agent, agent)
        self._routingServer1.done()
        self._readServer1.done()

    def test_should_fail_when_driver_closed_using_session_run(self):
        # TODO remove this block once fixed
        if get_driver_name() in ["dotnet", "go", "javascript"]:
            self.skipTest("Skipped because it needs investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(self._readServer1, "reader.script")

        session = driver.session("r", database=self.adb)
        result = session.run("RETURN 1 as n")
        sequence = self.collect_records(result)
        summary = result.consume()
        session.close()
        session = driver.session("r", database=self.adb)
        driver.close()

        failed_on_run = False
        try:
            session.run("RETURN 1 as n")
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "java.lang.IllegalStateException",
                    e.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::IllegalStateException",
                    e.errorType
                )
            failed_on_run = True

        self.assertTrue(failed_on_run)
        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(summary.server_info.address in
                        [get_dns_resolved_server_address(self._readServer1),
                         self._readServer1.address])
        self.assertEqual([1], sequence)

    def test_should_fail_when_writing_without_writers_using_session_run(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1,
                          "router_yielding_no_writers_adb_sequentially.script")

        session = driver.session("w", database=self.adb)

        failed = False
        try:
            # drivers doing eager loading will fail here
            result = session.run("RETURN 1 as n")
            # drivers doing lazy loading should fail here
            result.next()
        except types.DriverError as e:
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.SessionExpiredException",
                    e.errorType
                )
            elif get_driver_name() in ["python"]:
                self.assertEqual(
                    "<class 'neo4j.exceptions.SessionExpired'>",
                    e.errorType
                )
            failed = True

        session.close()
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def test_should_write_successfully_after_leader_switch_using_tx_run(self):
        # TODO remove this block once fixed
        if get_driver_name() in ["go"]:
            self.skipTest("Fails on tx rollback attempt")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, None)
        self.start_server(self._routingServer1,
                          "router_with_leader_change.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_yielding_failure_on_run.script",
            vars_={
                **self.get_vars(),
                "#FAILURE#": '{"code": "Neo.ClientError.Cluster.NotALeader", '
                             '"message": "message"}'
            }
        )
        self.start_server(
            self._writeServer2,
            "writer_tx.script"
        )

        session = driver.session("w", database=self.adb)

        # Attempts:
        # 1 - 1 writer that returns an error
        # 2 - 1 writer that does not respond
        # 3 - 0 writers
        for attempt in range(3):
            with self.assertRaises(types.DriverError) as e:
                tx = session.begin_transaction()
                result = tx.run("RETURN 1 as n")
                # drivers doing lazy loading should fail here
                result.next()
            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.SessionExpiredException",
                    e.exception.errorType
                )
            elif get_driver_name() in ["python"]:
                if attempt == 0:
                    self.assertEqual(
                        "<class 'neo4j.exceptions.NotALeader'>",
                        e.exception.errorType
                    )
                else:
                    self.assertEqual(
                        "<class 'neo4j.exceptions.SessionExpired'>",
                        e.exception.errorType
                    )
            if attempt == 0:
                tx.rollback()
            self._writeServer1.done()

        tx = session.begin_transaction()
        list(tx.run("RETURN 1 as n"))
        tx.commit()

        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer2.done()

    def test_should_rediscover_when_all_connections_fail_using_s_and_tx_run(
            self):
        # TODO remove this block once fixed
        if get_driver_name() in ["go"]:
            self.skipTest("Session close fails with ConnectivityError")
        # TODO remove this block once fixed
        if get_driver_name() in ["javascript"]:
            self.skipTest("write_session result consumption times out")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_writer1_sequentially.script")
        self.start_server(
            self._writeServer1,
            "writer_with_unexpected_interruption_on_second_run.script")
        self.start_server(
            self._readServer1,
            "reader_tx_with_unexpected_interruption_on_second_run.script")
        self.start_server(
            self._readServer2,
            "reader_tx_with_unexpected_interruption_on_second_run.script")

        write_session = driver.session("w", database=self.adb)
        read_session1 = driver.session("r", database=self.adb)
        read_tx1 = read_session1.begin_transaction()
        read_session2 = driver.session("r", database=self.adb)
        read_tx2 = read_session2.begin_transaction()

        list(write_session.run("RETURN 1 as n"))
        list(read_tx1.run("RETURN 1 as n"))
        list(read_tx2.run("RETURN 1 as n"))
        read_tx1.commit()
        read_tx2.commit()

        def run_and_assert_error(runner):
            with self.assertRaises(types.DriverError) as exc:
                # drivers doing eager loading will fail here
                result = runner.run("RETURN 1 as n")
                # drivers doing lazy loading should fail here
                result.next()

            if get_driver_name() in ["java"]:
                self.assertEqual(
                    "org.neo4j.driver.exceptions.SessionExpiredException",
                    exc.exception.errorType
                )
            elif get_driver_name() in ["python"]:
                self.assertEqual(
                    "<class 'neo4j.exceptions.SessionExpired'>",
                    exc.exception.errorType
                )
            elif get_driver_name() in ["ruby"]:
                self.assertEqual(
                    "Neo4j::Driver::Exceptions::SessionExpiredException",
                    exc.exception.errorType
                )

        run_and_assert_error(write_session)
        run_and_assert_error(read_session1.begin_transaction())
        run_and_assert_error(read_session2.begin_transaction())

        write_session.close()
        read_session1.close()
        read_session2.close()
        route_count1 = self.route_call_count(self._routingServer1)
        self._writeServer1.done()
        self._readServer1.done()
        self._readServer2.done()

        self.start_server(self._writeServer1, "writer.script")
        self.start_server(self._readServer1, "reader.script")

        write_session = driver.session("w", database=self.adb)
        list(write_session.run("RETURN 1 as n"))

        read_session1 = driver.session("r", database=self.adb)
        list(read_session1.run("RETURN 1 as n"))

        driver.close()
        self._routingServer1.done()
        route_count2 = self.route_call_count(self._routingServer1)
        self.assertTrue(route_count2 > route_count1 > 0)
        self._writeServer1.done()
        self._readServer1.done()

    def test_should_succeed_when_another_conn_fails_and_discover_using_tx_run(
            self):
        # TODO remove this block once fixed
        if get_driver_name() in ["go"]:
            self.skipTest("Session close fails with ConnectivityError")
        # TODO remove this block once fixed
        if get_driver_name() in ["javascript"]:
            self.skipTest("Transaction result consumption times out")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_yielding_writer1_sequentially.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_unexpected_interruption_on_run_path.script")

        session1 = driver.session("w", database=self.adb)
        tx1 = session1.begin_transaction()
        session2 = driver.session("w", database=self.adb)
        tx2 = session2.begin_transaction()

        list(tx1.run("RETURN 1 as n"))

        with self.assertRaises(types.DriverError) as exc:
            # drivers doing eager loading will fail here
            result = tx2.run("RETURN 5 as n")
            # drivers doing lazy loading should fail here
            result.next()

        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.SessionExpiredException",
                exc.exception.errorType
            )
        elif get_driver_name() in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.SessionExpired'>",
                exc.exception.errorType
            )
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::SessionExpiredException",
                exc.exception.errorType
            )

        tx1.commit()
        session1.close()
        session2.close()
        route_count1 = self.route_call_count(self._routingServer1)

        # Another discovery is expected since the only writer failed in tx2
        session = driver.session("w", database=self.adb)
        tx = session.begin_transaction()
        list(tx.run("RETURN 1 as n"))
        tx.commit()

        session.close()
        driver.close()
        self._routingServer1.done()
        route_count2 = self.route_call_count(self._routingServer1)
        self.assertTrue(route_count2 > route_count1 > 0)
        self._writeServer1.done()

    def test_should_get_rt_from_leader_w_and_r_via_leader_using_session_run(
            self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(
            self._routingServer1,
            "router_and_writer_with_sequential_access_and_bookmark.script"
        )

        session = driver.session("w", database=self.adb)
        list(session.run("RETURN 1 as n"))
        records = list(session.run("RETURN 5 as n"))
        session.close()
        driver.close()

        self._routingServer1.done()
        self.assertEqual([types.Record(values=[types.CypherInt(1)])], records)

    def test_should_get_rt_from_follower_w_and_r_via_leader_using_session_run(
            self):
        # TODO remove this block once fixed
        if get_driver_name() in ["javascript"]:
            self.skipTest("Requires investigation")
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)
        self.start_server(self._routingServer1, "router_adb.script")
        self.start_server(
            self._writeServer1,
            "writer_with_sequential_access_and_bookmark.script"
        )

        session = driver.session("w", database=self.adb)
        list(session.run("RETURN 1 as n"))
        records = list(session.run("RETURN 5 as n"))
        session.close()
        driver.close()

        self._routingServer1.done()
        self.assertEqual([types.Record(values=[types.CypherInt(1)])], records)

    @driver_feature(types.Feature.API_LIVENESS_CHECK,
                    types.Feature.TMP_GET_CONNECTION_POOL_METRICS)
    def test_should_drop_connections_failing_liveness_check(self):
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, liveness_check_timeout_ms=0)
        self.start_server(self._routingServer1,
                          "router_adb_multi_no_bookmarks.script")
        self.start_server(
            self._writeServer1,
            "writer_tx_with_unexpected_interruption_on_status_check.script"
        )

        sessions = []
        txs = []

        for _ in range(5):
            session = driver.session("w", database=self.adb)
            tx = session.begin_transaction()
            sessions.append(session)
            txs.append(tx)

        for tx in txs:
            list(tx.run("RETURN 1 as n"))
            tx.commit()

        for session in sessions:
            session.close()

        self._wait_for_idle_connections(driver, 5)

        session = driver.session("w", database=self.adb)
        list(session.run("RETURN 1 as n"))
        session.close()

        self._wait_for_idle_connections(driver, 1)
        driver.close()
        self._routingServer1.done()
        self._writeServer1.done()

    @driver_feature(types.Feature.TMP_DRIVER_MAX_CONNECTION_POOL_SIZE,
                    types.Feature.API_CONNECTION_ACQUISITION_TIMEOUT)
    def test_should_enforce_pool_size_per_cluster_member(self):
        acq_timeout_ms = 2000
        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent, max_connection_pool_size=1,
                        connection_acquisition_timeout_ms=acq_timeout_ms)
        self.start_server(self._routingServer1,
                          "router_adb_multi_no_bookmarks.script")
        self.start_server(self._writeServer1, "writer_tx.script")
        self.start_server(self._writeServer2, "writer_tx.script")
        self.start_server(self._readServer1, "reader_tx.script")

        session0 = driver.session("w", database=self.adb)
        tx0 = session0.begin_transaction()

        session1 = driver.session("w", database=self.adb)
        tx1 = session1.begin_transaction()

        session2 = driver.session("w", database=self.adb)

        if self.driver_supports_features(types.Feature.OPT_EAGER_TX_BEGIN):
            with self.assertRaises(types.DriverError) as exc:
                session2.begin_transaction()
        else:
            with self.assertRaises(types.DriverError) as exc:
                tx = session2.begin_transaction()
                list(tx.run("RETURN 1 as n"))

        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.ClientException",
                exc.exception.errorType
            )
            self.assertTrue("Unable to acquire connection from the "
                            "pool within configured maximum time of "
                            f"{acq_timeout_ms}ms"
                            in exc.exception.msg)

        session2.close()

        session3 = driver.session("r", database=self.adb)
        tx3 = session3.begin_transaction()
        list(tx3.run("RETURN 1 as n"))
        tx3.commit()
        session3.close()

        list(tx0.run("RETURN 1 as n"))
        tx0.commit()
        session0.close()
        list(tx1.run("RETURN 1 as n"))
        tx1.commit()
        session1.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self._readServer1.done()

    def _wait_for_idle_connections(self, driver, expected_idle_connections):
        attempts = 0
        while True:
            metrics = driver.get_connection_pool_metrics(
                self._writeServer1.address)
            if metrics.idle == expected_idle_connections:
                break
            attempts += 1
            if attempts == 10:
                self.fail("Timeout out waiting for idle connections")
            time.sleep(.1)

    def test_does_not_use_read_connection_for_write(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript", "go", "dotnet", "ruby"]:
            self.skipTest("Requires address field in summary")

        def read(tx):
            result = tx.run("RETURN 1 as n")
            list(result)  # exhaust the result
            return result.consume()

        def write(tx):
            result = tx.run("RETURN 1 as n")
            list(result)  # exhaust the result
            return result.consume()

        self.start_server(self._routingServer1,
                          "router_adb_multi_no_bookmarks.script")
        self.start_server(self._writeServer1, "writer_tx.script")
        self.start_server(self._readServer1, "reader_tx.script")

        driver = Driver(self._backend, self._uri_with_context, self._auth,
                        self._userAgent)

        session = driver.session("w", database=self.adb)

        read_summary = session.read_transaction(read)
        write_summary = session.write_transaction(write)

        self.assertNotEqual(read_summary.server_info.address,
                            write_summary.server_info.address)
