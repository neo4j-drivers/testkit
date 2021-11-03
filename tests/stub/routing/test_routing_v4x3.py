import nutkit.protocol as types
from tests.shared import driver_feature

from .test_routing_v4x4 import RoutingV4x4


class RoutingV4x3(RoutingV4x4):

    required_features = types.Feature.BOLT_4_3,
    bolt_version = "4.3"
    server_agent = "Neo4j/4.3.0"
    adb = "adb"

    def test_should_successfully_get_routing_table_with_context(self):
        super().test_should_successfully_get_routing_table_with_context()

    def test_should_successfully_get_routing_table(self):
        super().test_should_successfully_get_routing_table()

    # Checks that routing is used to connect to correct server and that
    # parameters for session run is passed on to the target server
    # (not the router).
    def test_should_read_successfully_from_reader_using_session_run(self):
        super().test_should_read_successfully_from_reader_using_session_run()

    def test_should_read_successfully_from_reader_using_session_run_with_default_db_driver(self):
        super().test_should_read_successfully_from_reader_using_session_run_with_default_db_driver()

    def test_should_read_successfully_from_reader_using_tx_run_default_db(self):
        super().test_should_read_successfully_from_reader_using_tx_run_default_db()

    def test_should_send_system_bookmark_with_route(self):
        super().test_should_send_system_bookmark_with_route()

    def test_should_read_successfully_from_reader_using_tx_function(self):
        super().test_should_read_successfully_from_reader_using_tx_function()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_using_session_run(
            self):
        super().test_should_fail_when_reading_from_unexpectedly_interrupting_reader_using_session_run()

    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_on_run_using_session_run(
            self):
        super().test_should_fail_when_reading_from_unexpectedly_interrupting_reader_on_run_using_session_run()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_using_tx_run(
            self):
        super().test_should_fail_when_reading_from_unexpectedly_interrupting_reader_using_tx_run()

    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_on_run_using_tx_run(
            self):
        super().test_should_fail_when_reading_from_unexpectedly_interrupting_reader_on_run_using_tx_run()

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME,
                    types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_reading_from_unexpectedly_interrupting_readers_using_tx_function(
            self):
        super().test_should_fail_when_reading_from_unexpectedly_interrupting_readers_using_tx_function()

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME)
    def test_should_fail_when_reading_from_unexpectedly_interrupting_readers_on_run_using_tx_function(
            self):
        super().test_should_fail_when_reading_from_unexpectedly_interrupting_readers_on_run_using_tx_function()

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME,
                    types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_writing_to_unexpectedly_interrupting_writers_using_tx_function(
            self):
        super().test_should_fail_when_writing_to_unexpectedly_interrupting_writers_using_tx_function()

    @driver_feature(types.Feature.TMP_DRIVER_MAX_TX_RETRY_TIME)
    def test_should_fail_when_writing_to_unexpectedly_interrupting_writers_on_run_using_tx_function(
            self):
        super().test_should_fail_when_writing_to_unexpectedly_interrupting_writers_on_run_using_tx_function()

    def test_should_write_successfully_on_writer_using_session_run(self):
        super().test_should_write_successfully_on_writer_using_session_run()

    def test_should_write_successfully_on_writer_using_tx_run(self):
        super().test_should_write_successfully_on_writer_using_tx_run()

    def test_should_write_successfully_on_writer_using_tx_function(self):
        super().test_should_write_successfully_on_writer_using_tx_function()

    def test_should_write_successfully_on_leader_switch_using_tx_function(self):
        super().test_should_write_successfully_on_leader_switch_using_tx_function()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_write_until_success_with_leader_change_using_tx_function(
            self):
        super().test_should_retry_write_until_success_with_leader_change_using_tx_function()

    def test_should_retry_write_until_success_with_leader_change_on_run_using_tx_function(
            self):
        super().test_should_retry_write_until_success_with_leader_change_on_run_using_tx_function()

    def test_should_retry_write_until_success_with_leader_shutdown_during_tx_using_tx_function(
            self):
        super().test_should_retry_write_until_success_with_leader_shutdown_during_tx_using_tx_function()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_using_session_run(
            self):
        super().test_should_fail_when_writing_on_unexpectedly_interrupting_writer_using_session_run()

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_run_using_session_run(
            self):
        super().test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_run_using_session_run()

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_pull_using_session_run(
            self):
        super().test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_pull_using_session_run()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_using_tx_run(
            self):
        super().test_should_fail_when_writing_on_unexpectedly_interrupting_writer_using_tx_run()

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_run_using_tx_run(
            self):
        super().test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_run_using_tx_run()

    def test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_pull_using_tx_run(
            self):
        super().test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_pull_using_tx_run()

    def test_should_fail_discovery_when_router_fails_with_procedure_not_found_code(
            self):
        super().test_should_fail_discovery_when_router_fails_with_procedure_not_found_code()

    def test_should_fail_discovery_when_router_fails_with_unknown_code(self):
        super().test_should_fail_discovery_when_router_fails_with_unknown_code()

    def test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code(self):
        super().test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code()

    def test_should_fail_when_writing_on_writer_that_returns_forbidden_on_read_only_database(self):
        super().test_should_fail_when_writing_on_writer_that_returns_forbidden_on_read_only_database()

    def test_should_fail_when_writing_on_writer_that_returns_database_unavailable(self):
        super().test_should_fail_when_writing_on_writer_that_returns_database_unavailable()

    def test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code(self):
        super().test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code()

    def test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code_using_tx_run(self):
        super().test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code_using_tx_run()

    def test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code_using_tx_run(self):
        super().test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code_using_tx_run()

    def test_should_use_write_session_mode_and_initial_bookmark_when_writing_using_tx_run(self):
        super().test_should_use_write_session_mode_and_initial_bookmark_when_writing_using_tx_run()

    def test_should_use_read_session_mode_and_initial_bookmark_when_reading_using_tx_run(self):
        super().test_should_use_read_session_mode_and_initial_bookmark_when_reading_using_tx_run()

    def test_should_pass_bookmark_from_tx_to_tx_using_tx_run(self):
        super().test_should_pass_bookmark_from_tx_to_tx_using_tx_run()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_read_tx_until_success_on_error(self):
        super().test_should_retry_read_tx_until_success_on_error()

    def test_should_retry_read_tx_until_success_on_run_error(self):
        super().test_should_retry_read_tx_until_success_on_run_error()

    def test_should_retry_read_tx_until_success_on_pull_error(self):
        super().test_should_retry_read_tx_until_success_on_pull_error()

    def test_should_retry_read_tx_until_success_on_no_connection(self):
        super().test_should_retry_read_tx_until_success_on_no_connection()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_write_tx_until_success_on_error(self):
        super().test_should_retry_write_tx_until_success_on_error()

    def test_should_retry_write_tx_until_success_on_run_error(self):
        super().test_should_retry_write_tx_until_success_on_run_error()

    def test_should_retry_write_tx_until_success_on_pull_error(self):
        super().test_should_retry_write_tx_until_success_on_pull_error()

    def test_should_retry_write_tx_until_success_on_no_connection(self):
        super().test_should_retry_write_tx_until_success_on_no_connection()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_read_tx_and_rediscovery_until_success(self):
        super().test_should_retry_read_tx_and_rediscovery_until_success()

    def test_should_retry_read_tx_and_rediscovery_until_success_on_run_failure(
            self):
        super().test_should_retry_read_tx_and_rediscovery_until_success_on_run_failure()

    def test_should_retry_read_tx_and_rediscovery_until_success_on_pull_failure(
            self):
        super().test_should_retry_read_tx_and_rediscovery_until_success_on_pull_failure()

    @driver_feature(types.Feature.OPT_PULL_PIPELINING)
    def test_should_retry_write_tx_and_rediscovery_until_success(self):
        super().test_should_retry_write_tx_and_rediscovery_until_success()

    def test_should_retry_write_tx_and_rediscovery_until_success_on_run_failure(
            self):
        super().test_should_retry_write_tx_and_rediscovery_until_success_on_run_failure()

    def test_should_retry_write_tx_and_rediscovery_until_success_on_pull_failure(
            self):
        super().test_should_retry_write_tx_and_rediscovery_until_success_on_pull_failure()

    def test_should_use_initial_router_for_discovery_when_others_unavailable(
            self):
        super().test_should_use_initial_router_for_discovery_when_others_unavailable()

    def test_should_successfully_read_from_readable_router_using_tx_function(self):
        super().test_should_successfully_read_from_readable_router_using_tx_function()

    def test_should_send_empty_hello(self):
        super().test_should_send_empty_hello()

    def test_should_serve_reads_and_fail_writes_when_no_writers_available(self):
        super().test_should_serve_reads_and_fail_writes_when_no_writers_available()

    def test_should_accept_routing_table_without_writers_and_then_rediscover(self):
        super().test_should_accept_routing_table_without_writers_and_then_rediscover()

    def test_should_fail_on_routing_table_with_no_reader(self):
        super().test_should_fail_on_routing_table_with_no_reader()

    def test_should_accept_routing_table_with_single_router(self):
        super().test_should_accept_routing_table_with_single_router()

    def test_should_successfully_send_multiple_bookmarks(self):
        super().test_should_successfully_send_multiple_bookmarks()

    def test_should_forget_address_on_database_unavailable_error(self):
        super().test_should_forget_address_on_database_unavailable_error()

    def test_should_use_resolver_during_rediscovery_when_existing_routers_fail(self):
        super().test_should_use_resolver_during_rediscovery_when_existing_routers_fail()

    def test_should_revert_to_initial_router_if_known_router_throws_protocol_errors(self):
        super().test_should_revert_to_initial_router_if_known_router_throws_protocol_errors()

    def test_should_successfully_check_if_support_for_multi_db_is_available(self):
        super().test_should_successfully_check_if_support_for_multi_db_is_available()

    def test_should_read_successfully_on_empty_discovery_result_using_session_run(self):
        super().test_should_read_successfully_on_empty_discovery_result_using_session_run()

    def test_should_fail_with_routing_failure_on_db_not_found_discovery_failure(self):
        super().test_should_fail_with_routing_failure_on_db_not_found_discovery_failure()

    def test_should_fail_with_routing_failure_on_invalid_bookmark_discovery_failure(self):
        super().test_should_fail_with_routing_failure_on_invalid_bookmark_discovery_failure()

    def test_should_fail_with_routing_failure_on_invalid_bookmark_mixture_discovery_failure(self):
        super().test_should_fail_with_routing_failure_on_invalid_bookmark_mixture_discovery_failure()

    def test_should_fail_with_routing_failure_on_forbidden_discovery_failure(self):
        super().test_should_fail_with_routing_failure_on_forbidden_discovery_failure()

    def test_should_fail_with_routing_failure_on_any_security_discovery_failure(self):
        super().test_should_fail_with_routing_failure_on_any_security_discovery_failure()

    def test_should_read_successfully_from_reachable_db_after_trying_unreachable_db(self):
        super().test_should_read_successfully_from_reachable_db_after_trying_unreachable_db()

    def test_should_ignore_system_bookmark_when_getting_rt_for_multi_db(self):
        super().test_should_ignore_system_bookmark_when_getting_rt_for_multi_db()

    def test_should_request_rt_from_all_initial_routers_until_successful_on_unknown_failure(self):
        super().test_should_request_rt_from_all_initial_routers_until_successful_on_unknown_failure()

    def test_should_request_rt_from_all_initial_routers_until_successful_on_authorization_expired(self):
        super().test_should_request_rt_from_all_initial_routers_until_successful_on_authorization_expired()

    def test_should_successfully_acquire_rt_when_router_ip_changes(self):
        super().test_should_successfully_acquire_rt_when_router_ip_changes()

    def test_should_successfully_get_server_protocol_version(self):
        super().test_should_successfully_get_server_protocol_version()

    def test_should_successfully_get_server_agent(self):
        super().test_should_successfully_get_server_agent()

    def test_should_fail_when_driver_closed_using_session_run(self):
        super().test_should_fail_when_driver_closed_using_session_run()
