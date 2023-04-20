import inspect
import os

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class AuthorizationBase(TestkitTestCase):
    # While there is no unified language agnostic error type mapping, a
    # dedicated driver mapping is required to determine if the expected
    # error is returned.
    def assert_is_authorization_error(self, error):
        driver = get_driver_name()
        self.assertEqual("Neo.ClientError.Security.AuthorizationExpired",
                         error.code)
        expected_type = None
        if driver in ["java"]:
            expected_type = \
                "org.neo4j.driver.exceptions.AuthorizationExpiredException"
        elif driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.TransientError'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["dotnet"]:
            expected_type = "AuthorizationExpired"
        elif driver in ["go"]:
            expected_type = "Neo4jError"
        elif driver in ["ruby"]:
            expected_type = \
                "Neo4j::Driver::Exceptions::AuthorizationExpiredException"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)

    def assert_is_token_error(self, error):
        driver = get_driver_name()
        self.assertEqual("Neo.ClientError.Security.TokenExpired", error.code)
        self.assertIn("Token expired", error.msg)

        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.TokenExpired'>"
        elif driver in ["go"]:
            expected_type = "TokenExpiredError"
        elif driver in ["javascript"]:
            pass
        elif driver == "java":
            expected_type = "org.neo4j.driver.exceptions.TokenExpiredException"
        elif driver == "ruby":
            expected_type = "Neo4j::Driver::Exceptions::TokenExpiredException"
        elif driver == "dotnet":
            expected_type = "ClientError"
        elif driver == "go":
            expected_type = "TokenExpiredError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)

    def assert_is_retryable_token_error(self, error):
        driver = get_driver_name()
        self.assertEqual("Neo.ClientError.Security.TokenExpired", error.code)
        self.assertIn("Token expired", error.msg)

        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.TokenExpiredRetryable'>"
        elif driver in ["go", "javascript"]:
            pass  # code and msg check are enough
        elif driver in ["dotnet"]:
            expected_type = "ClientError"
        elif driver == "java":
            expected_type = \
                "org.neo4j.driver.exceptions.TokenExpiredRetryableException"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)

    def assert_re_auth_unsupported_error(self, error):
        self.assertIsInstance(error, types.DriverError)
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.ConfigurationError'>",
                error.errorType
            )
            self.assertIn(
                "user switching is not supported for bolt protocol "
                "version(5, 0)",
                error.msg.lower()
            )
        elif driver in ["javascript"]:
            self.assertEqual(
                "N/A",
                error.code
            )
            self.assertEqual(
                "Driver is connected to a database that does not support "
                "user switch.",
                error.msg
            )
        elif driver in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.UnsupportedFeatureException",
                error.errorType
            )
        elif driver in ["go"]:
            self.assertEqual("feature not supported", error.errorType)
            self.assertIn("session auth", error.msg)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def _find_version_script(self, script_fns):
        if isinstance(script_fns, str):
            script_fns = [script_fns]
        classes = inspect.getmro(self.__class__)
        tried_locations = []
        for cls in classes:
            cls_vars = None
            if hasattr(cls, "get_vars") and callable(cls.get_vars):
                try:
                    cls_vars = cls.get_vars(self)
                except NotImplementedError:
                    pass
            if not cls_vars or "#VERSION#" not in cls_vars:
                continue
            version_folder = "v{}".format(
                cls_vars["#VERSION#"].replace(".", "x")
            )
            for script_fn in script_fns:
                script_path = self.script_path(version_folder,
                                               script_fn)
                tried_locations.append(script_path)
                if os.path.exists(script_path):
                    return script_path
        raise FileNotFoundError("{!r} tried {!r}".format(
            script_fns, ", ".join(tried_locations)
        ))

    def start_server(self, server, script_fn, vars_=None):
        if vars_ is None:
            vars_ = self.get_vars()
        script_path = self._find_version_script(script_fn)
        server.start(path=script_path, vars_=vars_)

    def script_fn_with_features(self, script_fn):
        has_logon = getattr(self, "has_logon", False)
        minimal = self.driver_supports_features(
            types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS
        )
        auth_pipeline = self.driver_supports_features(
            types.Feature.OPT_AUTH_PIPELINING
        )
        parts = script_fn.rsplit(".", 1)
        if minimal and not has_logon:
            return (
                f"{parts[0]}_pipelined_minimal.{parts[1]}",
                # pipelined is optional, as it makes little sense to have
                # an extra script for it for protocol versions pre
                # LOGOFF/LOGON message (there is nothing to pipeline
                # there).
                f"{parts[0]}_minimal.{parts[1]}",
            )
        elif minimal and auth_pipeline:
            return f"{parts[0]}_pipelined_minimal.{parts[1]}",
        elif auth_pipeline:
            return (
                f"{parts[0]}_pipelined.{parts[1]}",
                f"{parts[0]}.{parts[1]}",
            )
        elif minimal:
            raise RuntimeError(
                "Tests for driver with "
                "types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS but without "
                "types.Feature.OPT_AUTH_PIPELINING are (currently) missing "
                "when logon is supported. "
                "Feel free to add them when needed."
            )
        else:
            return script_fn

    def get_vars(self):
        raise NotImplementedError

    _AUTH_EXPIRED = (
        '{"code": "Neo.ClientError.Security.AuthorizationExpired",'
        ' "message": "Authorization expired"}'
    )
    _TOKEN_EXPIRED = ('{"code": "Neo.ClientError.Security.TokenExpired", '
                      '"message": "Token expired"}')


# TODO: find a way to test that driver ditches all open connection in the pool
#       when encountering Neo.ClientError.Security.AuthorizationExpired
# TODO: re-write tests, where possible, to use only one server, utilizing
#       on_send_RetryableNegative and potentially other hooks.
class TestAuthorizationV4x3(AuthorizationBase):

    required_features = types.Feature.BOLT_4_3,

    def setUp(self):
        super().setUp()
        self._routing_server1 = StubServer(9000)
        self._read_server1 = StubServer(9010)
        self._read_server2 = StubServer(9011)
        self._uri = "neo4j://%s:%d" % (self._routing_server1.host,
                                       self._routing_server1.port)
        self._auth = types.AuthorizationToken(
            "basic", principal="p", credentials="c"
        )
        self._userAgent = "007"

    def tearDown(self):
        self._routing_server1.reset()
        self._read_server1.reset()
        self._read_server2.reset()
        super().tearDown()

    def get_vars(self, host=None):
        if host is None:
            host = self._routing_server1.host
        return {
            "#VERSION#": "4.3",
            "#HOST#": host,
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }

    def get_db(self):
        return "adb"

    def switch_unused_servers(self, servers, new_script_path):
        contact_count = []
        for server in servers:
            contact_count.append(server.count_responses("<ACCEPT>"))
            if contact_count[-1] not in (0, 1):
                return
        for count, server in zip(contact_count, servers):
            if count == 0:
                server.reset()
                self.start_server(server, new_script_path)

    def _fail_on_pull_using_session_run(self, error, error_assertion):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = error
        self.start_server(self._read_server1,
                          "reader_yielding_error_on_pull.script", vars_=vars_)

        session = driver.session("r", database=self.get_db())
        result = session.run("RETURN 1 AS n")
        with self.assertRaises(types.DriverError) as exc:
            result.next()
        error_assertion(exc.exception)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_fail_with_auth_expired_on_pull_using_session_run(self):
        self._fail_on_pull_using_session_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_with_token_expired_on_pull_using_session_run(self):
        self._fail_on_pull_using_session_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    def _fail_on_begin_using_tx_run(self, error, error_assertion):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = error
        self.start_server(self._read_server1,
                          "reader_tx_yielding_error_on_begin.script",
                          vars_=vars_)

        session = driver.session("r", database=self.get_db())

        if not self.driver_supports_features(types.Feature.OPT_EAGER_TX_BEGIN):
            tx = session.begin_transaction()
            with self.assertRaises(types.DriverError) as exc:
                tx.run("cypher").next()
        else:
            # this is what all drivers should do
            with self.assertRaises(types.DriverError) as exc:
                session.begin_transaction()
        error_assertion(exc.exception)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_fail_with_auth_expired_on_begin_using_tx_run(self):
        if get_driver_name() in ["javascript"]:
            self.skipTest("Fails on sending RESET after auth-error and "
                          "surfaces SessionExpired instead.")
        self._fail_on_begin_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_with_token_expired_on_begin_using_tx_run(self):
        self._fail_on_begin_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    def _fail_on_run_using_tx_run(self, error, error_assertion):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = error
        self.start_server(
            self._read_server1, "reader_tx_yielding_error_on_run.script",
            vars_=vars_
        )

        session = driver.session("r", database=self.get_db())
        tx = session.begin_transaction()
        with self.assertRaises(types.DriverError) as exc:
            result = tx.run("RETURN 1 AS n")
            # TODO remove consume() once all drivers report the error on run
            if get_driver_name() in ["javascript", "dotnet"]:
                result.consume()

        error_assertion(exc.exception)
        tx.rollback()
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_fail_with_auth_expired_on_run_using_tx_run(self):
        self._fail_on_run_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_with_token_expired_on_run_using_tx_run(self):
        self._fail_on_run_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    def _fail_on_pull_using_tx_run(self, error, error_assertion):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = error
        self.start_server(self._read_server1,
                          "reader_tx_yielding_error_on_pull.script",
                          vars_=vars_)

        session = driver.session("r", database=self.get_db())
        tx = session.begin_transaction()
        with self.assertRaises(types.DriverError) as exc:
            result = tx.run("RETURN 1 AS n")
            result.next()
        error_assertion(exc.exception)
        tx.rollback()
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_fail_with_auth_expired_on_pull_using_tx_run(self):
        self._fail_on_pull_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_with_token_expired_on_pull_using_tx_run(self):
        self._fail_on_pull_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    def _fail_on_commit_using_tx_run(self, error, error_assertion):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = error
        self.start_server(
            self._read_server1,
            "reader_tx_yielding_error_on_commit_with_pull_or_discard.script",
            vars_=vars_
        )

        session = driver.session("r", database=self.get_db())
        tx = session.begin_transaction()
        tx.run("RETURN 1 AS n")
        with self.assertRaises(types.DriverError) as exc:
            tx.commit()
        error_assertion(exc.exception)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_fail_with_auth_expired_on_commit_using_tx_run(self):
        self._fail_on_commit_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_with_token_expired_on_commit_using_tx_run(self):
        self._fail_on_commit_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def _fail_on_rollback_using_tx_run(self, error, error_assertion):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = error
        self.start_server(
            self._read_server1,
            "reader_tx_yielding_error_on_rollback_with_pull_or_discard.script",
            vars_=vars_
        )

        session = driver.session("r", database=self.get_db())
        tx = session.begin_transaction()
        tx.run("RETURN 1 AS n")
        with self.assertRaises(types.DriverError) as exc:
            tx.rollback()
        error_assertion(exc.exception)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_fail_with_auth_expired_on_rollback_using_tx_run(self):
        self._fail_on_rollback_using_tx_run(
            self._AUTH_EXPIRED, self.assert_is_authorization_error
        )

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_with_token_expired_on_rollback_using_tx_run(self):
        self._fail_on_rollback_using_tx_run(
            self._TOKEN_EXPIRED, self.assert_is_token_error
        )

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_retry_on_auth_expired_on_begin_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        # FIXME: test assumes that the driver contacts read_server1 first

        # Note: swapping scripts with hooks is not possible because some
        # drivers (e.g., Java) don't call the transaction function if they
        # can't run a successful BEGIN first.
        vars_ = self.get_vars()
        vars_["#ERROR#"] = self._AUTH_EXPIRED
        self.start_server(self._read_server1,
                          "reader_tx_yielding_error_on_begin.script",
                          vars_=vars_)
        self.start_server(self._read_server2, "reader_tx.script")

        session = driver.session("r", database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 AS n")
            sequences.append(list(result))

        session.execute_read(work)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()
        self._read_server2.done()
        # TODO: Some drivers check the result of BEGIN before calling the
        #       transaction function, others don't
        self.assertIn(attempt_count, {1, 2})
        self.assertEqual([[types.Record([types.CypherInt(1)])]], sequences)

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_on_token_expired_on_begin_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        # FIXME: test assumes that the driver contacts read_server1 first

        # Note: swapping scripts with hooks is not possible because some
        # drivers (e.g., Java) don't call the transaction function if they
        # can't run a successful BEGIN first.
        vars_ = self.get_vars()
        vars_["#ERROR#"] = self._TOKEN_EXPIRED
        self.start_server(self._read_server1,
                          "reader_tx_yielding_error_on_begin.script",
                          vars_=vars_)
        self.start_server(self._read_server2, "reader_tx.script")

        session = driver.session("r", database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 AS n")
            sequences.append(list(result))

        with self.assertRaises(types.DriverError) as exc:
            session.execute_read(work)
        self.assert_is_token_error(exc.exception)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()
        self._read_server2.reset()
        self.assertEqual(self._read_server2.count_responses("<ACCEPT>"), 0)
        # TODO: Some drivers check the result of BEGIN before calling the
        #       transaction function, others don't
        self.assertIn(attempt_count, {0, 1})
        self.assertEqual([], sequences)

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_retry_on_auth_expired_on_run_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        vars_ = self.get_vars()
        vars_["#ERROR#"] = self._AUTH_EXPIRED
        self.start_server(self._routing_server1, "router.script")
        self.start_server(
            self._read_server1, "reader_tx_yielding_error_on_run.script",
            vars_=vars_
        )
        self.start_server(
            self._read_server2, "reader_tx_yielding_error_on_run.script",
            vars_=vars_
        )

        session = driver.session("r", database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 AS n")
            sequences.append(list(result))

        session.execute_read(work, hooks={
            "on_send_RetryableNegative": lambda _: self.switch_unused_servers(
                (self._read_server1, self._read_server2), "reader_tx.script"
            )
        })
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()
        self._read_server2.done()
        self.assertEqual(2, attempt_count)
        self.assertEqual([[types.Record([types.CypherInt(1)])]], sequences)

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_on_token_expired_on_run_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        vars_ = self.get_vars()
        vars_["#ERROR#"] = self._TOKEN_EXPIRED
        self.start_server(self._routing_server1, "router.script")
        self.start_server(
            self._read_server1, "reader_tx_yielding_error_on_run.script",
            vars_=vars_
        )
        self.start_server(
            self._read_server2, "reader_tx_yielding_error_on_run.script",
            vars_=vars_
        )

        session = driver.session("r", database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 AS n")
            sequences.append(list(result))

        with self.assertRaises(types.DriverError) as exc:
            session.execute_read(work, hooks={
                "on_send_RetryableNegative": lambda _:
                self.switch_unused_servers(
                    (self._read_server1, self._read_server2),
                    "reader_tx.script"
                )
            })
        self.assert_is_token_error(exc.exception)
        session.close()
        driver.close()

        self._routing_server1.done()
        reader1_connections = self._read_server1.count_responses("<ACCEPT>")
        reader2_connections = self._read_server2.count_responses("<ACCEPT>")
        if reader1_connections == 1:
            self._read_server1.done()
            self._read_server2.reset()
        elif reader2_connections == 1:
            self._read_server2.done()
            self._read_server1.reset()
        if reader1_connections + reader2_connections != 1:
            self.fail("Not exactly 1 read attempt. Reader 1: %i + Reader 2: %i"
                      % (reader1_connections, reader2_connections))
        self.assertEqual(1, attempt_count)
        self.assertEqual([], sequences)

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_retry_on_auth_expired_on_pull_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = self._AUTH_EXPIRED
        self.start_server(self._read_server1,
                          "reader_tx_yielding_error_on_pull.script",
                          vars_=vars_)
        self.start_server(self._read_server2,
                          "reader_tx_yielding_error_on_pull.script",
                          vars_=vars_)

        session = driver.session("r", database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 AS n")
            sequences.append(list(result))

        session.execute_read(work, hooks={
            "on_send_RetryableNegative": lambda _: self.switch_unused_servers(
                (self._read_server1, self._read_server2), "reader_tx.script"
            )
        })
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()
        self._read_server2.done()
        self.assertEqual(2, attempt_count)
        self.assertEqual([[types.Record([types.CypherInt(1)])]], sequences)

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_on_token_expired_on_pull_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = self._TOKEN_EXPIRED
        self.start_server(self._read_server1,
                          "reader_tx_yielding_error_on_pull.script",
                          vars_=vars_)
        self.start_server(self._read_server2,
                          "reader_tx_yielding_error_on_pull.script",
                          vars_=vars_)

        session = driver.session("r", database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 AS n")
            sequences.append(list(result))

        with self.assertRaises(types.DriverError) as exc:
            session.execute_read(work, hooks={
                "on_send_RetryableNegative": lambda _:
                self.switch_unused_servers(
                    (self._read_server1, self._read_server2),
                    "reader_tx.script"
                )
            })
        self.assert_is_token_error(exc.exception)
        session.close()
        driver.close()

        self._routing_server1.done()
        reader1_connections = self._read_server1.count_responses("<ACCEPT>")
        reader2_connections = self._read_server2.count_responses("<ACCEPT>")
        if reader1_connections == 1:
            self._read_server1.done()
            self._read_server2.reset()
        elif reader2_connections == 1:
            self._read_server2.done()
            self._read_server1.reset()
        if reader1_connections + reader2_connections != 1:
            self.fail("Not exactly 1 read attempt. Reader 1: %i + Reader 2: %i"
                      % (reader1_connections, reader2_connections))
        self.assertEqual(1, attempt_count)
        self.assertEqual([], sequences)

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_retry_on_auth_expired_on_commit_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = self._AUTH_EXPIRED
        self.start_server(self._read_server1,
                          "reader_tx_yielding_error_on_commit.script",
                          vars_=vars_)
        self.start_server(self._read_server2,
                          "reader_tx_yielding_error_on_commit.script",
                          vars_=vars_)

        session = driver.session("r", database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 AS n")
            sequences.append(list(result))

        session.execute_read(work, hooks={
            "on_send_RetryablePositive": lambda _: self.switch_unused_servers(
                (self._read_server1, self._read_server2), "reader_tx.script"
            )
        })
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()
        self._read_server2.done()
        self.assertEqual(2, attempt_count)
        self.assertEqual([[types.Record([types.CypherInt(1)])],
                          [types.Record([types.CypherInt(1)])]], sequences)

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_should_fail_on_token_expired_on_commit_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)
        self.start_server(self._routing_server1, "router.script")
        vars_ = self.get_vars()
        vars_["#ERROR#"] = self._TOKEN_EXPIRED
        self.start_server(self._read_server1,
                          "reader_tx_yielding_error_on_commit.script",
                          vars_=vars_)
        self.start_server(self._read_server2,
                          "reader_tx_yielding_error_on_commit.script",
                          vars_=vars_)

        session = driver.session("r", database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 AS n")
            sequences.append(list(result))

        with self.assertRaises(types.DriverError) as exc:
            session.execute_read(work, hooks={
                "on_send_RetryableNegative": lambda _:
                self.switch_unused_servers(
                    (self._read_server1, self._read_server2),
                    "reader_tx.script"
                )
            })
        self.assert_is_token_error(exc.exception)
        session.close()
        driver.close()

        self._routing_server1.done()
        reader1_connections = self._read_server1.count_responses("<ACCEPT>")
        reader2_connections = self._read_server2.count_responses("<ACCEPT>")
        if reader1_connections == 1:
            self._read_server1.done()
            self._read_server2.reset()
        elif reader2_connections == 1:
            self._read_server2.done()
            self._read_server1.reset()
        if reader1_connections + reader2_connections != 1:
            self.fail("Not exactly 1 read attempt. Reader 1: %i + Reader 2: %i"
                      % (reader1_connections, reader2_connections))
        self.assertEqual(1, attempt_count)
        self.assertEqual([[types.Record([types.CypherInt(1)])]], sequences)


class TestAuthorizationV4x1(TestAuthorizationV4x3):
    required_features = types.Feature.BOLT_4_1,

    def get_vars(self, host=None):
        if host is None:
            host = self._routing_server1.host
        return {
            "#VERSION#": "4.1",
            "#HOST#": host,
            "#ROUTINGMODE#": '"mode": "r", ',
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }


class TestAuthorizationV5x0(TestAuthorizationV4x3):

    required_features = types.Feature.BOLT_5_0,

    def get_vars(self, host=None):
        if host is None:
            host = self._routing_server1.host
        return {
            "#VERSION#": "5.0",
            "#HOST#": host,
            "#ROUTINGMODE#": '"mode": "r", ',
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }


class TestAuthorizationV5x1(TestAuthorizationV4x3):

    required_features = types.Feature.BOLT_5_1,

    def get_vars(self, host=None):
        if host is None:
            host = self._routing_server1.host
        return {
            "#VERSION#": "5.1",
            "#HOST#": host,
            "#ROUTINGMODE#": '"mode": "r", ',
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }


class TestAuthorizationV3(TestAuthorizationV4x3):
    required_features = types.Feature.BOLT_3_0,

    def get_vars(self, host=None):
        if host is None:
            host = self._routing_server1.host
        return {
            "#VERSION#": "3",
            "#HOST#": host,
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }

    def get_db(self):
        return None


class TestNoRoutingAuthorization(AuthorizationBase):

    required_features = types.Feature.BOLT_4_4,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._uri = "bolt://%s:%d" % (self._server.host,
                                      self._server.port)
        self._auth = types.AuthorizationToken(
            "basic", principal="p", credentials="c"
        )
        self._userAgent = "007"

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def get_vars(self):
        return {
            "#VERSION#": "4.4"
        }

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_drop_connection_after_AuthorizationExpired(self):  # noqa: N802,E501
        self.start_server(
            self._server,
            "reader_return_1_failure_return_2_3_4_and_5_succeed.script"
        )
        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)

        session1 = driver.session("r", fetch_size=1)
        session2 = driver.session("r")

        session1.run("INFINITE RECORDS UNTIL DISCARD").next()

        with self.assertRaises(types.DriverError) as exc:
            session2.run("AuthorizationExpired").next()
        self.assert_is_authorization_error(exc.exception)

        session2.close()
        session1.close()

        accept_count = self._server.count_responses("<ACCEPT>")

        # fetching another connection and run a query to force
        # drivers which lazily close the connection do it
        session3 = driver.session("r")
        session3.run("ONE RECORD").next()
        session3.close()

        hangup_count = self._server.count_responses("<HANGUP>")

        self.assertEqual(accept_count, hangup_count)
        self.assertGreaterEqual(accept_count, 2)

        driver.close()

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_be_able_to_use_current_sessions_after_AuthorizationExpired(self):  # noqa: N802,E501
        self.start_server(
            self._server,
            "reader_return_1_failure_return_2_3_4_and_5_succeed.script"
        )

        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)

        session1 = driver.session("r", fetch_size=1)
        session2 = driver.session("r")

        list(session1.run("ONE RECORD"))

        with self.assertRaises(types.DriverError) as exc:
            session2.run("AuthorizationExpired").next()
        self.assert_is_authorization_error(exc.exception)

        session2.close()

        session1.run("INFINITE RECORDS UNTIL DISCARD").next()
        session1.close()

    @driver_feature(types.Feature.OPT_AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_be_able_to_use_current_tx_after_AuthorizationExpired(self):  # noqa: N802,E501
        self.start_server(
            self._server,
            "reader_return_1_failure_return_2_3_4_and_5_succeed.script"
        )

        def allocate_connections(n):
            sessions = [driver.session("r") for _ in range(n)]
            txs = [s.begin_transaction() for s in sessions]
            [list(tx.run("TX RUN ONE RECORD")) for tx in txs]
            [tx.commit() for tx in txs]
            [s.close() for s in sessions]

        driver = Driver(self._backend, self._uri, self._auth,
                        user_agent=self._userAgent)

        session1 = driver.session("r", fetch_size=1)
        session2 = driver.session("r")

        tx1 = session1.begin_transaction()
        list(tx1.run("TX RUN 1/3 ONE RECORD"))

        with self.assertRaises(types.DriverError) as exc:
            session2.run("AuthorizationExpired").next()
        self.assert_is_authorization_error(exc.exception)

        list(tx1.run("TX RUN 2/3 ONE RECORD"))
        # running a query in a session to make sure drivers that close
        # connections lazily, will encounter session1's connection which after
        # the AuthorizationExpired is flagged to be removed. BUT: it's still
        # in use by session1, so is must survive.
        allocate_connections(1)

        session2.close()

        # same as above!
        # allocating increasing number of connections makes sure the driver has
        # to run through all existing connections and potentially clean them up
        # before eventually deciding a new connection must be created
        allocate_connections(2)

        list(tx1.run("TX RUN 3/3 ONE RECORD"))

        # now, when session1 has releases its connection, the driver should
        # remove the connection
        hangup_count_pre = self._server.count_responses("<HANGUP>")
        tx1.commit()
        session1.close()
        # fetching another connection and run a query to force
        # drivers which lazily close the connection do it
        allocate_connections(3)
        hangup_count_post = self._server.count_responses("<HANGUP>")

        self.assertEqual(hangup_count_pre + 1, hangup_count_post)


class TestAuthenticationSchemesV4x4(AuthorizationBase):

    required_features = types.Feature.BOLT_4_4,

    def get_vars(self):
        return {
            "#VERSION#": "4.4"
        }

    def post_script_assertions(self):
        pass

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._uri = "bolt://%s:%d" % (self._server.host,
                                      self._server.port)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def test_basic_scheme(self):
        def test():
            if realm == "foobar":
                script_fn = "scheme_basic_realm_foobar.script"
            else:
                script_fn = "scheme_basic.script"
            script_fn = self.script_fn_with_features(script_fn)
            self.start_server(self._server, script_fn)

            if realm:
                auth = types.AuthorizationToken(
                    "basic", principal="neo4j", credentials="pass", realm=realm
                )
            else:
                auth = types.AuthorizationToken("basic", principal="neo4j",
                                                credentials="pass")
            driver = Driver(self._backend, self._uri, auth)
            session = driver.session("r")
            list(session.run("RETURN 1 AS n"))
            session.close()
            driver.close()
            self._server.done()
            self.post_script_assertions()

        for realm in (None, "", "foobar"):
            with self.subTest(realm=realm):
                test()
            self._server.reset()

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_bearer_scheme(self):
        script_fn = "scheme_bearer.script"
        script_fn = self.script_fn_with_features(script_fn)
        self.start_server(self._server, script_fn)

        auth = types.AuthorizationToken("bearer", credentials="QmFuYW5hIQ==")
        driver = Driver(self._backend, self._uri, auth)
        session = driver.session("r")
        list(session.run("RETURN 1 AS n"))
        session.close()
        driver.close()
        self._server.done()
        self.post_script_assertions()

    @driver_feature(types.Feature.AUTH_CUSTOM)
    def test_custom_scheme(self):
        script_fn = "scheme_custom.script"
        script_fn = self.script_fn_with_features(script_fn)
        self.start_server(self._server, script_fn)

        auth = types.AuthorizationToken("wild-scheme",
                                        principal="I See Something",
                                        credentials="You Don't See!",
                                        realm="And it's blue.",
                                        parameters={
                                            "sky?": "no",
                                            "my eyes": 0.1,
                                            "da be dee da be daa?": True
                                        })
        driver = Driver(self._backend, self._uri, auth)
        session = driver.session("r")
        list(session.run("RETURN 1 AS n"))
        session.close()
        driver.close()
        self._server.done()
        self.post_script_assertions()

    @driver_feature(types.Feature.AUTH_CUSTOM)
    def test_custom_scheme_empty(self):
        script_fn = "scheme_custom_empty.script"
        script_fn = self.script_fn_with_features(script_fn)
        self.start_server(self._server, script_fn)

        auth = types.AuthorizationToken("minimal-scheme",
                                        principal="",
                                        credentials="",
                                        realm="",
                                        parameters={})
        driver = Driver(self._backend, self._uri, auth)
        session = driver.session("r")
        list(session.run("RETURN 1 AS n"))
        session.close()
        driver.close()
        self._server.done()
        self.post_script_assertions()

    @driver_feature(types.Feature.AUTH_KERBEROS)
    def test_kerberos_scheme(self):
        script_fn = "scheme_kerberos.script"
        script_fn = self.script_fn_with_features(script_fn)
        self.start_server(self._server, script_fn)

        auth = types.AuthorizationToken("kerberos", credentials="QmFuYW5hIQ==")
        driver = Driver(self._backend, self._uri, auth)
        session = driver.session("r")
        list(session.run("RETURN 1 AS n"))
        session.close()
        driver.close()
        self._server.done()
        self.post_script_assertions()


class TestAuthenticationSchemesV5x1(TestAuthenticationSchemesV4x4):

    required_features = types.Feature.BOLT_5_1,

    has_logon = True

    def get_vars(self):
        return {
            "#VERSION#": "5.1"
        }

    def post_script_assertions(self):
        # add OPT_MINIMAL_RESETS assertion (if driver claims to support it)
        if self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            self.assertEqual(self._server.count_requests("RESET"), 0)

        super().post_script_assertions()
