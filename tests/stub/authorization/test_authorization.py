import inspect
import os

from nutkit.frontend import Driver
import nutkit.protocol as types
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
        if driver in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.AuthorizationExpiredException",
                error.errorType)
        elif driver in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.TransientError'>", error.errorType
            )
        elif driver in ["javascript"]:
            # only test for code
            pass
        elif driver in ["dotnet"]:
            self.assertEqual("AuthorizationExpired", error.errorType)
        elif driver in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::AuthorizationExpiredException",
                error.errorType)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def assert_is_token_error(self, error):
        driver = get_driver_name()
        self.assertEqual("Neo.ClientError.Security.TokenExpired",
                         error.code)
        if driver in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.TokenExpired'>", error.errorType
            )
        elif driver in ["go", "javascript"]:
            self.assertEqual("Neo.ClientError.Security.TokenExpired",
                             error.code)
            self.assertIn(
                "Token expired", error.msg
            )
        elif driver == "java":
            self.assertEqual(
                "org.neo4j.driver.exceptions.TokenExpiredException",
                error.errorType)
            self.assertEqual("Neo.ClientError.Security.TokenExpired",
                             error.code)
            self.assertIn("Token expired", error.msg)
        elif driver == "ruby":
            self.assertEqual(
                "Neo4j::Driver::Exceptions::TokenExpiredException",
                error.errorType)
            self.assertEqual("Neo.ClientError.Security.TokenExpired",
                             error.code)
            self.assertIn("Token expired", error.msg)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def start_server(self, server, script_fn, vars_=None):
        if vars_ is None:
            vars_ = self.get_vars()
        classes = (self.__class__, *inspect.getmro(self.__class__))
        tried_locations = []
        for cls in classes:
            if hasattr(cls, "get_vars") and callable(cls.get_vars):
                try:
                    cls_vars = cls.get_vars(self)
                except NotImplementedError:
                    pass
                if "#VERSION#" in cls_vars:
                    version_folder = \
                        "v{}".format(cls_vars["#VERSION#"].replace(".", "x"))
                    script_path = self.script_path(version_folder, script_fn)
                    tried_locations.append(script_path)
                    if os.path.exists(script_path):
                        server.start(path=script_path, vars_=vars_)
                        return
        raise FileNotFoundError("{!r} tried {!r}".format(
            script_fn, ", ".join(tried_locations)
        ))

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
            "basic", principal="p", credentials="c")
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
        result = session.run("RETURN 1 as n")
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
        with self.assertRaises(types.DriverError) as exc:
            tx = session.begin_transaction()
            # TODO: remove block when all drivers behave the same way
            if get_driver_name() in ["javascript", "go"]:
                tx.run("cypher").next()
        error_assertion(exc.exception)
        if get_driver_name() in ["go"]:
            with self.assertRaises(types.DriverError):
                # session will throw upon closure if there is a pending tx
                # tx will throw the last seen error upon closure
                tx.close()
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
            result = tx.run("RETURN 1 as n")
            # TODO remove consume() once all drivers report the error on run
            if get_driver_name() in ["javascript", "dotnet"]:
                result.consume()

        error_assertion(exc.exception)
        if get_driver_name() in ["go"]:
            # session will throw upon closure if there is a pending tx
            # tx will throw the last seen error upon closure
            with self.assertRaises(types.DriverError):
                tx.close()
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
            result = tx.run("RETURN 1 as n")
            result.next()
        error_assertion(exc.exception)
        if get_driver_name() in ["go"]:
            # session will throw upon closure if there is a pending tx
            # tx will throw the last seen error upon closure
            with self.assertRaises(types.DriverError):
                tx.close()
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
            vars_=vars_)

        session = driver.session("r", database=self.get_db())
        tx = session.begin_transaction()
        tx.run("RETURN 1 as n")
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
            vars_=vars_)

        session = driver.session("r", database=self.get_db())
        tx = session.begin_transaction()
        tx.run("RETURN 1 as n")
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
            result = tx.run("RETURN 1 as n")
            sequences.append(list(result))

        session.read_transaction(work)
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
            result = tx.run("RETURN 1 as n")
            sequences.append(list(result))

        with self.assertRaises(types.DriverError) as exc:
            session.read_transaction(work)
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
            result = tx.run("RETURN 1 as n")
            sequences.append(list(result))

        session.read_transaction(work, hooks={
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
            result = tx.run("RETURN 1 as n")
            sequences.append(list(result))

        with self.assertRaises(types.DriverError) as exc:
            session.read_transaction(work, hooks={
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
            result = tx.run("RETURN 1 as n")
            sequences.append(list(result))

        session.read_transaction(work, hooks={
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
            result = tx.run("RETURN 1 as n")
            sequences.append(list(result))

        with self.assertRaises(types.DriverError) as exc:
            session.read_transaction(work, hooks={
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
            result = tx.run("RETURN 1 as n")
            sequences.append(list(result))

        session.read_transaction(work, hooks={
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
            result = tx.run("RETURN 1 as n")
            sequences.append(list(result))

        with self.assertRaises(types.DriverError) as exc:
            session.read_transaction(work, hooks={
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

    required_features = types.Feature.BOLT_4_0,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._uri = "bolt://%s:%d" % (self._server.host,
                                      self._server.port)
        self._auth = types.AuthorizationToken(
            "basic", principal="p", credentials="c")
        self._userAgent = "007"

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def get_vars(self):
        return {
            "#VERSION#": "4.0"
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
    def test_should_be_able_to_use_current_sessions_after_AuthorizationExpired(  # noqa: N802,E501
            self):
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
    def test_should_be_able_to_use_current_tx_after_AuthorizationExpired(  # noqa: N802,E501
            self):
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


class TestAuthenticationSchemes(AuthorizationBase):

    required_features = types.Feature.BOLT_4_3,

    def get_vars(self):
        return {
            "#VERSION#": "4.3"
        }

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
            implicit_defaults = self.driver_supports_features(
                types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS
            )
            if realm == "foobar":
                script_fn = "scheme_basic_realm_foobar%s.script"
            else:
                script_fn = "scheme_basic%s.script"
            script_fn = script_fn % ("_minimal" if implicit_defaults else "")
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

        for realm in (None, "", "foobar"):
            with self.subTest("realm-%s" % realm):
                test()
            self._server.reset()

    @driver_feature(types.Feature.AUTH_BEARER)
    def test_bearer_scheme(self):
        implicit_defaults = self.driver_supports_features(
            types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS
        )
        script_fn = "scheme_bearer%s.script"
        script_fn = script_fn % ("_minimal" if implicit_defaults else "")
        self.start_server(self._server, script_fn)

        auth = types.AuthorizationToken("bearer", credentials="QmFuYW5hIQ==")
        driver = Driver(self._backend, self._uri, auth)
        session = driver.session("r")
        list(session.run("RETURN 1 AS n"))
        session.close()
        driver.close()
        self._server.done()

    @driver_feature(types.Feature.AUTH_CUSTOM)
    def test_custom_scheme(self):
        implicit_defaults = self.driver_supports_features(
            types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS
        )
        script_fn = "scheme_custom%s.script"
        script_fn = script_fn % ("_minimal" if implicit_defaults else "")
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

    @driver_feature(types.Feature.AUTH_CUSTOM)
    def test_custom_scheme_empty(self):
        implicit_defaults = self.driver_supports_features(
            types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS
        )
        script_fn = "scheme_custom_empty%s.script"
        script_fn = script_fn % ("_minimal" if implicit_defaults else "")
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

    @driver_feature(types.Feature.AUTH_KERBEROS)
    def test_kerberos_scheme(self):
        implicit_defaults = self.driver_supports_features(
            types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS
        )
        script_fn = "scheme_kerberos%s.script"
        script_fn = script_fn % ("_minimal" if implicit_defaults else "")
        self.start_server(self._server, script_fn)

        auth = types.AuthorizationToken("kerberos", credentials="QmFuYW5hIQ==")
        driver = Driver(self._backend, self._uri, auth)
        session = driver.session("r")
        list(session.run("RETURN 1 AS n"))
        session.close()
        driver.close()
        self._server.done()
