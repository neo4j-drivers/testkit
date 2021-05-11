from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
    driver_feature
)
from tests.stub.shared import StubServer


def get_extra_hello_props():
    if get_driver_name() in ["java"]:
        return ', "realm": ""'
    elif get_driver_name() in ["javascript"]:
        return ', "realm": "", "ticket": ""'
    return ""


class BaseAuthorizationTests(TestkitTestCase):
    # While there is no unified language agnostic error type mapping, a
    # dedicated driver mapping is required to determine if the expected
    # error is returned.
    def assert_is_authorization_error(self, error):
        driver = get_driver_name()
        self.assertEqual("Neo.ClientError.Security.AuthorizationExpired",
                         error.code)
        if driver in ['java']:
            self.assertEqual(
                'org.neo4j.driver.exceptions.AuthorizationExpiredException',
                error.errorType)
        elif driver in ['python']:
            self.assertEqual(
                "<class 'neo4j.exceptions.TransientError'>", error.errorType
            )
        elif driver in ['javascript']:
            # only test for code
            pass
        elif driver in ['dotnet']:
            self.assertEqual("AuthorizationExpired", error.errorType)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)


# TODO: find a way to test that driver ditches all open connection in the pool
#       when encountering Neo.ClientError.Security.AuthorizationExpired
# TODO: re-write tests, where possible, to use only one server, utilizing
#       on_send_RetryableNegative and potentially other hooks.
class AuthorizationTests(BaseAuthorizationTests):
    def setUp(self):
        super().setUp()
        self._routing_server1 = StubServer(9000)
        self._read_server1 = StubServer(9010)
        self._read_server2 = StubServer(9011)
        self._uri = "neo4j://%s:%d" % (self._routing_server1.host,
                                       self._routing_server1.port)
        self._auth = types.AuthorizationToken(
            scheme="basic", principal="p", credentials="c")
        self._userAgent = "007"

    def tearDown(self):
        self._routing_server1.reset()
        self._read_server1.reset()
        self._read_server2.reset()
        super().tearDown()

    def router_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: ROUTE #ROUTINGCTX# [] "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020"], "role":"WRITE"}]}}
        {?
            C: RESET
            S: SUCCESS {}
        ?}
        """

    def read_script_with_auth_expired_on_pull(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: RUN "RETURN 1 as n" {} {"mode": "r", "db": "adb"}
        S: SUCCESS {"fields": ["n"]}
        C: PULL {"n": 1000}
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
        {?
            C: RESET
            S: SUCCESS {}
        ?}
        """

    def read_tx_script_with_auth_expired_on_begin(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"mode": "r", "db": "adb"}
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script_with_auth_expired_on_run(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script_with_auth_expired_on_pull(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        S: SUCCESS {"fields": ["n"]}
        C: PULL {"n": 1000}
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script_with_auth_expired_on_commit(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script_with_auth_expired_on_rollback(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: ROLLBACK
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

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

    @staticmethod
    def collectRecords(result):
        sequence = []
        while True:
            next_ = result.next()
            if isinstance(next_, types.NullRecord):
                break
            sequence.append(next_.values[0].value)
        return sequence

    def switch_unused_servers(self, servers, new_script):
        contact_count = []
        for server in servers:
            contact_count.append(server.count_responses("<ACCEPT>"))
            if contact_count[-1] not in (0, 1):
                return
        for count, server in zip(contact_count, servers):
            if count == 0:
                server.reset()
                server.start(script=new_script,
                             vars=self.get_vars())

    def test_should_fail_with_auth_expired_on_pull_using_session_run(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        self._read_server1.start(
            script=self.read_script_with_auth_expired_on_pull(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        try:
            res = session.run("RETURN 1 as n")
            res.next()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    def test_should_fail_with_auth_expired_on_begin_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        self._read_server1.start(
            script=self.read_tx_script_with_auth_expired_on_begin(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        try:
            tx = session.beginTransaction()
            # some drivers don't send begin before attempting to utilize the
            # transaction (e.g., Python)
            tx.run("cypher")
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    def test_should_fail_with_auth_expired_on_run_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        self._read_server1.start(
            script=self.read_tx_script_with_auth_expired_on_run(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        try:
            tx.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    def test_should_fail_with_auth_expired_on_pull_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        self._read_server1.start(
            script=self.read_tx_script_with_auth_expired_on_pull(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        try:
            tx.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    def test_should_fail_with_auth_expired_on_commit_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        self._read_server1.start(
            script=self.read_tx_script_with_auth_expired_on_commit(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n").consume()
        try:
            tx.commit()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    def test_should_fail_with_auth_expired_on_rollback_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        self._read_server1.start(
            script=self.read_tx_script_with_auth_expired_on_rollback(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        try:
            tx.rollback()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()

    def test_should_retry_on_auth_expired_on_begin_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        # FIXME: test assumes that the driver contacts read_server1 first
        # Note: swapping scripts with hooks is not possible because some drivers
        # (e.g., Java) don't call the transaction function if they can't run
        # a successful BEGIN first.
        self._read_server1.start(
            script=self.read_tx_script_with_auth_expired_on_begin(),
            vars=self.get_vars()
        )
        self._read_server2.start(script=self.read_tx_script(),
                                 vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()
        self._read_server2.done()
        # TODO: Some drivers check the result of BEGIN before calling the
        #       transaction function, others don't
        self.assertIn(attempt_count, {1, 2})
        self.assertEqual([[1]], sequences)

    def test_should_retry_on_auth_expired_on_run_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        self._read_server1.start(
            script=self.read_tx_script_with_auth_expired_on_run(),
            vars=self.get_vars()
        )
        self._read_server2.start(
            script=self.read_tx_script_with_auth_expired_on_run(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work, hooks={
            "on_send_RetryableNegative": lambda _: self.switch_unused_servers(
                (self._read_server1, self._read_server2), self.read_tx_script()
            )
        })
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()
        self._read_server2.done()
        self.assertEqual(2, attempt_count)
        self.assertEqual([[1]], sequences)

    def test_should_retry_on_auth_expired_on_pull_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        self._read_server1.start(
            script=self.read_tx_script_with_auth_expired_on_pull(),
            vars=self.get_vars()
        )
        self._read_server2.start(
            script=self.read_tx_script_with_auth_expired_on_pull(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work, hooks={
            "on_send_RetryableNegative": lambda _: self.switch_unused_servers(
                (self._read_server1, self._read_server2), self.read_tx_script()
            )
        })
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()
        self._read_server2.done()
        self.assertEqual(2, attempt_count)
        self.assertEqual([[1]], sequences)

    def test_should_retry_on_auth_expired_on_commit_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routing_server1.start(script=self.router_script(),
                                    vars=self.get_vars())
        self._read_server1.start(
            script=self.read_tx_script_with_auth_expired_on_commit(),
            vars=self.get_vars()
        )
        self._read_server2.start(
            script=self.read_tx_script_with_auth_expired_on_commit(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        attempt_count = 0
        sequences = []

        def work(tx):
            nonlocal attempt_count
            attempt_count += 1
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work, hooks={
            "on_send_RetryablePositive": lambda _: self.switch_unused_servers(
                (self._read_server1, self._read_server2), self.read_tx_script()
            )
        })
        session.close()
        driver.close()

        self._routing_server1.done()
        self._read_server1.done()
        self._read_server2.done()
        self.assertEqual(2, attempt_count)
        self.assertEqual([[1], [1]], sequences)


class AuthorizationTestsV4(AuthorizationTests):
    def router_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO RESET
        !: AUTO GOODBYE
        
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {#ROUTINGMODE# "db": "system"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        {?
            C: RESET
            S: SUCCESS {}
        ?}
        """

    def get_vars(self, host=None):
        if host is None:
            host = self._routing_server1.host
        return {
            "#VERSION#": "4.1",
            "#HOST#": host,
            "#ROUTINGMODE#": '"mode": "r", ',
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }


class AuthorizationTestsV3(AuthorizationTests):
    def router_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO RESET
        !: AUTO GOODBYE
        
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        {?
            C: RESET
            S: SUCCESS {}
        ?}
        """

    def read_script_with_auth_expired_on_pull(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: RUN "RETURN 1 as n" {} {"mode": "r"}
        S: SUCCESS {"fields": ["n"]}
        C: PULL_ALL
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        
        C: BEGIN {"mode": "r"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
        {?
            C: RESET
            S: SUCCESS {}
        ?}
        """

    def read_tx_script_with_auth_expired_on_begin(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"mode": "r"}
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script_with_auth_expired_on_run(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: BEGIN {"mode": "r"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script_with_auth_expired_on_pull(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: BEGIN {"mode": "r"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        S: SUCCESS {"fields": ["n"]}
        C: PULL_ALL
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script_with_auth_expired_on_commit(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: BEGIN {"mode": "r"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def read_tx_script_with_auth_expired_on_rollback(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: BEGIN {"mode": "r"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: ROLLBACK
        S: FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
        S: <EXIT>
        """

    def get_vars(self, host=None):
        if host is None:
            host = self._routing_server1.host
        return {
            "#VERSION#": 3,
            "#HOST#": host,
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }

    def get_db(self):
        return None


class NoRoutingAuthorizationTests(BaseAuthorizationTests):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9010)
        self._uri = "bolt://%s:%d" % (self._server.host,
                                      self._server.port)
        self._auth = types.AuthorizationToken(
            scheme="basic", principal="p", credentials="c")
        self._userAgent = "007"

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def read_return_1_failure_return_2_and_3_succeed_script(self):
        return """
        !: BOLT 4
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        !: ALLOW CONCURRENT

        {+
            {{
                C: RUN "RETURN 1 as n" {} {"mode": "r"}
                C: PULL {"n": 1000}
                S: SUCCESS {"fields": ["n"]}
                   FAILURE {"code": "Neo.ClientError.Security.AuthorizationExpired", "message": "Authorization expired"}
                S: <EXIT>
            ----
                C: RUN "RETURN 2 as n" {} {"mode": "r"}
                S: SUCCESS {"fields": ["n"]}
                C: PULL {"n": 1}
                S: RECORD [1]
                   SUCCESS {"type": "r", "has_more": true}
                {{
                    C: DISCARD {"n": -1}
                    S: SUCCESS {}
                ----
                    C: PULL {"n": "*"}
                    S: RECORD [1]
                       SUCCESS {"type": "r"}
                }}
            ----
                C: RUN "RETURN 3 as n" {} {"mode": "r"}
                C: PULL {"n": "*"}
                S: SUCCESS {"fields": ["n"]}
                   RECORD [1]
                   SUCCESS {"type": "r"}
            }}

        +}
        """

    @driver_feature(types.Feature.AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_drop_connection_after_AuthorizationExpired(self):
        self._server.start(
            script=self.read_return_1_failure_return_2_and_3_succeed_script()
        )
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)

        session1 = driver.session('r', fetchSize=1)
        session2 = driver.session('r')

        session1.run('RETURN 2 as n').next()

        try:
            session2.run('RETURN 1 as n').next()
        except types.DriverError as e:
            self.assert_is_authorization_error(e)

        session2.close()
        session1.close()

        accept_count = self._server.count_responses("<ACCEPT>")

        # fetching another connection and run a query to force
        # drivers which lazy close the connection do it
        session3 = driver.session('r')
        session3.run('RETURN 3 as n').next()
        session3.close()

        hangup_count = self._server.count_responses("<HANGUP>")

        self.assertEqual(accept_count, hangup_count)
        self.assertGreaterEqual(accept_count, 2)

        driver.close()

    @driver_feature(types.Feature.AUTHORIZATION_EXPIRED_TREATMENT)
    def test_should_be_able_to_use_current_sessions_after_AuthorizationExpired(self):
        self._server.start(
            script=self.read_return_1_failure_return_2_and_3_succeed_script()
        )

        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)

        session1 = driver.session('r', fetchSize=1)
        session2 = driver.session('r')

        session1.run('RETURN 2 as n').next()

        try:
            session2.run('RETURN 1 as n').next()
        except types.DriverError as e:
            self.assert_is_authorization_error(e)

        session2.close()

        session1.run('RETURN 2 as n').next()
        session1.close()
