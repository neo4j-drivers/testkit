from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


def get_extra_hello_props():
    if get_driver_name() in ["java"]:
        return ', "realm": ""'
    elif get_driver_name() in ["javascript"]:
        return ', "realm": "", "ticket": ""'
    return ""


class AuthorizationTests(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._routingServer1 = StubServer(9000)
        self._readServer1 = StubServer(9010)
        self._readServer2 = StubServer(9011)
        self._uri = "neo4j://%s:%d" % (self._routingServer1.host,
                                       self._routingServer1.port)
        self._auth = types.AuthorizationToken(
            scheme="basic", principal="p", credentials="c")
        self._userAgent = "007"

    def tearDown(self):
        self._routingServer1.reset()
        self._readServer1.reset()
        self._readServer2.reset()
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
            host = self._routingServer1.host
        return {
            "#VERSION#": "4.3",
            "#HOST#": host,
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }

    def get_db(self):
        return "adb"

    # While there is no unified language agnostic error type mapping, a
    # dedicated driver mapping is required to determine if the expected
    # error is returned.
    def assert_is_authorization_error(self, error):
        driver = get_driver_name()
        if driver in ['java']:
            self.assertEqual(
                'org.neo4j.driver.exceptions.AuthorizationExpiredException',
                error.errorType)
        elif driver in ['javascript']:
            self.assertEqual(
                'Neo.ClientError.Security.AuthorizationExpired',
                error.code
            )
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    @staticmethod
    def collectRecords(result):
        sequence = []
        while True:
            next = result.next()
            if isinstance(next, types.NullRecord):
                break
            sequence.append(next.values[0].value)
        return sequence

    def test_should_fail_with_auth_expired_on_pull_using_session_run(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_script_with_auth_expired_on_pull(),
            vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        try:
            session.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()

    def test_should_fail_with_auth_expired_on_begin_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_auth_expired_on_begin(),
            vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        try:
            session.beginTransaction()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()

    def test_should_fail_with_auth_expired_on_run_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_auth_expired_on_run(),
            vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        try:
            tx.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()

    def test_should_fail_with_auth_expired_on_pull_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_auth_expired_on_pull(),
            vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        try:
            tx.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()

    def test_should_fail_with_auth_expired_on_commit_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_auth_expired_on_commit(),
            vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n").consume()
        try:
            tx.commit()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()

    def test_should_fail_with_auth_expired_on_rollback_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_auth_expired_on_rollback(),
            vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        try:
            tx.rollback()
        except types.DriverError as e:
            self.assert_is_authorization_error(error=e)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()

    def test_should_retry_on_auth_expired_on_begin_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_auth_expired_on_begin(),
            vars=self.get_vars())
        self._readServer2.start(script=self.read_tx_script(),
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

        self._routingServer1.done()
        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual(1, attempt_count)
        self.assertEqual([[1]], sequences)

    def test_should_retry_on_auth_expired_on_run_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_auth_expired_on_run(),
            vars=self.get_vars())
        self._readServer2.start(script=self.read_tx_script(),
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

        self._routingServer1.done()
        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual(2, attempt_count)
        self.assertEqual([[1]], sequences)

    def test_should_retry_on_auth_expired_on_pull_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_auth_expired_on_pull(),
            vars=self.get_vars())
        self._readServer2.start(script=self.read_tx_script(),
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

        self._routingServer1.done()
        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual(2, attempt_count)
        self.assertEqual([[1]], sequences)

    def test_should_retry_on_auth_expired_on_commit_using_tx_function(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go', 'python', 'dotnet']:
            self.skipTest("requires authorization expired response support")
        driver = Driver(self._backend, self._uri, self._auth,
                        userAgent=self._userAgent)
        self._routingServer1.start(script=self.router_script(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_auth_expired_on_commit(),
            vars=self.get_vars())
        self._readServer2.start(script=self.read_tx_script(),
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

        self._routingServer1.done()
        self._readServer1.done()
        self._readServer2.done()
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
            host = self._routingServer1.host
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
        
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {#ROUTINGMODE#}
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
            host = self._routingServer1.host
        return {
            "#VERSION#": 3,
            "#HOST#": host,
            "#ROUTINGMODE#": "",
            "#ROUTINGCTX#": '{"address": "' + host + ':9000"}'
        }

    def get_db(self):
        return None
