import unittest

from tests.shared import new_backend, get_driver_name
from tests.stub.shared import StubServer
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


script_read = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET
!: AUTO GOODBYE

C: BEGIN {"mode": "r"}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
"""

script_retry_with_fail_after_commit = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE

C: BEGIN {}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
C: COMMIT
S: FAILURE {"code": "$error", "message": "<whatever>"}
C: RESET
S: SUCCESS {}
$extra_reset_1
C: BEGIN {}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
$extra_reset_2
"""

script_retry_with_fail_after_pull = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE

C: BEGIN {}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {}
   FAILURE {"code": "$error", "message": "<whatever>"}
C: RESET
S: SUCCESS {}
$extra_reset_1
C: BEGIN {}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
$extra_reset_2
"""

script_commit_disconnect = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET

C: BEGIN {}
S: SUCCESS {}
C: RUN "RETURN 1" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "w"}
C: COMMIT
S: <EXIT>
"""


class TestRetry(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)
        self._driverName = get_driver_name()

    def tearDown(self):
        self._backend.close()
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analys.
        self._server.reset()

    def test_read(self):
        self._server.start(script=script_read)
        num_retries = 0

        def once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = AuthorizationToken(scheme="basic", principal="neo4j",
                                  credentials="pass")
        driver = Driver(self._backend,
                        "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.readTransaction(once)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 1)

        session.close()
        driver.close()
        self._server.done()

    def _run_with_transient_error(self, script, err):
        # We could probably use AUTO RESET in the script but this makes the
        # diffs more obvious.
        vars = {
            "$extra_reset_1": "",
            "$extra_reset_2": "",
            "$error": err,
        }
        if self._driverName not in ["go", "python"]:
            vars["$extra_reset_2"] = "C: RESET\nS: SUCCESS {}"
        if self._driverName in ["java", "javascript"]:
            vars["$extra_reset_1"] = "C: RESET\nS: SUCCESS {}"

        self._server.start(script=script, vars=vars)
        num_retries = 0

        def twice(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        auth = AuthorizationToken(scheme="basic", principal="neo4j",
                                  credentials="pass")
        driver = Driver(self._backend,
                        "bolt://%s" % self._server.address, auth)
        session = driver.session("r")
        x = session.writeTransaction(twice)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 2)

        session.close()
        driver.close()
        self._server.done()

    def test_retry_database_unavailable(self):
        # Simple case, correctly classified transient error
        self._run_with_transient_error(
                script_retry_with_fail_after_commit,
                "Neo.TransientError.Database.DatabaseUnavailable")

    def test_retry_made_up_transient(self):
        # Driver should retry all transient error (with some exceptions), make
        # up a transient error and the driver should retry.
        self._run_with_transient_error(
                script_retry_with_fail_after_commit,
                "Neo.TransientError.Completely.MadeUp")

    def test_retry_NotALeader(self):
        if get_driver_name() in ['dotnet']:
            self.skipTest("Behaves strange")
        if get_driver_name() in ['java', 'python']:
            self.skipTest("Sends ROLLBACK after RESET")
        # Cluster special treatment
        self._run_with_transient_error(
                script_retry_with_fail_after_pull,
                "Neo.ClientError.Cluster.NotALeader")

    def test_retry_ForbiddenReadOnlyDatabase(self):
        if get_driver_name() in ['dotnet']:
            self.skipTest("Behaves strange")
        if get_driver_name() in ['java', 'python']:
            self.skipTest("Sends ROLLBACK after RESET")
        # Cluster special treatment
        self._run_with_transient_error(
                script_retry_with_fail_after_pull,
                "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase")

    def test_disconnect_on_commit(self):
        # Should NOT retry when connection is lost on unconfirmed commit.
        # The rule could be relaxed on read transactions therefore we test on
        # writeTransaction.  An error should be raised to indicate the failure
        if self._driverName in ["java", 'python', 'javascript', 'dotnet']:
            self.skipTest("Keeps retrying on commit despite connection "
                          "being dropped")
        self._server.start(script=script_commit_disconnect)
        num_retries = 0

        def once(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            result.next()
        auth = AuthorizationToken(scheme="basic")
        driver = Driver(self._backend,
                        "bolt://%s" % self._server.address, auth)
        session = driver.session("w")

        with self.assertRaises(types.DriverError):  # Check further...
            session.writeTransaction(once)

        self.assertEqual(num_retries, 1)
        session.close()
        driver.close()
        self._server.done()

class TestRetryClustering(unittest.TestCase): 
    def setUp(self):
        self._backend = new_backend()
        self._routingServer = StubServer(9001)
        self._readServer = StubServer(9002)
        self._writeServer = StubServer(9003)
        self._uri = "neo4j://%s?region=china&policy=my_policy" % self._routingServer.address
        self._auth = AuthorizationToken(
                scheme="basic", principal="p", credentials="c")
        self._userAgent = "007"
    
    def test_retry_ForbiddenOnReadOnlyDatabase(self):
        self._run_with_transient_error(
                script_retry_with_fail_after_pull,
                "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase")
    def test_retry_NotALeader(self):
        self._run_with_transient_error(
                script_retry_with_fail_after_pull,
                "Neo.ClientError.Cluster.NotALeader")

    def _run_with_transient_error(self, script, err):
        self._routingServer.start(script=self.router_script(), vars=self.get_vars())
        # We could probably use AUTO RESET in the script but this makes the
        # diffs more obvious.
        vars = {
            "$extra_reset_1": "",
            "$extra_reset_2": "",
            "$error": err,
        }
        if get_driver_name() not in ["go", "python"]:
            vars["$extra_reset_2"] = "C: RESET\nS: SUCCESS {}"
        if get_driver_name() in ["java", "javascript"]:
            vars["$extra_reset_1"] = "C: RESET\nS: SUCCESS {}"

        self._writeServer.start(script=script, vars=vars)
        num_retries = 0

        def twice(tx):
            nonlocal num_retries
            num_retries = num_retries + 1
            result = tx.run("RETURN 1")
            record = result.next()
            return record.values[0]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)

        session = driver.session("r")
        x = session.writeTransaction(twice)
        self.assertIsInstance(x, types.CypherInt)
        self.assertEqual(x.value, 1)
        self.assertEqual(num_retries, 2)

        session.close()
        driver.close()
        self._writeServer.done()
        self._routingServer.done()


    def tearDown(self):
        self._backend.close()
        self._routingServer.reset()
        self._readServer.reset()
        self._writeServer.reset()
    
    def router_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# None
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9002"], "role":"READ"}, {"addresses": ["#HOST#:9003"], "role":"WRITE"}]}}
        C: ROUTE #ROUTINGCTX# None
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9002"], "role":"READ"}, {"addresses": ["#HOST#:9003"], "role":"WRITE"}]}}
        C: ROUTE #ROUTINGCTX# None
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9002"], "role":"READ"}, {"addresses": ["#HOST#:9003"], "role":"WRITE"}]}}
        """

    def get_vars(self):
        host = self._routingServer.host
        v = {
            "#VERSION#": "4.3",
            "#HOST#": host,
            "#ROUTINGCTX#": '{"address": "' + host + ':9001", "region": "china", "policy": "my_policy"}',
            "#EXTRA_HELLO_PROPS#": self.get_extra_hello_props(),
        }
        v["#HELLO_ROUTINGCTX#"] = v["#ROUTINGCTX#"]

        if get_driver_name() in ['javascript']:
            v["#HELLO_ROUTINGCTX#"] = '{"region": "china", "policy": "my_policy"}'
        return v

    def get_extra_hello_props(self):
        if get_driver_name() in ["java"]:
            return ', "realm": ""'
        elif get_driver_name() in ["javascript"]:
            return ', "realm": "", "ticket": ""'
        return ""


