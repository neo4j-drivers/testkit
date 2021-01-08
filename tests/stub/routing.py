import unittest

from tests.shared import get_driver_name, new_backend
from tests.stub.shared import StubServer
from nutkit.frontend import Driver, AuthorizationToken


def get_extra_hello_props():
    if get_driver_name() in ["java"]:
        return ', "realm": ""'
    elif get_driver_name() in ["javascript"]:
        return ', "realm": "", "ticket": ""'
    return ""


# This should be the latest/current version of the protocol.
# Older protocol that needs to be tested inherits from this and override
# to handle variations.
class Routing(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._routingServer = StubServer(9001)
        self._readServer = StubServer(9002)
        self._writeServer = StubServer(9003)
        self._uri = "neo4j://%s?region=china&policy=my_policy" % self._routingServer.address
        self._auth = AuthorizationToken(scheme="basic", principal="p", credentials="c")
        self._userAgent = "007"

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
        C: RUN "CALL dbms.routing.getRoutingTable($context, $#DBPARAM#)" {"context": #ROUTINGCTX#, "#DBPARAM#": "adb"} {"mode": "r", "db": "system"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9002"], "role":"READ"}, {"addresses": ["#HOST#:9003"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def read_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {"mode": "r", "db": "adb"}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
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
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
        """

    def write_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {"db": "adb"}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "w"}
        """

    def write_tx_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
        """

    def get_vars(self):
        host = self._routingServer.host
        v = {
            "#VERSION#": "4.1",
            "#DBPARAM#": "db",
            "#HOST#": host,
            "#ROUTINGCTX#": '{"address": "' + host + ':9001", "region": "china", "policy": "my_policy"}',
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
        }
        v["#HELLO_ROUTINGCTX#"] = v["#ROUTINGCTX#"]

        if get_driver_name() in ['dotnet', 'java', 'javascript', 'python']:
            v["#DBPARAM#"] = "database"

        if get_driver_name() in ['javascript']:
            v["#HELLO_ROUTINGCTX#"] = '{"region": "china", "policy": "my_policy"}'

        return v

    def get_db(self):
        return "adb"

    # Checks that routing is used to connect to correct server and that
    # parameters for session run is passed on to the target server
    # (not the router).
    def test_session_run_read(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer.start(script=self.router_script(),
                                  vars=self.get_vars())
        self._readServer.start(script=self.read_script(), vars=self.get_vars())
        session = driver.session('r', database=self.get_db())
        session.run("RETURN 1 as n")
        session.close()
        driver.close()
        self._routingServer.done()
        self._readServer.done()

    # Same test as for session.run but for transaction run.
    def test_tx_run_read(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer.start(script=self.router_script(),
                                  vars=self.get_vars())
        self._readServer.start(script=self.read_tx_script(),
                               vars=self.get_vars())
        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()
        driver.close()
        self._routingServer.done()
        self._readServer.done()

    # Checks that write server is used
    def test_session_run_write(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer.start(script=self.router_script(),
                                  vars=self.get_vars())
        self._writeServer.start(script=self.write_script(),
                                vars=self.get_vars())
        session = driver.session('w', database=self.get_db())
        session.run("RETURN 1 as n")
        session.close()
        driver.close()
        self._routingServer.done()
        self._writeServer.done()

    # Checks that write server is used
    def test_tx_run_write(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer.start(script=self.router_script(),
                                  vars=self.get_vars())
        self._writeServer.start(script=self.write_tx_script(),
                                vars=self.get_vars())
        session = driver.session('w', database=self.get_db())
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()
        driver.close()
        self._routingServer.done()
        self._writeServer.done()


class RoutingV3(Routing):
    def router_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {#ROUTINGMODE#}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9002"], "role":"READ"}, {"addresses": ["#HOST#:9003"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def read_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {"mode": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
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
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
        """

    def write_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "w"}
        """

    def write_tx_script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           SUCCESS {"type": "w"}
        C: COMMIT
        S: SUCCESS {}
        """

    def get_vars(self):
        host = self._routingServer.host
        v = {
            "#VERSION#": 3,
            "#HOST#": host,
            "#ROUTINGMODE#": "",
            "#ROUTINGCTX#": '{"address": "' + host + ':9001", "region": "china", "policy": "my_policy"}',
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
            "#EXTR_HELLO_ROUTING_PROPS#": "",
        }

        if get_driver_name() in ['go']:
            v["#ROUTINGMODE#"] = '"mode": "r"'

        if get_driver_name() in ['java']:
            v["#EXTR_HELLO_ROUTING_PROPS#"] = ', "routing": ' + v['#ROUTINGCTX#'] 

        if get_driver_name() in ['javascript']:
            v["#ROUTINGCTX#"] = '{"region": "china", "policy": "my_policy"}'

        return v

    def get_db(self):
        return None


class NoRouting(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)

    def tearDown(self):
        self._backend.close()
        self._server.reset()

    def script(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #ROUTING# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.1.0", "connection_id": "bolt-123456789"}
        C: RUN "RETURN 1 as n" {} {"mode": "r", "db": "adb"}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        """

    def get_vars(self):
        return {
            "#VERSION#": "4.1",
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
            "#ROUTING#": ', "routing": null' if get_driver_name() not in ['javascript'] else ''
        }

    # Checks that routing is disabled when URI is bolt, no routing in
    # HELLO and no call to retrieve routing table. From bolt >= 4.1 the
    # routing context is used to disable/enable server side routing.
    def test_session_run_read(self):
        # Driver is configured to talk to "routing" stub server
        uri = "bolt://%s" % self._server.address
        self._server.start(script=self.script(), vars=self.get_vars())
        driver = Driver(self._backend, uri,
                        AuthorizationToken(scheme="basic", principal="p",
                                           credentials="c"),
                        userAgent="007")
        session = driver.session('r', database="adb")
        session.run("RETURN 1 as n")
        session.close()
        driver.close()
        self._server.done()
