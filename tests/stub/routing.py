import fcntl
import json
import socket
import struct
from sys import platform

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


# This should be the latest/current version of the protocol.
# Older protocol that needs to be tested inherits from this and override
# to handle variations.
class Routing(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._routingServer1 = StubServer(9000)
        self._routingServer2 = StubServer(9001)
        self._routingServer3 = StubServer(9002)
        self._readServer1 = StubServer(9010)
        self._readServer2 = StubServer(9011)
        self._readServer3 = StubServer(9012)
        self._writeServer1 = StubServer(9020)
        self._writeServer2 = StubServer(9021)
        self._writeServer3 = StubServer(9022)
        self._uri = "neo4j://%s?region=china&policy=my_policy" % self._routingServer1.address
        self._auth = types.AuthorizationToken(
            scheme="basic", principal="p", credentials="c")
        self._userAgent = "007"

    def tearDown(self):
        self._routingServer1.reset()
        self._routingServer2.reset()
        self._routingServer3.reset()
        self._readServer1.reset()
        self._readServer2.reset()
        self._readServer3.reset()
        self._writeServer1.reset()
        self._writeServer2.reset()
        self._writeServer3.reset()
        super().tearDown()

    def router_script_adb(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]}}
        """

    def router_script_adb_multi(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        !: ALLOW RESTART

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {+
            C: ROUTE #ROUTINGCTX# "adb"
            S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]}}
        +}
        """

    def router_script_default_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# null
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]}}
        """

    def router_script_connectivity_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        !: ALLOW RESTART

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {{
            C: ROUTE #ROUTINGCTX# null
        ----
            C: ROUTE #ROUTINGCTX# "system"
        }}
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]}}
        """

    def router_script_with_procedure_not_found_failure_connectivity_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {{
            C: ROUTE #ROUTINGCTX# null
        ----
            C: ROUTE #ROUTINGCTX# "system"
        }}
        S: FAILURE {"code": "Neo.ClientError.Procedure.ProcedureNotFound", "message": "blabla"}
        S: IGNORED
        S: <EXIT>
        """

    def router_script_with_unknown_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "*"
        S: FAILURE {"code": "Neo.ClientError.General.Unknown", "message": "wut!"}
        S: IGNORED
        S: <EXIT>
        """

    def router_script_with_leader_change(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9020"],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"}, {"addresses": ["#HOST#:9000"], "role": "ROUTE"}]}}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9020"],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"}, {"addresses": ["#HOST#:9000"], "role": "ROUTE"}]}}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": [],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"}, {"addresses": ["#HOST#:9000"], "role": "ROUTE"}]}}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9021"],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"}, {"addresses": ["#HOST#:9000"], "role": "ROUTE"}]}}
        """

    def router_script_with_another_router(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9012"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9022"], "role":"WRITE"}]}}
        """

    def router_script_with_reader_support(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {?
            C: ROUTE #ROUTINGCTX# "adb"
            S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9000", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]}}
            {?
                C: GOODBYE
                S: SUCCESS {}
                   <EXIT>
            ?}
        ?}
        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
           <EXIT>
        """

    def router_script_with_one_reader_and_exit(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]}}
           <EXIT>
        """

    def router_script_with_another_router_and_fake_reader(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {{
            C: ROUTE #ROUTINGCTX# null
        ----
            C: ROUTE #ROUTINGCTX# "system"
        }}
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9100"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9022"], "role":"WRITE"}]}}
           <EXIT>
        """

    def router_script_with_empty_context_and_reader_support(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        {{
            C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": {"address": "#HOST#:9000"} #EXTRA_HELLO_PROPS#}
        ----
            C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": {} #EXTRA_HELLO_PROPS#}
        }}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {?
            C: ROUTE {"address": "#HOST#:9000"} "adb"
            S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9000", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]}}
            {?
                C: GOODBYE
                S: SUCCESS {}
                   <EXIT>
            ?}
        ?}
        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
        """

    def router_script_with_empty_writers_adb(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": [], "role":"WRITE"}]}}
           <EXIT>
        """

    def router_script_with_empty_writers_any_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "*"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": [], "role":"WRITE"}]}}
        """

    def router_script_with_one_writer(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020"], "role":"WRITE"}]}}
        """

    def router_script_with_the_other_one_writer(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9021"], "role":"WRITE"}]}}
        """

    def router_script_with_another_router_and_non_existent_reader(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9099"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]}}
           <EXIT>
        """

    def router_script_with_empty_response(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": []}}
        """

    def router_script_with_db_not_found_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        !: ALLOW RESTART

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: ROUTE #ROUTINGCTX# "adb"
        S: FAILURE {"code": "Neo.ClientError.Database.DatabaseNotFound", "message": "wut!"}
        {?
            C: RESET
            S: SUCCESS {}
        ?}
        {?
            C: GOODBYE
            S: SUCCESS {}
               <EXIT>
        ?}
        """

    def router_script_with_unreachable_db_and_adb_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS#}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {?
            C: ROUTE #ROUTINGCTX# "unreachable"
            S: SUCCESS { "rt": { "ttl": 1000, "servers": []}}
            {?
                C: GOODBYE
                S: SUCCESS {}
                   <EXIT>
            ?}
        ?}
        C: ROUTE #ROUTINGCTX# "adb"
        S: SUCCESS { "rt": { "ttl": 1000, "servers": [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]}}
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

    def read_script_default_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {"mode": "r"}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        """

    def read_script_with_unexpected_interruption(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {"mode": "r", "db": "adb"}
        C: PULL {"n": 1000}
        S: <EXIT>
        """

    def read_script_with_bookmarks(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {"mode": "r", "db": "adb", "bookmarks{}": ["sys:1234", "foo:5678"]}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r", "bookmark": "foo:6678"}
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
        """

    def read_tx_script_with_exit(self):
        return "{}\n<EXIT>".format(self.read_tx_script())

    def read_tx_script_with_bookmarks(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"mode": "r", "db": "adb", "bookmarks": ["OldBookmark"]}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {"bookmark": "NewBookmark"}
        """

    def read_tx_script_with_unexpected_interruption(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: <EXIT>
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

    def write_script_with_unexpected_interruption(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {"db": "adb"}
        C: PULL {"n": 1000}
        S: <EXIT>
        """

    def write_script_with_bookmark(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {"db": "adb", "bookmarks": ["NewBookmark"]}
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

    def write_tx_script_with_leader_switch_and_retry(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: FAILURE {"code": "Neo.ClientError.Cluster.NotALeader", "message": "blabla"}
           IGNORED
        C: BEGIN {"db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "w"}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "w"}
        C: COMMIT
        S: SUCCESS {}
        """

    def write_tx_script_with_bookmarks(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"db": "adb", "bookmarks": ["OldBookmark"]}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {"bookmark": "NewBookmark"}
        """

    def write_read_tx_script_with_bookmark(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"db": "adb", "bookmarks": ["BookmarkA"]}
        S: SUCCESS {}
        C: RUN "CREATE (n {name:'Bob'})" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["name"]}
           SUCCESS {"type": "w"}
        C: COMMIT
        S: SUCCESS {"bookmark": "BookmarkB"}
        C: BEGIN {"db": "adb", "bookmarks": ["BookmarkB"]}
        S: SUCCESS {}
        C: RUN "MATCH (n) RETURN n.name AS name" {} {}
           PULL {"n": 1000}
        S: SUCCESS {"fields": ["name"]}
           RECORD ["Bob"]
           SUCCESS {}
        C: COMMIT
        S: SUCCESS {"bookmark": "BookmarkC"}
        """

    def write_tx_script_with_unexpected_interruption(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: <EXIT>
        """

    def write_script_with_not_a_leader_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: RUN "RETURN 1 as n" {} {"db": "adb"}
        C: PULL {"n": 1000}
        S: FAILURE {"code": "Neo.ClientError.Cluster.NotALeader", "message": "blabla"}
        S: IGNORED
        C: RESET
        S: SUCCESS {}
        """

    def write_tx_script_with_not_a_leader_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: FAILURE {"code": "Neo.ClientError.Cluster.NotALeader", "message": "blabla"}
        S: IGNORED
        C: RESET
        S: SUCCESS {}
        """

    def write_tx_script_with_database_unavailable_failure_on_commit(self):
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
        S: FAILURE {"code": "Neo.TransientError.General.DatabaseUnavailable", "message": "Database shut down."}
        S: <EXIT>
        """

    def write_tx_script_multiple_bookmarks(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"db": "adb", "bookmarks{}": ["neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29", "neo4j:bookmark:v1:tx94", "neo4j:bookmark:v1:tx56", "neo4j:bookmark:v1:tx16", "neo4j:bookmark:v1:tx68"]}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {"bookmark": "neo4j:bookmark:v1:tx95"}
        """

    def write_tx_script_with_database_unavailable_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET

        C: BEGIN {"db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        S: SUCCESS {}
        C: PULL {"n": 1000}
        S: FAILURE {"code": "Neo.TransientError.General.DatabaseUnavailable", "message": "Database is busy doing store copy"}
        C: RESET
        S: SUCCESS {}
        """

    def get_vars(self):
        host = self._routingServer1.host
        v = {
            "#VERSION#": "4.3",
            "#HOST#": host,
            "#ROUTINGCTX#": '{"address": "' + host + ':9000", "region": "china", "policy": "my_policy"}',
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
        }
        v["#HELLO_ROUTINGCTX#"] = v["#ROUTINGCTX#"]

        if get_driver_name() in ['javascript']:
            v["#HELLO_ROUTINGCTX#"] = '{"region": "china", "policy": "my_policy"}'

        return v

    def get_db(self):
        return "adb"

    def route_call_count(self, server):
        return server.count_requests("ROUTE")

    @staticmethod
    def collectRecords(result):
        sequence = []
        while True:
            next = result.next()
            if isinstance(next, types.NullRecord):
                break
            sequence.append(next.values[0].value)
        return sequence

    @staticmethod
    def get_ip_address(NICname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', NICname[:15].encode("UTF-8"))
        )[20:24])

    def get_ip_addresses(self):
        ip_addresses = []
        for ix in socket.if_nameindex():
            name = ix[1]
            ip = self.get_ip_address(name)
            if name != "lo":
                ip_addresses.append(ip)
        return ip_addresses

    def test_should_successfully_get_routing_table_with_context(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'go', 'javascript']:
            self.skipTest("needs verifyConnectivity support")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_connectivity_db(),
                                   vars=self.get_vars())

        driver.verifyConnectivity()
        driver.close()

        self._routingServer1.done()

    # Checks that routing is used to connect to correct server and that
    # parameters for session run is passed on to the target server
    # (not the router).
    def test_should_read_successfully_from_reader_using_session_run(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_script(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)

    def test_should_read_successfully_from_reader_using_session_run_with_default_db_driver(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_default_db(),
                                   vars=self.get_vars())
        self._readServer1.start(script=self.read_script_default_db(),
                                vars=self.get_vars())

        session = driver.session('r')
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)

    # Same test as for session.run but for transaction run.
    def test_should_read_successfully_from_reader_using_tx_run(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        result = tx.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        tx.commit()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)

    def test_should_read_successfully_from_reader_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
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

    def test_should_fail_when_reading_from_unexpectedly_interrupting_reader_using_session_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['javascript', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_script_with_unexpected_interruption(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
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
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script_with_unexpected_interruption(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        tx = session.beginTransaction()
        failed = False
        try:
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual('org.neo4j.driver.exceptions.SessionExpiredException', e.errorType)
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(failed)

    # Checks that write server is used
    def test_should_write_successfully_on_writer_using_session_run(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_script(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
        session.run("RETURN 1 as n")
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()

    # Checks that write server is used
    def test_should_write_successfully_on_writer_using_tx_run(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()

    def test_should_write_successfully_on_writer_using_tx_function(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())

        def work(tx):
            tx.run("RETURN 1 as n")

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()

    def test_should_write_successfully_on_leader_switch_using_tx_function(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent, None)
        self._routingServer1.start(script=self.router_script_adb_multi(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_with_leader_switch_and_retry(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual([[1], [1]], sequences)
        self.assertEqual(self.route_call_count(self._routingServer1), 2)

    def test_should_retry_write_until_success_with_leader_change_using_tx_function(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'go', 'javascript']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_leader_change(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_with_unexpected_interruption(), vars=self.get_vars())
        self._writeServer2.start(script=self.write_tx_script(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
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
        if get_driver_name() in ['dotnet', 'go', 'javascript']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_leader_change(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_with_database_unavailable_failure_on_commit(),
                                 vars=self.get_vars())
        self._writeServer2.start(script=self.write_tx_script(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
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
        if get_driver_name() in ['javascript', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_script_with_unexpected_interruption(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
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
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_with_unexpected_interruption(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
        tx = session.beginTransaction()
        failed = False
        try:
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual('org.neo4j.driver.exceptions.SessionExpiredException', e.errorType)
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_discovery_when_router_fails_with_procedure_not_found_code(self):
        # TODO add support and remove this block
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_procedure_not_found_failure_connectivity_db(), vars=self.get_vars())

        failed = False
        try:
            driver.verifyConnectivity()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual('org.neo4j.driver.exceptions.ServiceUnavailableException', e.errorType)
            failed = True
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def test_should_fail_discovery_when_router_fails_with_unknown_code(self):
        # TODO add support and remove this block
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_unknown_failure(), vars=self.get_vars())

        failed = False
        try:
            driver.verifyConnectivity()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual('org.neo4j.driver.exceptions.ServiceUnavailableException', e.errorType)
            failed = True
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("consume not implemented in backend")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_script_with_not_a_leader_failure(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
        failed = False
        try:
            session.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual('org.neo4j.driver.exceptions.SessionExpiredException', e.errorType)
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['javascript', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_script_with_not_a_leader_failure(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
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
                        e.errorType
                    )
                failed = True

        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_on_writer_that_returns_not_a_leader_code_using_tx_run(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'go']:
            self.skipTest("consume not implemented in backend")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_with_not_a_leader_failure(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
        tx = session.beginTransaction()
        failed = False
        try:
            tx.run("RETURN 1 as n").consume()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual('org.neo4j.driver.exceptions.SessionExpiredException', e.errorType)
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_fail_when_writing_without_explicit_consumption_on_writer_that_returns_not_a_leader_code_using_tx_run(
            self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_with_not_a_leader_failure(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
        tx = session.beginTransaction()
        failed = False
        try:
            # drivers doing eager loading will fail here
            tx.run("RETURN 1 as n")
            # else they should fail here
            tx.commit()
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual('org.neo4j.driver.exceptions.SessionExpiredException', e.errorType)
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertTrue(failed)

    def test_should_use_write_session_mode_and_initial_bookmark_when_writing_using_tx_run(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_with_bookmarks(), vars=self.get_vars())

        session = driver.session('w', bookmarks=["OldBookmark"], database=self.get_db())
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        last_bookmarks = session.lastBookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual(["NewBookmark"], last_bookmarks)

    def test_should_use_read_session_mode_and_initial_bookmark_when_reading_using_tx_run(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script_with_bookmarks(), vars=self.get_vars())

        session = driver.session('r', bookmarks=["OldBookmark"], database=self.get_db())
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
        # TODO remove this block once all languages work
        if get_driver_name() in ['javascript']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_read_tx_script_with_bookmark(), vars=self.get_vars())

        session = driver.session('w', bookmarks=["BookmarkA"], database=self.get_db())
        tx = session.beginTransaction()
        tx.run("CREATE (n {name:'Bob'})")
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

    def test_should_retry_read_tx_until_success(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(),
                                   vars=self.get_vars())
        self._readServer1.start(
            script=self.read_tx_script_with_unexpected_interruption(),
            vars=self.get_vars()
        )
        self._readServer2.start(
            script=self.read_tx_script_with_unexpected_interruption(),
            vars=self.get_vars()
        )

        session = driver.session('r', database=self.get_db())
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            try:
                result = tx.run("RETURN 1 as n")
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
                working_reader.start(script=self.read_tx_script(),
                                     vars=self.get_vars())
                raise

            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self._readServer2.done()
        self.assertEqual([[1]], sequences)
        self.assertEqual(2, try_count)

    def test_should_retry_write_tx_until_success(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(),
                                   vars=self.get_vars())
        self._writeServer1.start(
            script=self.write_tx_script_with_unexpected_interruption(),
            vars=self.get_vars()
        )
        self._writeServer2.start(
            script=self.write_tx_script_with_unexpected_interruption(),
            vars=self.get_vars()
        )

        session = driver.session('w', database=self.get_db())
        sequences = []
        try_count = 0

        def work(tx):
            nonlocal try_count
            try_count = try_count + 1
            try:
                result = tx.run("RETURN 1 as n")
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
                working_writer.start(script=self.write_tx_script(),
                                     vars=self.get_vars())
                raise
            sequences.append(self.collectRecords(result))

        session.writeTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self._writeServer2.done()
        self.assertEqual([[]], sequences)
        self.assertEqual(2, try_count)

    def test_should_retry_read_tx_and_rediscovery_until_success(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'javascript', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_another_router(), vars=self.get_vars())
        self._routingServer2.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script_with_unexpected_interruption(), vars=self.get_vars())
        self._readServer2.start(script=self.read_tx_script(), vars=self.get_vars())
        self._readServer3.start(script=self.read_tx_script_with_unexpected_interruption(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
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
        if get_driver_name() in ['dotnet', 'javascript', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_another_router(), vars=self.get_vars())
        self._routingServer2.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_with_unexpected_interruption(), vars=self.get_vars())
        self._writeServer2.start(script=self.write_tx_script(), vars=self.get_vars())
        self._writeServer3.start(script=self.write_tx_script_with_unexpected_interruption(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
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
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_another_router_and_fake_reader(),
                                   vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script(), vars=self.get_vars())

        driver.verifyConnectivity()
        self._routingServer1.done()
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        session = driver.session('r', database=self.get_db())
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
        if get_driver_name() in ['python']:
            self.skipTest("requires investigation")
        # TODO remove this block once it works
        if get_driver_name() in ['java']:
            self.skipTest("java driver uses separate connections for routing and reading, "
                          "but the stub server does not currently support this")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_reader_support(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        sequences = []

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        session.close()
        driver.close()

        self._routingServer1.done()
        self.assertLessEqual(
            self._routingServer1.count_responses("<ACCEPT>"), 2
        )
        self.assertEqual(self._routingServer1.count_requests("COMMIT"), 1)
        self.assertEqual([[1]], sequences)

    def test_should_send_empty_hello(self):
        # TODO remove this block once all languages work
        driver = Driver(self._backend, "neo4j://%s" % self._routingServer1.address, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_empty_context_and_reader_support(),
                                   vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
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
        if get_driver_name() in ['dotnet', 'go']:
            self.skipTest("consume not implemented in backend or requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_empty_writers_adb(), vars=self.get_vars())
        self._routingServer2.start(script=self.router_script_with_empty_writers_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
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
                self.assertEqual('org.neo4j.driver.exceptions.SessionExpiredException', e.errorType)
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
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("verifyConnectivity not implemented in backend")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_empty_writers_any_db(), vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script_with_bookmarks(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_script_with_bookmark(), vars=self.get_vars())

        driver.verifyConnectivity()
        session = driver.session('w', bookmarks=["OldBookmark"], database=self.get_db())
        sequences = []
        self._routingServer1.done()
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())

        def work(tx):
            result = tx.run("RETURN 1 as n")
            sequences.append(self.collectRecords(result))

        session.readTransaction(work)
        session.run("RETURN 1 as n").consume()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self._writeServer1.done()
        self.assertEqual([[1]], sequences)

    def test_should_accept_routing_table_with_single_router(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_script(), vars=self.get_vars())
        self._readServer2.start(script=self.read_script(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
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
            self._readServer2.stop()
        else:
            self._readServer1.stop()
            self._readServer2.done()
        self.assertEqual([1], sequence)

    def test_should_successfully_send_multiple_bookmarks(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_multiple_bookmarks(), vars=self.get_vars())

        session = driver.session('w',
                                 bookmarks=["neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29", "neo4j:bookmark:v1:tx94",
                                            "neo4j:bookmark:v1:tx56",
                                            "neo4j:bookmark:v1:tx16", "neo4j:bookmark:v1:tx68"], database=self.get_db())
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        last_bookmarks = session.lastBookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._writeServer1.done()
        self.assertEqual(["neo4j:bookmark:v1:tx95"], last_bookmarks)

    def test_should_forget_address_on_database_unavailable_error(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'go']:
            self.skipTest("requires investigation")
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_one_writer(), vars=self.get_vars())
        self._writeServer1.start(script=self.write_tx_script_with_database_unavailable_failure(), vars=self.get_vars())
        self._routingServer2.start(script=self.router_script_with_the_other_one_writer(), vars=self.get_vars())
        self._writeServer2.start(script=self.write_tx_script(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
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
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("resolver not implemented in backend")
        resolver_invoked = False

        def resolver(address):
            nonlocal resolver_invoked
            address_tokens = address.split(':')
            if not resolver_invoked:
                resolver_invoked = True
                return [address]
            elif self._routingServer1.host == address_tokens[0] and self._routingServer1.port == int(address_tokens[1]):
                return [self._routingServer2.address]
            self.fail("unexpected")

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent, resolver)
        self._routingServer1.start(script=self.router_script_with_one_reader_and_exit(), vars=self.get_vars())
        self._routingServer2.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script_with_exit(), vars=self.get_vars())
        self._readServer2.start(script=self.read_tx_script(), vars=self.get_vars())

        session = driver.session('w', database=self.get_db())
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
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("resolver not implemented in backend")

        def resolver(address):
            self.assertEqual(self._routingServer1.address, address)
            return [self._routingServer1.address, self._routingServer3.address]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent, resolver)
        self._routingServer1.start(script=self.router_script_with_another_router_and_non_existent_reader(),
                                   vars=self.get_vars())
        self._routingServer2.start(script=self.router_script_with_empty_response(), vars=self.get_vars())
        self._routingServer3.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_tx_script(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
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

    def should_support_multi_db(self):
        return True

    def test_should_successfully_check_if_support_for_multi_db_is_available(self):
        # TODO add support and remove this block
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("supportsMultiDb not implemented in backend")

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_default_db(), vars=self.get_vars())
        self._readServer1.start(script=self.read_script(), vars=self.get_vars())

        supports_multi_db = driver.supportsMultiDB()

        driver.close()
        self._routingServer1.done()
        self.assertLessEqual(self._readServer1.count_responses("<ACCEPT>"), 1)
        self.assertEqual(self._readServer1.count_requests("RUN"), 0)
        self._readServer1.reset()
        self.assertEqual(self.should_support_multi_db(), supports_multi_db)

    def test_should_read_successfully_on_empty_discovery_result_using_session_run(self):
        # TODO add support and remove this block
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("resolver not implemented in backend")

        def resolver(address):
            return [self._routingServer1.address, self._routingServer2.address]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent, resolver)
        self._routingServer1.start(script=self.router_script_with_empty_response(), vars=self.get_vars())
        self._routingServer2.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_script(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
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
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("add code support")
        if not self.should_support_multi_db():
            return

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_db_not_found_failure(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        failed = False
        try:
            session.run("RETURN 1 as n")
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual('org.neo4j.driver.exceptions.FatalDiscoveryException', e.errorType)
            self.assertEqual('Neo.ClientError.Database.DatabaseNotFound', e.code)
            failed = True
        session.close()
        driver.close()

        self._routingServer1.done()
        self.assertTrue(failed)

    def test_should_read_successfully_from_reachable_db_after_trying_unreachable_db(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['javascript', 'go']:
            self.skipTest("requires investigation")

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_with_unreachable_db_and_adb_db(), vars=self.get_vars())
        self._readServer1.start(script=self.read_script(), vars=self.get_vars())

        session = driver.session('r', database="unreachable")
        failed_on_unreachable = False
        try:
            session.run("RETURN 1 as n")
        except types.DriverError as e:
            if get_driver_name() in ['java']:
                self.assertEqual('org.neo4j.driver.exceptions.ServiceUnavailableException', e.errorType)
            failed_on_unreachable = True
        session.close()

        session = driver.session('r', database=self.get_db())
        result = session.run("RETURN 1 as n")
        self.assertEqual(self.route_call_count(self._routingServer1), 2)
        sequence = self.collectRecords(result)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertTrue(failed_on_unreachable)
        self.assertEqual([1], sequence)

    def test_should_pass_system_bookmark_when_getting_rt_for_multi_db(self):
        pass

    def test_should_ignore_system_bookmark_when_getting_rt_for_multi_db(self):
        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_script_with_bookmarks(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db(), bookmarks=["sys:1234", "foo:5678"])
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
        if get_driver_name() in ['javascript', 'go', 'dotnet']:
            self.skipTest("add resolvers and connection timeout support")

        resolver_call_num = 0
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

        # The resolver is used to convert the original address to multiple fake domain names.
        def resolver(address):
            nonlocal resolver_call_num
            nonlocal resolved_addresses
            self.assertEqual(0, resolver_call_num)
            self.assertEqual(self._routingServer1.address, address)
            resolver_call_num += 1
            return resolved_addresses

        # The domain name resolver is used to verify that the fake domain names are given to it
        # and to convert them to routing server addresses.
        # This resolver is expected to be called multiple times.
        # The combined use of resolver and domain name resolver allows to host multiple initial routers on a single IP.
        def domainNameResolver(name):
            nonlocal domain_name_resolver_call_num
            nonlocal resolved_addresses
            nonlocal resolved_domain_name_addresses
            if domain_name_resolver_call_num >= len(resolved_addresses):
                return [name]
            expected_name = resolved_addresses[domain_name_resolver_call_num].split(":")[0]
            self.assertEqual(expected_name, name)
            resolved_domain_name_address = resolved_domain_name_addresses[domain_name_resolver_call_num]
            domain_name_resolver_call_num += 1
            return [resolved_domain_name_address]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent, resolverFn=resolver,
                        domainNameResolverFn=domainNameResolver, connectionTimeoutMs=1000)
        self._routingServer1.start(script=self.router_script_with_unknown_failure(), vars=self.get_vars())
        # _routingServer2 is deliberately turned off
        self._routingServer3.start(script=self.router_script_adb(), vars=self.get_vars())
        self._readServer1.start(script=self.read_script(), vars=self.get_vars())

        session = driver.session('r', database=self.get_db())
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        session.close()
        driver.close()

        self._routingServer1.done()
        self._routingServer3.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)

    def test_should_successfully_acquire_rt_when_router_ip_changes(self):
        # TODO remove this block once all languages work
        if get_driver_name() in ['dotnet', 'go', 'javascript']:
            self.skipTest("needs verifyConnectivity support")
        ip_addresses = []
        if platform == "linux":
            ip_addresses = self.get_ip_addresses()
        if len(ip_addresses) < 2:
            self.skipTest("at least 2 IP addresses are required for this test "
                          "and only linux is supported at the moment")

        router_ip_address = ip_addresses[0]

        def domain_name_resolver(ignored):
            nonlocal router_ip_address
            return [router_ip_address]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent,
                        domainNameResolverFn=domain_name_resolver)
        self._routingServer1.start(script=self.router_script_with_one_reader_and_exit(), vars=self.get_vars())

        driver.verifyConnectivity()
        self._routingServer1.done()
        router_ip_address = ip_addresses[1]
        self._routingServer1.start(script=self.router_script_with_one_reader_and_exit(), vars=self.get_vars())
        driver.verifyConnectivity()
        driver.close()

        self._routingServer1.done()


class RoutingV4(Routing):
    def router_script_adb(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_adb_multi(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {+
            C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
            C: PULL {"n": -1}
            S: SUCCESS {"fields": ["ttl", "servers"]}
            S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
            S: SUCCESS {"type": "r"}
        +}
        """

    def router_script_default_db(self):
        # The first getRoutingTable variant is less verbose and preferable.
        # Since this is legacy functionality anyway, we don't enforce it.
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {{
            C: RUN "CALL dbms.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        ----
            C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": null} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        }}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_connectivity_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {+
            {{
                C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": null} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
            ----
                C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "system"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
            ----
                C: RUN "CALL dbms.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
            }}
            C: PULL {"n": -1}
            S: SUCCESS {"fields": ["ttl", "servers"]}
            S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
            S: SUCCESS {"type": "r"}
        +}
        """

    def router_script_with_procedure_not_found_failure_connectivity_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: FAILURE {"code": "Neo.ClientError.Procedure.ProcedureNotFound", "message": "blabla"}
        S: IGNORED
        S: <EXIT>
        """

    def router_script_with_unknown_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: FAILURE {"code": "Neo.ClientError.General.Unknown", "message": "wut!"}
        S: IGNORED
        S: <EXIT>
        """

    def router_script_with_leader_change(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
           PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
           RECORD [9223372036854775807, [{"addresses": ["#HOST#:9020"],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"}, {"addresses": ["#HOST#:9000"], "role": "ROUTE"}]]
           SUCCESS {}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
           PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
           RECORD [9223372036854775807, [{"addresses": ["#HOST#:9020"],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"}, {"addresses": ["#HOST#:9000"], "role": "ROUTE"}]]
           SUCCESS {}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
           PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
           RECORD [9223372036854775807, [{"addresses": [],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"}, {"addresses": ["#HOST#:9000"], "role": "ROUTE"}]]
           SUCCESS {}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
           PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
           RECORD [9223372036854775807, [{"addresses": ["#HOST#:9021"],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"}, {"addresses": ["#HOST#:9000"], "role": "ROUTE"}]]
           SUCCESS {}
        """

    def router_script_with_another_router(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9012"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9022"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_with_reader_support(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {?
            C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
            C: PULL {"n": -1}
            S: SUCCESS {"fields": ["ttl", "servers"]}
            S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9000", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
            S: SUCCESS {"type": "r"}
            {?
                C: GOODBYE
                S: SUCCESS {}
                   <EXIT>
            ?}
        ?}
        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
           <EXIT>
        """

    def router_script_with_one_reader_and_exit(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
           <EXIT>
        """

    def router_script_with_another_router_and_fake_reader(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {{
            C: RUN "CALL dbms.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r", "db": "system"}
        ----
            C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": null} {"[mode]": "r", "db": "system"}
        ----
            C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "system"} {"[mode]": "r", "db": "system"}
        }}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9100"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9022"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
           <EXIT>
        """

    def router_script_with_empty_context_and_reader_support(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        !: AUTO GOODBYE
        {{
            C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": {"address": "#HOST#:9000"} #EXTRA_HELLO_PROPS#}
        ----
            C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": {} #EXTRA_HELLO_PROPS#}
        }}
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {?
            C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": {"address": "#HOST#:9000"}, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
            C: PULL {"n": -1}
            S: SUCCESS {"fields": ["ttl", "servers"]}
            S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9000", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
            S: SUCCESS {"type": "r"}
            {?
                C: GOODBYE
                S: SUCCESS {}
                   <EXIT>
            ?}
        ?}
        C: BEGIN {"mode": "r", "db": "adb"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL {"n": 1000}
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
        """

    def router_script_with_empty_writers_adb(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": [], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_with_empty_writers_any_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {{
            C: RUN "CALL dbms.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        ----
            C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "*"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        }}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": [], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
           <EXIT>
        """

    def router_script_with_one_writer(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_with_the_other_one_writer(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_with_another_router_and_non_existent_reader(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9099"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
           <EXIT>
        """

    def router_script_with_empty_response(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, []]
        S: SUCCESS {"type": "r"}
        """

    def router_script_with_db_not_found_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: FAILURE {"code": "Neo.ClientError.Database.DatabaseNotFound", "message": "wut!"}
        {?
            C: RESET
            S: SUCCESS {}
        ?}
        """

    def router_script_with_unreachable_db_and_adb_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007", "routing": #HELLO_ROUTINGCTX# #EXTRA_HELLO_PROPS# }
        S: SUCCESS {"server": "Neo4j/4.0.0", "connection_id": "bolt-123456789"}
        {?
            C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "unreachable"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
            C: PULL {"n": -1}
            S: SUCCESS {"fields": ["ttl", "servers"]}
            S: RECORD [1000, []]
            S: SUCCESS {"type": "r"}
            {?
                C: GOODBYE
                S: SUCCESS {}
                   <EXIT>
            ?}
        ?}
        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system", "[bookmarks]": "*"}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_with_bookmarks(self, bookmarks_in):
        bookmarks_in = ', "bookmarks{}": %s' % json.dumps(bookmarks_in)
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: RUN "CALL dbms.routing.getRoutingTable($context, $database)" {"context": #ROUTINGCTX#, "database": "adb"} {"[mode]": "r", "db": "system"%s}
        C: PULL {"n": -1}
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r", "bookmark": "sys:2234"}
        """ % bookmarks_in

    def get_vars(self):
        host = self._routingServer1.host
        v = {
            "#VERSION#": 4.1,
            "#HOST#": host,
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
            "#ROUTINGCTX#": '{"address": "' + host + ':9000", "region": "china", "policy": "my_policy"}',
        }

        v["#HELLO_ROUTINGCTX#"] = v["#ROUTINGCTX#"]

        if get_driver_name() in ['javascript']:
            v["#HELLO_ROUTINGCTX#"] = '{"region": "china", "policy": "my_policy"}'

        return v

    def route_call_count(self, server):
        return server.count_requests(
            'RUN "CALL dbms.routing.getRoutingTable('
        )

    # Ignore this on older protocol versions than 4.3
    def test_should_read_successfully_from_reader_using_session_run_with_default_db_driver(self):
        pass

    def test_should_pass_system_bookmark_when_getting_rt_for_multi_db(self):
        # passing bookmarks of the system db when fetching the routing table
        # makes sure that newly (asynchronously) created databases exist.
        # (causal consistency on database existence)
        bookmarks = ["sys:1234", "foo:5678"]

        driver = Driver(self._backend, self._uri, self._auth, self._userAgent)
        self._routingServer1.start(
            script=self.router_script_with_bookmarks(bookmarks),
            vars=self.get_vars()
        )
        self._readServer1.start(script=self.read_script_with_bookmarks(),
                                vars=self.get_vars())

        session = driver.session('r', database=self.get_db(),
                                 bookmarks=bookmarks)
        result = session.run("RETURN 1 as n")
        sequence = self.collectRecords(result)
        last_bookmarks = session.lastBookmarks()
        session.close()
        driver.close()

        self._routingServer1.done()
        self._readServer1.done()
        self.assertEqual([1], sequence)
        self.assertEqual(["foo:6678"], last_bookmarks)

    def test_should_ignore_system_bookmark_when_getting_rt_for_multi_db(self):
        pass


class RoutingV3(Routing):
    def router_script_adb(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        !: ALLOW RESTART

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_adb_multi(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        {+
            C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
            C: PULL_ALL
            S: SUCCESS {"fields": ["ttl", "servers"]}
            S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
            S: SUCCESS {"type": "r"}
        +}
        """

    def router_script_default_db(self):
        return self.router_script_adb()

    def router_script_connectivity_db(self):
        return self.router_script_adb()

    def router_script_with_two_requests_adb(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
           <EXIT>
        """

    def router_script_with_procedure_not_found_failure_connectivity_db(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: FAILURE {"code": "Neo.ClientError.Procedure.ProcedureNotFound", "message": "blabla"}
        S: IGNORED
        S: <EXIT>
        """

    def router_script_with_unknown_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: FAILURE {"code": "Neo.ClientError.General.Unknown", "message": "wut!"}
        S: IGNORED
        S: <EXIT>
        """

    def router_script_with_leader_change(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO HELLO
        !: AUTO GOODBYE

        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {}
           PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
           RECORD [9223372036854775807, [{"addresses": ["#HOST#:9020"],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"},{"addresses": ["#HOST#:9000"], "role": "ROUTE"}]]
           SUCCESS {}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {}
           PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
           RECORD [9223372036854775807, [{"addresses": ["#HOST#:9020"],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"},{"addresses": ["#HOST#:9000"], "role": "ROUTE"}]]
           SUCCESS {}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {}
           PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
           RECORD [9223372036854775807, [{"addresses": [],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"},{"addresses": ["#HOST#:9000"], "role": "ROUTE"}]]
           SUCCESS {}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {}
           PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
           RECORD [9223372036854775807, [{"addresses": ["#HOST#:9021"],"role": "WRITE"}, {"addresses": ["#HOST#:9006","#HOST#:9007"], "role": "READ"},{"addresses": ["#HOST#:9000"], "role": "ROUTE"}]]
           SUCCESS {}
        """

    def router_script_with_another_router(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9012"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9022"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_with_reader_support(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        {?
            C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
            C: PULL_ALL
            S: SUCCESS {"fields": ["ttl", "servers"]}
            S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9000", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
            S: SUCCESS {"type": "r"}
            {?
                C: GOODBYE
                S: SUCCESS {}
                   <EXIT>
            ?}
        ?}
        C: BEGIN {"mode": "r"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
           <EXIT>
        """

    def router_script_with_another_router_and_fake_reader(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9100"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9022"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
           <EXIT>
        """

    def router_script_with_one_reader_and_exit(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
           <EXIT>
        """

    def router_script_with_empty_context_and_reader_support(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: ALLOW RESTART

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS_EMPTY_CTX#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        {?
            {{
                C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": {"address": "#HOST#:9000"}} {"[mode]": "r"}
            ----
                C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": {}} {"[mode]": "r"}
            }}
            C: PULL_ALL
            S: SUCCESS {"fields": ["ttl", "servers"]}
            S: RECORD [1000, [{"addresses": ["#HOST#:9000"], "role":"ROUTE"}, {"addresses": ["#HOST#:9000", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
            S: SUCCESS {"type": "r"}
            {?
                C: GOODBYE
                S: SUCCESS {}
                   <EXIT>
            ?}
        ?} 
        C: BEGIN {"mode": "r"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {}
        """

    def router_script_with_empty_writers_adb(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE

        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": [], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
           <EXIT>
        """

    def router_script_with_empty_writers_any_db(self):
        return self.router_script_with_empty_writers_adb()

    def router_script_with_one_writer(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9020"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """

    def router_script_with_the_other_one_writer(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9010", "#HOST#:9011"], "role":"READ"}, {"addresses": ["#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
        """


    def router_script_with_another_router_and_non_existent_reader(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, [{"addresses": ["#HOST#:9001"], "role":"ROUTE"}, {"addresses": ["#HOST#:9099"], "role":"READ"}, {"addresses": ["#HOST#:9020", "#HOST#:9021"], "role":"WRITE"}]]
        S: SUCCESS {"type": "r"}
           <EXIT>
        """

    def router_script_with_empty_response(self):
        return """
        !: BOLT #VERSION#
        !: AUTO RESET
        !: AUTO GOODBYE
        C: HELLO {"scheme": "basic", "credentials": "c", "principal": "p", "user_agent": "007" #EXTRA_HELLO_PROPS# #EXTR_HELLO_ROUTING_PROPS#}
        S: SUCCESS {"server": "Neo4j/3.5.0", "connection_id": "bolt-123456789"}
        C: RUN "CALL dbms.cluster.routing.getRoutingTable($context)" {"context": #ROUTINGCTX#} {"[mode]": "r"}
        C: PULL_ALL
        S: SUCCESS {"fields": ["ttl", "servers"]}
        S: RECORD [1000, []]
        S: SUCCESS {"type": "r"}
        """

    def router_script_with_db_not_found_failure(self):
        raise ValueError("No multi db support in V3")

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

    read_script_default_db = read_script

    def read_script_with_unexpected_interruption(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: RUN "RETURN 1 as n" {} {"mode": "r"}
        C: PULL_ALL
        S: <EXIT>
        """

    def read_script_with_bookmarks(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: RUN "RETURN 1 as n" {} {"mode": "r", "bookmarks{}": ["sys:1234", "foo:5678"]}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r", "bookmark": "foo:6678"}
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
        """

    def read_tx_script_with_bookmarks(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: BEGIN {"mode": "r", "bookmarks": ["OldBookmark"]}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "r"}
        C: COMMIT
        S: SUCCESS {"bookmark": "NewBookmark"}
        """

    def read_tx_script_with_unexpected_interruption(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: BEGIN {"mode": "r"}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: <EXIT>
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

    def write_script_with_unexpected_interruption(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: <EXIT>
        """

    def write_script_with_bookmark(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: RUN "RETURN 1 as n" {} {"bookmarks": ["NewBookmark"]}
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

    def write_tx_script_with_leader_switch_and_retry(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: BEGIN {}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: FAILURE {"code": "Neo.ClientError.Cluster.NotALeader", "message": "blabla"}
           IGNORED
        C: BEGIN {}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "w"}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           RECORD [1]
           SUCCESS {"type": "w"}
        C: COMMIT
        S: SUCCESS {}
        """

    def write_tx_script_with_bookmarks(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: BEGIN {"bookmarks": ["OldBookmark"]}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           SUCCESS {"type": "w"}
        C: COMMIT
        S: SUCCESS {"bookmark": "NewBookmark"}
        """

    def write_read_tx_script_with_bookmark(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: BEGIN {"bookmarks": ["BookmarkA"]}
        S: SUCCESS {}
        C: RUN "CREATE (n {name:'Bob'})" {} {}
           PULL_ALL
        S: SUCCESS {}
           SUCCESS {}
        C: COMMIT
        S: SUCCESS {"bookmark": "BookmarkB"}
        C: BEGIN {"bookmarks": ["BookmarkB"]}
        S: SUCCESS {}
        C: RUN "MATCH (n) RETURN n.name AS name" {} {}
           PULL_ALL
        S: SUCCESS {"fields": ["name"]}
           RECORD ["Bob"]
           SUCCESS {}
        C: COMMIT
        S: SUCCESS {"bookmark": "BookmarkC"}
        """

    def write_tx_script_with_unexpected_interruption(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: BEGIN {}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: <EXIT>
        """

    def write_script_with_not_a_leader_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: FAILURE {"code": "Neo.ClientError.Cluster.NotALeader", "message": "blabla"}
        S: IGNORED
        C: RESET
        S: SUCCESS {}
        """

    def write_tx_script_with_not_a_leader_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: BEGIN {}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: FAILURE {"code": "Neo.ClientError.Cluster.NotALeader", "message": "blabla"}
        S: IGNORED
        C: RESET
        S: SUCCESS {}
        """

    def write_tx_script_with_database_unavailable_failure_on_commit(self):
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
        S: FAILURE {"code": "Neo.TransientError.General.DatabaseUnavailable", "message": "Database shut down."}
        S: <EXIT>
        """

    def write_tx_script_multiple_bookmarks(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: BEGIN {"bookmarks{}": ["neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29", "neo4j:bookmark:v1:tx94", "neo4j:bookmark:v1:tx56", "neo4j:bookmark:v1:tx16", "neo4j:bookmark:v1:tx68"]}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: SUCCESS {"fields": ["n"]}
           SUCCESS {"type": "w"}
        C: COMMIT
        S: SUCCESS {"bookmark": "neo4j:bookmark:v1:tx95"}
        """

    def write_tx_script_with_database_unavailable_failure(self):
        return """
        !: BOLT #VERSION#
        !: AUTO HELLO
        !: AUTO GOODBYE
        !: AUTO RESET
        C: BEGIN {}
        S: SUCCESS {}
        C: RUN "RETURN 1 as n" {} {}
        C: PULL_ALL
        S: FAILURE {"code": "Neo.TransientError.General.DatabaseUnavailable", "message": "Database is busy doing store copy"}
        S: IGNORED
        """

    def get_vars(self):
        host = self._routingServer1.host
        v = {
            "#VERSION#": 3,
            "#HOST#": host,
            "#ROUTINGCTX#": '{"address": "' + host + ':9000", "region": "china", "policy": "my_policy"}',
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
            "#EXTR_HELLO_ROUTING_PROPS#": "",
            "#EXTR_HELLO_ROUTING_PROPS_EMPTY_CTX#": ""
        }

        if get_driver_name() in ['java']:
            v["#EXTR_HELLO_ROUTING_PROPS#"] = ', "routing": ' + v['#ROUTINGCTX#']
            v["#EXTR_HELLO_ROUTING_PROPS_EMPTY_CTX#"] = ', "routing": {"address": "' + host + ':9000"}'

        if get_driver_name() in ['javascript']:
            v["#ROUTINGCTX#"] = '{"region": "china", "policy": "my_policy"}'

        return v

    def get_db(self):
        return None

    def route_call_count(self, server):
        return server.count_requests(
            'RUN "CALL dbms.cluster.routing.getRoutingTable('
        )

    def should_support_multi_db(self):
        return False

    def test_should_read_successfully_from_reachable_db_after_trying_unreachable_db(self):
        pass

    def test_should_pass_system_bookmark_when_getting_rt_for_multi_db(self):
        pass


class NoRouting(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9000)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

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
        # TODO: '#ROUTING#' is the correct way to go (minimal data transmission)
        return {
            "#VERSION#": "4.1",
            "#EXTRA_HELLO_PROPS#": get_extra_hello_props(),
            "#ROUTING#": ', "routing": null' if get_driver_name() in ['java', 'dotnet', 'go'] else ''
        }

    # Checks that routing is disabled when URI is bolt, no routing in HELLO and
    # no call to retrieve routing table. From bolt >= 4.1 the routing context
    # is used to disable/enable server side routing.
    def test_should_read_successfully_using_session_run(self):
        # Driver is configured to talk to "routing" stub server
        uri = "bolt://%s" % self._server.address
        self._server.start(script=self.script(), vars=self.get_vars())
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken(scheme="basic", principal="p",
                                                 credentials="c"),
                        userAgent="007")

        session = driver.session('r', database="adb")
        session.run("RETURN 1 as n")
        session.close()
        driver.close()

        self._server.done()
