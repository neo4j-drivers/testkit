from abc import abstractmethod
import inspect
import os

import nutkit.protocol as types
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


# This should be the latest/current version of the protocol.
# Older protocol that needs to be tested inherits from this and override
# to handle variations.
class RoutingBase(TestkitTestCase):
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
        self._uri_template = "neo4j://%s:%d"
        self._uri_template_with_context = \
            self._uri_template + "?region=china&policy=my_policy"
        self._uri_with_context = self._uri_template_with_context % (
            self._routingServer1.host, self._routingServer1.port
        )
        self._auth = types.AuthorizationToken(
            "basic", principal="p", credentials="c"
        )
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

    @property
    @abstractmethod
    def bolt_version(self):
        pass

    @property
    @abstractmethod
    def server_agent(self):
        pass

    @property
    @abstractmethod
    def adb(self):
        pass

    def get_vars(self, host=None):
        if host is None:
            host = self._routingServer1.host
        v = {
            "#VERSION#": self.bolt_version,
            "#HOST#": host,
            "#SERVER_AGENT#": self.server_agent,
            "#ROUTINGCTX#":
                f'{{"address": "{host}:9000", '
                f'"region": "china", "policy": "my_policy"}}',
        }

        return v

    def start_server(self, server, script_fn, vars_=None):
        if vars_ is None:
            vars_ = self.get_vars()
        classes = (self.__class__, *inspect.getmro(self.__class__))
        tried_locations = []
        for cls in classes:
            if isinstance(getattr(cls, "bolt_version", None), str):
                version_folder = \
                    "v{}".format(cls.bolt_version.replace(".", "x"))
                script_path = self.script_path(version_folder, script_fn)
                tried_locations.append(script_path)
                if os.path.exists(script_path):
                    server.start(path=script_path, vars_=vars_)
                    return
        raise FileNotFoundError("{!r} tried {!r}".format(
            script_fn, ", ".join(tried_locations)
        ))

    @abstractmethod
    def route_call_count(self, server):
        pass

    @staticmethod
    def collect_records(result):
        sequence = []
        while True:
            record = result.next()
            if isinstance(record, types.NullRecord):
                break
            sequence.append(record.values[0].value)
        return sequence
