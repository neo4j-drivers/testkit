"""
Shared utilities for writing tests
Common between integration tests using Neo4j and stub server.

Uses environment variables for configuration:

TEST_BACKEND_HOST  Hostname of backend, default is localhost
TEST_BACKEND_PORT  Port on backend host, default is 9876
"""

import inspect
import os
import re
import unittest

from nutkit import protocol
from nutkit.backend import Backend


def get_backend_host_and_port():
    host = os.environ.get('TEST_BACKEND_HOST', '127.0.0.1')
    port = os.environ.get('TEST_BACKEND_PORT', 9876)
    return host, port


def new_backend():
    """ Returns connection to backend, caller is responsible for closing
    """
    host, port = get_backend_host_and_port()
    return Backend(host, port)


def get_driver_name():
    return os.environ['TEST_DRIVER_NAME']


class TestkitTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        id_ = re.sub(r"^([^\.]+\.)*?tests\.", "", self.id())
        self._backend = new_backend()
        self.addCleanup(self._backend.close)
        response = self._backend.sendAndReceive(protocol.StartTest(id_))
        if isinstance(response, protocol.SkipTest):
            self.skipTest(response.reason)

        # TODO: remove this compatibility layer when all drivers are adapted
        id_ = re.sub(r"stub.routing\.[^.]+\.", "stub.routing.", id_)
        response = self._backend.sendAndReceive(protocol.StartTest(id_))
        if isinstance(response, protocol.SkipTest):
            self.skipTest(response.reason)

        elif not isinstance(response, protocol.RunTest):
            raise Exception("Should be SkipTest or RunTest, "
                            "received {}: {}".format(type(response),
                                                     response))

    def script_path(self, *path):
        base_path = os.path.dirname(inspect.getfile(self.__class__))
        return os.path.join(base_path, "scripts", *path)
